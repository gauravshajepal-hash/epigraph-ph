import json
import logging
import re
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

import fitz  # PyMuPDF
import ollama
import yaml

from .db import DatabaseManager

logger = logging.getLogger(__name__)


OCR_PUNCT_TRANSLATION = str.maketrans(
    {
        "\u00a0": " ",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\ufb01": "fi",
        "\ufb02": "fl",
        "\uff08": "(",
        "\uff09": ")",
        "\uff0c": ",",
        "\uff1a": ":",
        "\uff1b": ";",
    }
)

OCR_PHRASE_REPAIRS = (
    (r"\bDepartmentofHealth\b", "Department of Health"),
    (r"\bEpidemiologyBureau\b", "Epidemiology Bureau"),
    (r"\bOFFICEOFTHESECRETARY\b", "OFFICE OF THE SECRETARY"),
    (r"\bADMINISTRATIVEORDER\b", "ADMINISTRATIVE ORDER"),
    (r"\bSCOPEANDCOVERAGE\b", "SCOPE AND COVERAGE"),
    (r"\bHIV/AIDS&ARTREGISTRYOFTHEPHILIPPINES\b", "HIV/AIDS & ART REGISTRY OF THE PHILIPPINES"),
    (r"\bARTREGISTRYOFTHEPHILIPPINES\b", "ART REGISTRY OF THE PHILIPPINES"),
    (r"\bSUMMARYOFNEWLYDIAGNOSEDCASES\b", "SUMMARY OF NEWLY DIAGNOSED CASES"),
    (r"\bNEWLYDIAGNOSEDCASES\b", "NEWLY DIAGNOSED CASES"),
    (r"\bSPECIFICPOPULATION\b", "SPECIFIC POPULATION"),
    (r"\bGeographicDistribution\b", "Geographic Distribution"),
    (r"\bModeofTransmission\b", "Mode of Transmission"),
    (r"\bTotalreporteddeaths\b", "Total reported deaths"),
    (r"\bTotalreported\b", "Total reported"),
    (r"\bAveragecasesper\b", "Average cases per"),
    (r"\bWithadvanced\b", "With advanced"),
    (r"\bHIVdisease\b", "HIV disease"),
    (r"\bPostHIV-TestCounseling\b", "Post HIV-Test Counseling"),
    (r"\bConfirmatoryrHIVdasite\b", "Confirmatory rHIVda site"),
    (r"\bTREATMENTHUBS\b", "TREATMENT HUBS"),
    (r"\bOUTPATIENTANDINPATIENTCARE&TREATMENT\b", "OUTPATIENT AND INPATIENT CARE & TREATMENT"),
    (r"\bAIDSCONTINUUMOFCARE\b", "AIDS CONTINUUM OF CARE"),
    (r"\bARTRegistryofthe\b", "ART Registry of the"),
    (r"\bARTRegistryof\b", "ART Registry of"),
    (r"\bHIV/AIDS&ARTRegistry\b", "HIV/AIDS & ART Registry"),
    (r"\bHIV/AIDS&ARTRegistryofthe\b", "HIV/AIDS & ART Registry of the"),
    (r"\bindividualsreported\b", "individuals reported"),
    (r"\bandwereaccountedtothetotal\b", "and were accounted to the total"),
    (r"\baccountedtothetotal\b", "accounted to the total"),
    (r"\breportedcasessince\b", "reported cases since"),
    (r"\bHIVcasesreportedto\b", "HIV cases reported to"),
    (r"\bdiagnosedwomenwerereported\b", "diagnosed women were reported"),
    (r"\bdiagnosedwomenwasreported\b", "diagnosed women was reported"),
    (r"\bdiagnosedpregnantcaseswith\b", "diagnosed pregnant cases with"),
    (r"\bTGWdiagnosedfrom\b", "TGW diagnosed from"),
    (r"\bSincethefrstreported\b", "Since the first reported"),
    (r"\bSincethefirstreported\b", "Since the first reported"),
    (r"\bHIVcaseinthe\b", "HIV case in the"),
    (r"\bHIVinfectioninthe\b", "HIV infection in the"),
    (r"\bPhilippinesin\b", "Philippines in"),
    (r"\btherehavebeen\b", "there have been"),
    (r"\bNinety-fourpercent\b", "Ninety-four percent"),
    (r"\bweremaleand\b", "were male and"),
    (r"\bwerefemaler\b", "were female"),
    (r"\bthetimeof\b", "the time of"),
    (r"\bthetime\b", "the time"),
    (r"\btimeof\b", "time of"),
    (r"\bAmongthe\b", "Among the"),
    (r"\bOfthe\b", "Of the"),
    (r"\btherewere\b", "there were"),
    (r"\bnumberofdiagnosedpregnantcaseswith\b", "number of diagnosed pregnant cases with"),
    (r"\bpregnantwomenreportedat\b", "pregnant women reported at"),
)

SUSPICIOUS_MERGED_FRAGMENTS = (
    "advanced",
    "average",
    "bureau",
    "cases",
    "counsel",
    "department",
    "diagnosed",
    "distribution",
    "epidemiology",
    "facility",
    "follow",
    "geographic",
    "guideline",
    "health",
    "individual",
    "newly",
    "philippines",
    "population",
    "reactive",
    "registry",
    "reported",
    "result",
    "service",
    "summary",
    "testing",
    "transmission",
    "treatment",
    "viral",
)


class ParsingManager:
    """
    Text-first parser for large PDFs.
    Native text is always kept; vision is only used for pages that look genuinely OCR-heavy.
    """

    def __init__(
        self,
        db: DatabaseManager,
        config_path: str = "config.yaml",
        worker_id: str = "parser",
        folder_filters: list[str] | None = None,
    ):
        self.db = db
        self.worker_id = worker_id
        self.folder_filters = folder_filters or []
        self.config_path = config_path

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Failed to load config for parsing: {e}")
            cfg = {}

        paths_cfg = cfg.get("paths", {})
        extraction_cfg = cfg.get("extraction", {})
        parsing_cfg = cfg.get("parsing", {})

        self.output_dir = Path(paths_cfg.get("markdown", "data/processed_md"))
        self.llm_model = extraction_cfg.get("model", "qwen3.5:9b")

        self.ocr_text_char_threshold = int(parsing_cfg.get("ocr_text_char_threshold", 80))
        self.image_text_char_threshold = int(parsing_cfg.get("image_text_char_threshold", 600))
        self.table_text_char_threshold = int(parsing_cfg.get("table_text_char_threshold", 1200))
        self.table_detection_max_chars = int(parsing_cfg.get("table_detection_max_chars", 1500))
        self.image_coverage_threshold = float(parsing_cfg.get("image_coverage_threshold", 0.35))
        self.vector_graphics_threshold = int(parsing_cfg.get("vector_graphics_threshold", 120))
        self.vector_text_char_threshold = int(parsing_cfg.get("vector_text_char_threshold", 300))
        self.vision_render_scale = float(parsing_cfg.get("vision_render_scale", 1.5))
        self.prefer_ocr_for_scanned_pages = bool(parsing_cfg.get("prefer_ocr_for_scanned_pages", True))
        self.ocr_min_chars = int(parsing_cfg.get("ocr_min_chars", 80))
        self.ocr_timeout_seconds = int(parsing_cfg.get("ocr_timeout_seconds", 120))
        self.save_ocr_failure_images = bool(parsing_cfg.get("save_ocr_failure_images", True))
        self.parse_worker_timeout_seconds = int(parsing_cfg.get("parse_worker_timeout_seconds", 7200))
        self.vision_timeout_seconds = int(parsing_cfg.get("vision_timeout_seconds", 45))
        self.vision_retries = int(parsing_cfg.get("vision_retries", 1))
        self.reset_model_on_vision_failure = bool(parsing_cfg.get("reset_model_on_vision_failure", True))
        self.flattened_table_min_lines = int(parsing_cfg.get("flattened_table_min_lines", 80))
        self.flattened_table_short_line_ratio = float(parsing_cfg.get("flattened_table_short_line_ratio", 0.55))
        self.flattened_table_numeric_line_ratio = float(parsing_cfg.get("flattened_table_numeric_line_ratio", 0.45))
        self.scrambled_table_min_lines = int(parsing_cfg.get("scrambled_table_min_lines", 8))
        self.scrambled_table_pipe_line_ratio = float(parsing_cfg.get("scrambled_table_pipe_line_ratio", 0.55))
        self.scrambled_table_numeric_token_ratio = float(parsing_cfg.get("scrambled_table_numeric_token_ratio", 0.58))
        self.scrambled_table_alpha_token_ratio = float(parsing_cfg.get("scrambled_table_alpha_token_ratio", 0.24))
        self.scrambled_table_short_cell_ratio = float(parsing_cfg.get("scrambled_table_short_cell_ratio", 0.62))
        self.ocr_cleanup_enabled = bool(parsing_cfg.get("ocr_cleanup_enabled", True))
        self.qa_warn_merged_token_count = int(parsing_cfg.get("qa_warn_merged_token_count", 60))
        self.qa_fail_merged_token_count = int(parsing_cfg.get("qa_fail_merged_token_count", 220))
        self.qa_warn_merged_token_density = float(parsing_cfg.get("qa_warn_merged_token_density", 2.0))
        self.qa_fail_merged_token_density = float(parsing_cfg.get("qa_fail_merged_token_density", 7.5))
        self.ollama_client = ollama.Client(timeout=self.vision_timeout_seconds)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.ocr_failure_dir = self.output_dir / "_ocr_failures"
        self.ocr_failure_dir.mkdir(parents=True, exist_ok=True)
        self.qa_dir = self.output_dir / "_qa"
        self.qa_dir.mkdir(parents=True, exist_ok=True)

    def process_pending_documents(self, limit=None):
        """Claims 'downloaded' documents and parses them with page-level checkpoints."""
        claim_limit = limit or 1
        docs = self.db.claim_documents(
            "downloaded",
            "parsing",
            claim_limit,
            self.worker_id,
            folder_filters=self.folder_filters,
        )
        logger.info(f"Claimed {len(docs)} documents pending text-first parsing.")

        parsed_count = 0
        failed_count = 0

        for doc in docs:
            doc_id = doc["id"]
            pdf_path = Path(doc["local_path"])

            if not pdf_path.exists():
                logger.error(f"Doc {doc_id} missing at {pdf_path}. Skipping.")
                self.db.update_document_status(doc_id, "failed")
                failed_count += 1
                continue

            try:
                logger.info(f"Parsing {pdf_path.name} with checkpoints...")
                out_path = self._parse_document_isolated(doc)
                latest_row = self.db.get_document(doc_id)
                latest_status = latest_row["status"] if latest_row else None

                if latest_status == "downloaded":
                    logger.info(
                        "Doc %s requeued for resumed parsing after an isolated worker crash.",
                        doc_id,
                    )
                    continue

                if not out_path.exists() or out_path.stat().st_size < 50:
                    logger.warning(f"Parsing yielded insufficient content for {pdf_path.name}")
                    self.db.update_document_status(doc_id, "failed")
                    failed_count += 1
                    continue

                qa_report = self._load_parse_quality_report(out_path) or self._write_parse_quality_report(out_path)
                gate_status = qa_report.get("gate_status", "pass")
                if gate_status == "fail":
                    logger.warning(
                        "Parse QA failed for %s: %s",
                        pdf_path.name,
                        "; ".join(qa_report.get("issues", [])) or "unspecified parse quality issue",
                    )
                    self.db.update_document_status(doc_id, "failed")
                    failed_count += 1
                    continue
                if gate_status == "warn":
                    logger.warning(
                        "Parse QA warning for %s: %s",
                        pdf_path.name,
                        "; ".join(qa_report.get("issues", [])) or "merged-token noise detected",
                    )

                self.db.update_document_status(doc_id, "parsed")
                parsed_count += 1
                logger.info(f"Successfully parsed {pdf_path.name}.")

            except Exception as e:
                logger.error(f"Parsing failure for {pdf_path.name}: {e}")
                self.db.update_document_status(doc_id, "failed")
                failed_count += 1

        logger.info(f"Parsing complete. Success: {parsed_count}, Failed: {failed_count}.")
        return parsed_count

    def backfill_quality_reports(self, overwrite: bool = False) -> dict:
        """Ensures tracked markdown outputs have QA reports."""
        scope_clause, scope_params = self.db._folder_scope_sql(self.folder_filters)
        with self.db.get_connection() as conn:
            docs = conn.execute(
                f"SELECT id, local_path FROM documents WHERE 1=1 {scope_clause} ORDER BY id",
                scope_params,
            ).fetchall()

        scanned = 0
        written = 0
        skipped = 0
        missing_markdown = []

        for doc in docs:
            markdown_path = self.output_dir / f"{Path(doc['local_path']).stem}.md"
            if not markdown_path.exists():
                missing_markdown.append(str(markdown_path))
                continue

            scanned += 1
            qa_path = self._qa_report_path(markdown_path)
            if qa_path.exists() and not overwrite:
                skipped += 1
                continue

            self._write_parse_quality_report(markdown_path)
            written += 1

        summary = {
            "scanned": scanned,
            "written": written,
            "skipped_existing": skipped,
            "missing_markdown": missing_markdown,
        }
        logger.info(
            "Parse QA backfill complete. scanned=%s written=%s skipped_existing=%s missing_markdown=%s",
            scanned,
            written,
            skipped,
            len(missing_markdown),
        )
        return summary

    def _parse_document_isolated(self, doc_row) -> Path:
        doc_id = int(doc_row["id"])
        start_checkpoint = int(doc_row["parse_checkpoint_page"] or 0)
        worker_cmd = [
            sys.executable,
            "-m",
            "modules.parse_worker",
            "--doc-id",
            str(doc_id),
            "--db-path",
            str(self.db.db_path),
            "--config-path",
            self.config_path,
            "--worker-id",
            self.worker_id,
        ]

        try:
            result = subprocess.run(
                worker_cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.parse_worker_timeout_seconds,
                cwd=str(Path(__file__).resolve().parent.parent),
            )
        except subprocess.TimeoutExpired as exc:
            latest_row = self.db.get_document(doc_id)
            latest_checkpoint = int(latest_row["parse_checkpoint_page"] or 0) if latest_row else 0
            if latest_checkpoint > start_checkpoint:
                logger.warning(
                    "Parse worker timed out for doc %s after making progress (%s -> %s pages). Requeueing.",
                    doc_id,
                    start_checkpoint,
                    latest_checkpoint,
                )
                self.db.update_document_status(doc_id, "downloaded")
                return self.output_dir / f"{Path(doc_row['local_path']).stem}.md"
            raise RuntimeError(
                f"parse worker timed out after {self.parse_worker_timeout_seconds}s for doc {doc_id}"
            ) from exc

        if result.returncode != 0:
            latest_row = self.db.get_document(doc_id)
            latest_checkpoint = int(latest_row["parse_checkpoint_page"] or 0) if latest_row else 0
            stderr_preview = (result.stderr or "").strip()
            logger.error(
                "Parse worker crashed for doc %s with exit code %s. stderr=%s",
                doc_id,
                result.returncode,
                stderr_preview[:800] if stderr_preview else "<empty>",
            )
            if latest_checkpoint > start_checkpoint:
                logger.warning(
                    "Doc %s made progress before crashing (%s -> %s pages). Requeueing to resume next iteration.",
                    doc_id,
                    start_checkpoint,
                    latest_checkpoint,
                )
                self.db.update_document_status(doc_id, "downloaded")
            else:
                raise RuntimeError(
                    f"parse worker crashed without progress for doc {doc_id} (exit {result.returncode})"
                )
            final_path = self.output_dir / f"{Path(doc_row['local_path']).stem}.md"
            return final_path

        payload = {}
        stdout_text = (result.stdout or "").strip()
        if stdout_text:
            try:
                payload = json.loads(stdout_text.splitlines()[-1])
            except json.JSONDecodeError:
                logger.warning("Could not decode parse worker output for doc %s.", doc_id)

        out_path_value = payload.get("out_path") if isinstance(payload, dict) else None
        if out_path_value:
            return Path(out_path_value)
        return self.output_dir / f"{Path(doc_row['local_path']).stem}.md"

    def _parse_document(self, doc_row, pdf_path: Path) -> Path:
        final_path = self.output_dir / f"{pdf_path.stem}.md"
        partial_path = final_path.with_suffix(f"{final_path.suffix}.part")

        with fitz.open(str(pdf_path)) as pdf:
            num_pages = len(pdf)
            checkpoint_page = int(doc_row["parse_checkpoint_page"] or 0)
            checkpoint_page = max(0, min(checkpoint_page, num_pages))

            if checkpoint_page > 0 and not partial_path.exists():
                logger.warning(
                    f"Partial markdown missing for {pdf_path.name}; restarting from page 1."
                )
                checkpoint_page = 0
                self.db.reset_parse_progress(doc_row["id"])

            if checkpoint_page == 0:
                with open(partial_path, "w", encoding="utf-8") as f:
                    f.write(self._build_header(doc_row, pdf_path))
                self.db.update_parse_progress(doc_row["id"], 0, num_pages, self.worker_id)
            else:
                logger.info(f"Resuming {pdf_path.name} from page {checkpoint_page + 1}/{num_pages}")
                self.db.touch_document_claim(doc_row["id"], self.worker_id)

            with open(partial_path, "a", encoding="utf-8") as output_file:
                for page_index in range(checkpoint_page, num_pages):
                    page = pdf.load_page(page_index)
                    output_file.write(
                        self._render_page_markdown(page, page_index, num_pages)
                    )
                    output_file.flush()
                    self.db.update_parse_progress(
                        doc_row["id"],
                        page_index + 1,
                        num_pages,
                        self.worker_id,
                    )

        self._finalize_markdown_file(partial_path, final_path)
        self._write_parse_quality_report(final_path)
        return final_path

    def _build_header(self, doc_row, pdf_path: Path) -> str:
        relative_source = pdf_path.parent.name
        return (
            "--- SOURCE INFO ---\n"
            f"Url: {doc_row['url']}\n"
            f"Folder: {relative_source}\n"
            f"Filename: {pdf_path.name}\n"
            "------------------\n\n"
        )

    def _render_page_markdown(self, page: fitz.Page, page_index: int, num_pages: int) -> str:
        native_text = page.get_text().strip()
        if self._looks_blank_page(page, native_text):
            logger.info(f"Page {page_index + 1}/{num_pages}: Blank page detected")
            return "\n".join(
                [
                    f"## [Page {page_index + 1}]",
                    "",
                    "#### [Blank Page]",
                    "No extractable text or visual content detected on this page.",
                    "",
                ]
            ).strip() + "\n\n"

        needs_vision, reasons = self._should_use_vision(page, native_text)
        flattened_table = any(reason.startswith("FlattenedTableText") for reason in reasons)
        page_lines = [f"## [Page {page_index + 1}]", ""]

        if flattened_table:
            positioned_text = self._clean_text_fragment(self._reconstruct_positioned_rows(page))
            if positioned_text and not self._looks_like_scrambled_table_text(positioned_text):
                logger.info(
                    f"Page {page_index + 1}/{num_pages}: Reconstructing positioned table layout ({', '.join(reasons)})"
                )
                page_lines.extend(
                    [
                        "#### [Position-Aware Table Layout]",
                        positioned_text,
                        "",
                    ]
                )
            else:
                logger.warning(
                    "Page %s/%s: deferring flattened table page because reconstruction is still too noisy.",
                    page_index + 1,
                    num_pages,
                )
                page_lines.extend(self._table_parsing_deferred_block())
        elif native_text:
            page_lines.extend(["#### [Native Text Content]", self._clean_text_fragment(native_text), ""])

        if needs_vision and not flattened_table:
            ocr_text = ""
            ocr_attempted = False
            if self._should_use_ocr(page, native_text, reasons):
                ocr_attempted = True
                logger.info(
                    f"Page {page_index + 1}/{num_pages}: OCR triggered ({', '.join(reasons)})"
                )
                ocr_text = self._run_ocr(page)
                if ocr_text:
                    ocr_text = self._clean_ocr_text(ocr_text)
                if ocr_text and len(ocr_text) >= self.ocr_min_chars:
                    if self._looks_like_scrambled_table_text(ocr_text):
                        logger.warning(
                            "Page %s/%s: OCR output looks like scrambled table text; deferring page instead of exporting numeric soup.",
                            page_index + 1,
                            num_pages,
                        )
                        page_lines.extend(self._table_parsing_deferred_block())
                    else:
                        page_lines.extend(["#### [OCR Text Content]", ocr_text, ""])
                else:
                    logger.warning(
                        "Page %s/%s: OCR returned insufficient text.",
                        page_index + 1,
                        num_pages,
                    )
                    self._save_ocr_failure_page(page)
                    page_lines.extend(
                        [
                            "#### [OCR Failure]",
                            "Scanned page OCR failed in the isolated worker. The page image was saved for manual review.",
                            "",
                        ]
                    )

            if ocr_attempted and (not ocr_text or len(ocr_text) < self.ocr_min_chars):
                logger.warning(
                    "Page %s/%s: skipping vision fallback after OCR failure on a scanned page.",
                    page_index + 1,
                    num_pages,
                )
            elif not ocr_text or len(ocr_text) < self.ocr_min_chars:
                logger.info(
                    f"Page {page_index + 1}/{num_pages}: Vision triggered ({', '.join(reasons)})"
                )
                vision_insight = self._run_vision_enrichment(page)
                if vision_insight and "NO_VISUAL_CONTENT" not in vision_insight:
                    page_lines.extend(
                        ["#### [AI Visual & Tabular Insights]", vision_insight, ""]
                    )
                elif self._looks_blank_page(page, native_text):
                    page_lines.extend(
                        [
                            "#### [Blank Page]",
                            "No extractable text or visual content detected on this page.",
                            "",
                        ]
                    )
                else:
                    page_lines.extend(
                        [
                            "#### [Vision Failure]",
                            "Low-text page returned no usable content from visual extraction.",
                            "",
                        ]
                    )
        else:
            logger.info(f"Page {page_index + 1}/{num_pages}: Using native text only")

        return "\n".join(page_lines).strip() + "\n\n"

    def _should_use_vision(self, page: fitz.Page, native_text: str) -> tuple[bool, list[str]]:
        native_chars = len(native_text)
        is_scanned = native_chars < self.ocr_text_char_threshold
        image_coverage = self._estimate_image_coverage(page)
        has_large_image = image_coverage >= self.image_coverage_threshold
        lines = [line.strip() for line in native_text.splitlines() if line.strip()]
        short_line_ratio = (
            sum(1 for line in lines if len(line) <= 12) / len(lines)
            if lines
            else 0.0
        )
        numeric_line_ratio = (
            sum(1 for line in lines if any(ch.isdigit() for ch in line)) / len(lines)
            if lines
            else 0.0
        )

        has_tables = False
        if native_chars < self.table_detection_max_chars or has_large_image:
            try:
                has_tables = len(page.find_tables().tables) > 0
            except Exception as e:
                logger.debug(f"Table detection failed on page {page.number + 1}: {e}")

        looks_like_flattened_table = (
            has_tables
            and len(lines) >= self.flattened_table_min_lines
            and short_line_ratio >= self.flattened_table_short_line_ratio
            and numeric_line_ratio >= self.flattened_table_numeric_line_ratio
        )

        dense_vector_graphics = False
        if native_chars < self.vector_text_char_threshold:
            dense_vector_graphics = len(page.get_drawings()) >= self.vector_graphics_threshold

        needs_vision = (
            is_scanned
            or (has_large_image and native_chars < self.image_text_char_threshold)
            or (has_tables and native_chars < self.table_text_char_threshold)
            or dense_vector_graphics
            or looks_like_flattened_table
        )

        reasons = []
        if is_scanned:
            reasons.append("LowTextDensity")
        if has_large_image and native_chars < self.image_text_char_threshold:
            reasons.append(f"LargeImage({image_coverage:.0%})")
        if has_tables and native_chars < self.table_text_char_threshold:
            reasons.append("SparseTableLayout")
        if dense_vector_graphics:
            reasons.append("DenseVectorGraphics")
        if looks_like_flattened_table:
            reasons.append(
                f"FlattenedTableText(lines={len(lines)},short={short_line_ratio:.0%},numeric={numeric_line_ratio:.0%})"
            )

        return needs_vision, reasons

    def _estimate_image_coverage(self, page: fitz.Page) -> float:
        page_area = max(page.rect.width * page.rect.height, 1.0)
        image_area = 0.0

        try:
            layout = page.get_text("dict")
        except Exception as e:
            logger.debug(f"Could not inspect layout on page {page.number + 1}: {e}")
            return 0.0

        for block in layout.get("blocks", []):
            if block.get("type") != 1:
                continue
            x0, y0, x1, y1 = block.get("bbox", (0, 0, 0, 0))
            image_area += max(0.0, (x1 - x0) * (y1 - y0))

        return min(image_area / page_area, 1.0)

    def _looks_blank_page(self, page: fitz.Page, native_text: str) -> bool:
        if native_text.strip():
            return False
        if self._estimate_image_coverage(page) > 0.10:
            return False
        return len(page.get_drawings()) == 0 and len(page.get_images(full=True)) <= 1

    def _save_ocr_failure_page(self, page: fitz.Page):
        if not self.save_ocr_failure_images:
            return
        try:
            pix = page.get_pixmap(
                matrix=fitz.Matrix(self.vision_render_scale, self.vision_render_scale),
                alpha=False,
            )
            out_path = self.ocr_failure_dir / f"page_{page.number + 1:04d}.png"
            pix.save(out_path)
        except Exception as exc:
            logger.warning("Could not persist OCR failure image for page %s: %s", page.number + 1, exc)

    def _should_use_ocr(self, page: fitz.Page, native_text: str, reasons: list[str]) -> bool:
        if not self.prefer_ocr_for_scanned_pages:
            return False

        image_coverage = self._estimate_image_coverage(page)
        return (
            "LowTextDensity" in reasons
            and any(reason.startswith("LargeImage(") for reason in reasons)
            and image_coverage >= 0.85
            and len(native_text) < self.image_text_char_threshold
        )

    def _run_ocr(self, page: fitz.Page) -> str:
        if not self.prefer_ocr_for_scanned_pages:
            return ""

        temp_path: Path | None = None
        try:
            pix = page.get_pixmap(
                matrix=fitz.Matrix(self.vision_render_scale, self.vision_render_scale),
                alpha=False,
            )
            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                temp_path = Path(tmp.name)
            pix.save(temp_path)

            worker_path = Path(__file__).with_name("ocr_worker.py")
            result = subprocess.run(
                [sys.executable, str(worker_path), str(temp_path)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.ocr_timeout_seconds,
            )
            if result.returncode != 0:
                logger.warning(
                    "OCR worker failed on page %s: %s",
                    page.number + 1,
                    (result.stderr or "").strip() or f"exit code {result.returncode}",
                )
                return ""

            payload = json.loads((result.stdout or "").strip() or "{}")
            return str(payload.get("text", "")).strip()
        except Exception as exc:
            logger.warning("OCR failed on page %s: %s", page.number + 1, exc)
            return ""
        finally:
            if temp_path:
                temp_path.unlink(missing_ok=True)

    def _finalize_markdown_file(self, partial_path: Path, final_path: Path) -> None:
        attempts = 12
        delay_seconds = 0.5
        last_error: PermissionError | None = None

        for attempt in range(1, attempts + 1):
            try:
                partial_path.replace(final_path)
                return
            except PermissionError as exc:
                last_error = exc
                logger.warning(
                    "Could not finalize markdown %s on attempt %s/%s: %s",
                    final_path.name,
                    attempt,
                    attempts,
                    exc,
                )
                time.sleep(delay_seconds)

        if last_error is not None:
            raise last_error

    def _clean_text_fragment(self, text: str) -> str:
        if not text:
            return ""

        cleaned_lines = []
        for raw_line in text.splitlines():
            line = raw_line.translate(OCR_PUNCT_TRANSLATION)
            for pattern, replacement in OCR_PHRASE_REPAIRS:
                line = re.sub(pattern, replacement, line)
            line = re.sub(r"(?<=[A-Za-z])&(?=[A-Za-z])", " & ", line)
            line = re.sub(r"(?<=[A-Za-z0-9\]])\.(?=[A-Z])", ". ", line)
            line = re.sub(r"(?<=[A-Za-z])(?=\[)", " ", line)
            line = re.sub(r"[ \t]+", " ", line).strip()
            cleaned_lines.append(line)

        return "\n".join(line for line in cleaned_lines if line).strip()

    def _clean_ocr_text(self, text: str) -> str:
        if not self.ocr_cleanup_enabled or not text:
            return text.strip()

        cleaned_lines = []
        for raw_line in text.splitlines():
            line = self._clean_text_fragment(raw_line)
            line = re.sub(r"([a-z])([A-Z][a-z])", r"\1 \2", line)
            line = re.sub(r"([a-z])([A-Z]{2,})", r"\1 \2", line)
            line = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", line)
            line = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", line)
            line = re.sub(r"(?<=[,;:!?])(?=[A-Za-z0-9])", " ", line)
            line = re.sub(r"(?<=[)])(?=[A-Za-z0-9])", " ", line)
            line = re.sub(r"(?<=[A-Za-z])(?=[(])", " ", line)
            line = re.sub(
                r"\b(In|From|As|Of|By|During|Since|For|On|At|To)(January|February|March|April|May|June|July|August|September|October|November|December)\b",
                r"\1 \2",
                line,
                flags=re.IGNORECASE,
            )
            line = re.sub(r"\b(Region)(\d+[A-Z]?|[A-Z])\b", r"\1 \2", line)
            line = re.sub(r"[ \t]+", " ", line).strip()
            cleaned_lines.append(line)

        cleaned_text = "\n".join(line for line in cleaned_lines if line).strip()
        cleaned_text = re.sub(r"\n{3,}", "\n\n", cleaned_text)
        return cleaned_text

    def _table_parsing_deferred_block(self) -> list[str]:
        return [
            "#### [Table Parsing Deferred]",
            "Detected OCR-scrambled or flattened table content on this page. Raw row text was withheld because the current table reconstruction is not reliable enough for downstream extraction.",
            "",
        ]

    def _looks_like_scrambled_table_text(self, text: str) -> bool:
        if not text:
            return False

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if len(lines) < self.scrambled_table_min_lines:
            return False

        pipe_lines = 0
        numeric_heavy_lines = 0
        short_cell_lines = 0
        numeric_tokens_total = 0
        alpha_tokens_total = 0

        for line in lines:
            if "|" in line:
                pipe_lines += 1

            numeric_tokens = re.findall(r"\b\d[\d,]*(?:\.\d+)?\b", line)
            alpha_tokens = re.findall(r"\b[A-Za-z]{3,}\b", line)
            numeric_tokens_total += len(numeric_tokens)
            alpha_tokens_total += len(alpha_tokens)

            if len(numeric_tokens) >= max(3, len(alpha_tokens) + 2):
                numeric_heavy_lines += 1

            cells = [cell.strip() for cell in line.split("|") if cell.strip()] if "|" in line else [line]
            if len(cells) >= 3:
                short_cells = sum(
                    1
                    for cell in cells
                    if len(re.sub(r"[^A-Za-z]", "", cell)) <= 2
                )
                if short_cells / max(len(cells), 1) >= self.scrambled_table_short_cell_ratio:
                    short_cell_lines += 1

        token_total = numeric_tokens_total + alpha_tokens_total
        numeric_token_ratio = numeric_tokens_total / max(token_total, 1)
        alpha_token_ratio = alpha_tokens_total / max(token_total, 1)
        pipe_line_ratio = pipe_lines / max(len(lines), 1)
        numeric_heavy_ratio = numeric_heavy_lines / max(len(lines), 1)
        short_cell_line_ratio = short_cell_lines / max(len(lines), 1)

        pipe_scramble = (
            pipe_line_ratio >= self.scrambled_table_pipe_line_ratio
            and numeric_token_ratio >= self.scrambled_table_numeric_token_ratio
            and alpha_token_ratio <= self.scrambled_table_alpha_token_ratio
        )
        dense_numeric_scramble = (
            numeric_heavy_ratio >= 0.72
            and alpha_token_ratio <= self.scrambled_table_alpha_token_ratio
        )
        short_cell_scramble = (
            pipe_line_ratio >= 0.35
            and short_cell_line_ratio >= self.scrambled_table_short_cell_ratio
        )
        return pipe_scramble or dense_numeric_scramble or short_cell_scramble

    def _qa_report_path(self, markdown_path: Path) -> Path:
        return self.qa_dir / f"{markdown_path.stem}.qa.json"

    def _load_parse_quality_report(self, markdown_path: Path) -> dict | None:
        qa_path = self._qa_report_path(markdown_path)
        if not qa_path.exists():
            return None
        try:
            return json.loads(qa_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not read parse QA report %s: %s", qa_path, exc)
            return None

    def _write_parse_quality_report(self, markdown_path: Path) -> dict:
        report = self._build_parse_quality_report(markdown_path)
        qa_path = self._qa_report_path(markdown_path)
        qa_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    def _build_parse_quality_report(self, markdown_path: Path) -> dict:
        text = markdown_path.read_text(encoding="utf-8", errors="ignore")
        page_sections = re.split(r"(?=^## \[Page \d+\])", text, flags=re.MULTILINE)
        page_reports = []
        empty_pages = []
        ocr_failure_pages = []
        vision_failure_pages = []
        blank_pages = []
        deferred_table_pages = []
        content_by_type = {
            "ocr_pages": 0,
            "native_pages": 0,
            "positioned_table_pages": 0,
            "vision_pages": 0,
            "deferred_table_pages": 0,
        }

        for section in page_sections:
            match = re.match(r"^## \[Page (\d+)\]", section)
            if not match:
                continue
            page_number = int(match.group(1))
            lines = section.splitlines()
            nonempty_lines = [line for line in lines[1:] if line.strip()]
            content_lines = [line for line in nonempty_lines if not line.startswith("#### [")]

            if "#### [OCR Text Content]" in section:
                content_by_type["ocr_pages"] += 1
            if "#### [Native Text Content]" in section:
                content_by_type["native_pages"] += 1
            if "#### [Position-Aware Table Layout]" in section:
                content_by_type["positioned_table_pages"] += 1
            if "#### [AI Visual & Tabular Insights]" in section:
                content_by_type["vision_pages"] += 1
            if "#### [Table Parsing Deferred]" in section:
                deferred_table_pages.append(page_number)
                content_by_type["deferred_table_pages"] += 1
            if "#### [Blank Page]" in section:
                blank_pages.append(page_number)
            if "#### [Vision Failure]" in section:
                vision_failure_pages.append(page_number)

            if not content_lines and page_number not in blank_pages:
                empty_pages.append(page_number)
            if "#### [OCR Failure]" in section:
                ocr_failure_pages.append(page_number)

            suspicious_tokens = self._find_suspicious_tokens("\n".join(content_lines))
            page_reports.append(
                {
                    "page": page_number,
                    "empty": not content_lines and page_number not in blank_pages,
                    "blank_page": page_number in blank_pages,
                    "ocr_failure": "#### [OCR Failure]" in section,
                    "vision_failure": "#### [Vision Failure]" in section,
                    "table_parsing_deferred": "#### [Table Parsing Deferred]" in section,
                    "suspicious_merged_token_count": len(suspicious_tokens),
                }
            )

        suspicious_tokens = self._find_suspicious_tokens(text)
        suspicious_counts = Counter(suspicious_tokens)
        suspicious_density = len(suspicious_tokens) / max(len(text) / 1000.0, 1.0)
        issues = []
        gate_status = "pass"

        if empty_pages:
            gate_status = "fail"
            issues.append(f"empty pages: {empty_pages[:10]}")
        if ocr_failure_pages:
            gate_status = "fail"
            issues.append(f"OCR failures: {ocr_failure_pages[:10]}")
        if vision_failure_pages:
            gate_status = "fail"
            issues.append(f"vision failures: {vision_failure_pages[:10]}")
        if deferred_table_pages and gate_status == "pass":
            gate_status = "warn"
            issues.append(f"table parsing deferred: {deferred_table_pages[:10]}")

        if content_by_type["ocr_pages"] > 0:
            if (
                len(suspicious_tokens) >= self.qa_fail_merged_token_count
                or suspicious_density >= self.qa_fail_merged_token_density
            ):
                gate_status = "fail"
                issues.append(
                    f"excessive merged-token noise ({len(suspicious_tokens)} suspicious tokens, density {suspicious_density:.2f}/1k chars)"
                )
            elif (
                gate_status == "pass"
                and (
                    len(suspicious_tokens) >= self.qa_warn_merged_token_count
                    or suspicious_density >= self.qa_warn_merged_token_density
                )
            ):
                gate_status = "warn"
                issues.append(
                    f"merged-token noise ({len(suspicious_tokens)} suspicious tokens, density {suspicious_density:.2f}/1k chars)"
                )

        return {
            "markdown_path": str(markdown_path),
            "gate_status": gate_status,
            "issues": issues,
            "page_count": len(page_reports),
            "empty_pages": empty_pages,
            "blank_pages": blank_pages,
            "ocr_failure_pages": ocr_failure_pages,
            "vision_failure_pages": vision_failure_pages,
            "ocr_pages": content_by_type["ocr_pages"],
            "native_pages": content_by_type["native_pages"],
            "positioned_table_pages": content_by_type["positioned_table_pages"],
            "vision_pages": content_by_type["vision_pages"],
            "deferred_table_pages": deferred_table_pages,
            "suspicious_merged_token_count": len(suspicious_tokens),
            "suspicious_merged_token_density_per_1k_chars": round(suspicious_density, 2),
            "suspicious_merged_token_examples": [
                {"token": token, "count": count}
                for token, count in suspicious_counts.most_common(25)
            ],
            "page_reports": page_reports,
        }

    def _find_suspicious_tokens(self, text: str) -> list[str]:
        tokens = re.findall(r"\b[A-Za-z/&+-]{12,}\b", text)
        suspicious = []
        for token in tokens:
            normalized = re.sub(r"[^A-Za-z]", "", token).lower()
            if len(normalized) < 12:
                continue
            fragment_hits = sum(1 for fragment in SUSPICIOUS_MERGED_FRAGMENTS if fragment in normalized)
            if fragment_hits >= 2:
                suspicious.append(token)
        return suspicious

    def _run_vision_enrichment(self, page: fitz.Page) -> str:
        pix = page.get_pixmap(matrix=fitz.Matrix(self.vision_render_scale, self.vision_render_scale))
        img_data = pix.tobytes("png")
        attempts = max(1, self.vision_retries + 1)

        for attempt in range(1, attempts + 1):
            try:
                response = self.ollama_client.chat(
                    model=self.llm_model,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "Extract only the visual information that native text usually misses. "
                                "Describe charts with concrete numbers, rebuild tables in Markdown, and "
                                "transcribe trapped text from infographics or images. "
                                "If the page has no meaningful visual-only content, reply only with "
                                "'NO_VISUAL_CONTENT'."
                            ),
                        },
                        {
                            "role": "user",
                            "content": "Extract visual-only data from this document page image.",
                            "images": [img_data],
                        },
                    ],
                )
                return response["message"]["content"].strip()
            except Exception as exc:
                logger.warning(
                    "Vision enrichment attempt %s/%s failed on page %s: %s",
                    attempt,
                    attempts,
                    page.number + 1,
                    exc,
                )
                if attempt >= attempts:
                    break
                if self.reset_model_on_vision_failure:
                    self._reset_model_runner()

        return ""

    def _reset_model_runner(self):
        try:
            subprocess.run(
                ["ollama", "stop", self.llm_model],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=15,
            )
        except Exception as exc:
            logger.warning("Could not reset Ollama runner for %s: %s", self.llm_model, exc)
        self.ollama_client = ollama.Client(timeout=self.vision_timeout_seconds)

    def _reconstruct_positioned_rows(self, page: fitz.Page) -> str:
        """
        Rebuilds digital tables using word coordinates instead of flattened reading order.
        This is much faster and more stable than vision for dense text tables.
        """
        words = page.get_text("words")
        if not words:
            return ""

        rows: list[dict] = []
        for word in words:
            x0, y0, x1, y1, text, *_ = word
            center_y = (y0 + y1) / 2

            target_row = None
            for row in rows:
                if abs(row["y"] - center_y) <= 3:
                    target_row = row
                    break

            if target_row is None:
                target_row = {"y": center_y, "points": []}
                rows.append(target_row)

            target_row["points"].append((x0, x1, text))

        rows.sort(key=lambda row: row["y"])
        rendered_rows = []

        for row in rows:
            cells = []
            current_cell = []
            previous_x1 = None

            for x0, x1, text in sorted(row["points"], key=lambda item: item[0]):
                if previous_x1 is not None and x0 - previous_x1 > 20:
                    cells.append(" ".join(current_cell))
                    current_cell = [text]
                else:
                    current_cell.append(text)
                previous_x1 = x1

            if current_cell:
                cells.append(" ".join(current_cell))

            clean_cells = [cell.strip() for cell in cells if cell.strip()]
            if clean_cells:
                rendered_rows.append(" | ".join(clean_cells))

        return "\n".join(rendered_rows)
