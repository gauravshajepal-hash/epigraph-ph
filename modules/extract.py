import logging
import json
import re
import subprocess
import time
from pathlib import Path

import ollama
import yaml
from pydantic import BaseModel, Field, ValidationError

from .db import DatabaseManager

logger = logging.getLogger(__name__)

CLAIM_PUNCT_TRANSLATION = str.maketrans(
    {
        "\u00a0": " ",
        "\u2022": "-",
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

CLAIM_PHRASE_REPAIRS = (
    (r"\bissuedon\b", "issued on"),
    (r"\bdirectedthe\b", "directed the"),
    (r"\bPoliciesand\b", "Policies and"),
    (r"\bNumberof\b", "Number of"),
    (r"\bnumberof\b", "number of"),
    (r"\bNumberofdiagnosedHIvcasesbymodeof\b", "Number of diagnosed HIV cases by mode of"),
    (r"\bDistributionofdiagnosedpregnantwomenby\b", "Distribution of diagnosed pregnant women by"),
    (r"\bSincethefrstreported\b", "Since the first reported"),
    (r"\bSincethefirstreported\b", "Since the first reported"),
    (r"\bHIVcaseinthe\b", "HIV case in the"),
    (r"\bHIVinfectioninthe\b", "HIV infection in the"),
    (r"\bHIVcasesreportedto\b", "HIV cases reported to"),
    (r"\breportedcasessince\b", "reported cases since"),
    (r"\btherehavebeen\b", "there have been"),
    (r"\bAmongthe\b", "Among the"),
    (r"\bAmongthem\b", "Among them"),
    (r"\bOfthe\b", "Of the"),
    (r"\bPhilippinesin\b", "Philippines in"),
    (r"\bGuidelinesinthe\b", "Guidelines in the"),
    (r"\bConductof\b", "Conduct of"),
    (r"\bA totalof\b", "A total of"),
    (r"\breportedin\b", "reported in"),
    (r"\bwithinformation\b", "with information"),
    (r"\bonriskreduction\b", "on risk reduction"),
    (r"\btorelevant\b", "to relevant"),
    (r"\btheseguidelines\b", "these guidelines"),
    (r"\bthisregard\b", "this regard"),
    (r"\barerequested\b", "are requested"),
    (r"\btolead\b", "to lead"),
    (r"\bcapacitybuilding\b", "capacity building"),
    (r"\bDOHRegional\b", "DOH Regional"),
    (r"\bOfficestofacilitate\b", "Offices to facilitate"),
    (r"\bFacilitiesoffering\b", "Facilities offering"),
    (r"\bHIVclinics\b", "HIV clinics"),
    (r"\bServicesTeam\b", "Services Team"),
    (r"\bProgramof\b", "Program of"),
    (r"\bTBpatients\b", "TB patients"),
    (r"\bCOVID 19\b", "COVID-19"),
    (r"\bstillbe\b", "still be"),
    (r"\bobjectiveof\b", "objective of"),
    (r"\bactivitieshave\b", "activities have"),
    (r"\bHIVTesting\b", "HIV Testing"),
    (r"\bHIVTreatment\b", "HIV Treatment"),
    (r"\bHIVCounselor\b", "HIV Counselor"),
    (r"\bHIVCounseling\b", "HIV Counseling"),
    (r"\bHIVScreening\b", "HIV Screening"),
    (r"\brapidHIV\b", "rapid HIV"),
    (r"\bfullblown\b", "full-blown"),
    (r"\bcasecontrol\b", "case-control"),
    (r"\bcommunityacquired\b", "community-acquired"),
    (r"\binjectiondrug\b", "injection-drug"),
    (r"\bprimarycare\b", "primary care"),
    (r"\bhealthcare\b", "health care"),
    (r"\bhealthcareworkers\b", "health care workers"),
    (r"\bhealthcareworker\b", "health care worker"),
    (r"\bserviceproviders\b", "service providers"),
    (r"\bserviceprovider\b", "service provider"),
    (r"\bcommunitybased\b", "community-based"),
    (r"\bhealthfacility\b", "health facility"),
    (r"\bcarefacility\b", "care facility"),
    (r"\bantiretroviraltherapy\b", "antiretroviral therapy"),
    (r"\bperformedby\b", "performed by"),
    (r"\binthe\b", "in the"),
    (r"\btherewere\b", "there were"),
    (r"\btherewere(?=\d)", "there were "),
    (r"\breportedto\b", "reported to"),
    (r"\bindividualsreported\b", "individuals reported"),
    (r"\bcasesreported\b", "cases reported"),
    (r"\bweremaleand\b", "were male and"),
    (r"\bwerefemaler\b", "were female"),
    (r"\btimeof\b", "time of"),
    (r"\bpermonth\b", "per month"),
    (r"\bInthe\b", "In the"),
    (r"\bMonth&Year\b", "Month & Year"),
    (r"\btheHARP\b", "the HARP"),
    (r"\bUnitsby\b", "Units by"),
    (r"\bARTRegistry\b", "ART Registry"),
    (r"\bARTRegistryofthe\b", "ART Registry of the"),
    (r"\bARTRegistryof\b", "ART Registry of"),
    (r"\bHIvcases\b", "HIV cases"),
    (r"\bcasesbymodeof\b", "cases by mode of"),
    (r"\bdiagnosedwomenwerereported\b", "diagnosed women were reported"),
    (r"\bdiagnosedwomenwasreported\b", "diagnosed women was reported"),
    (r"\bdiagnosedpregnantcaseswith\b", "diagnosed pregnant cases with"),
    (r"\bdiagnosedTGwby\b", "diagnosed TGW by"),
    (r"\bdiagnosedTGWby\b", "diagnosed TGW by"),
    (r"\bpregnantwomenby\b", "pregnant women by"),
    (r"\bTGWdiagnosedfrom\b", "TGW diagnosed from"),
    (r"\bTGWwere\b", "TGW were"),
    (r"\bdiagnosedfrom\b", "diagnosed from"),
    (r"\bmanagedin\b", "managed in"),
    (r"\bsentfor\b", "sent for"),
    (r"\btherecommendations\b", "the recommendations"),
    (r"\bnationalreference\b", "national reference"),
    (r"\bsendinglaboratory\b", "sending laboratory"),
    (r"\bdatatobegatheredfromtheclient\b", "data to be gathered from the client"),
    (r"\bACCESSTBproject\b", "ACCESS TB project"),
)

# --- Pydantic Schema ---

class Citation(BaseModel):
    source_url: str = Field(description="The exact HTTP URL where the original PDF can be found. Fallback to 'manual://filename.pdf' if dropped locally.")
    page_index: int = Field(ge=0, description="The 0-based index of the logical page where the claim physically resides in the source document.")
    line_offset: int | None = Field(default=None, description="Optional. The approximate line number in the extracted Markdown text.")
    snippet: str = Field(min_length=50, max_length=500, description="The EXACT verbatim substring from the source document that strictly proves the claim. DO NOT ALTER OR SUMMARIZE THIS.")

class KnowledgePoint(BaseModel):
    reasoning_trace: str = Field(default="", description="Step-by-step logical explanation of why this finding is epidemiologically significant for HIV/STI.")
    claim: str = Field(min_length=15, description="A standalone factual statement extracted from the document.")
    significance_score: float = Field(ge=0, le=1.0, description="Internal score of how critical this finding is to the HIV/STI knowledge base.")
    metadata: dict = Field(default_factory=dict, description="Flexible dictionary for specific values, entities, or categories found.")
    citation: Citation
    confidence: float = Field(ge=0, le=1.0, description="Confidence that the claim is purely supported by the verbatim snippet.")

class ExtractedPayload(BaseModel):
    findings: list[KnowledgePoint] = Field(
        default_factory=list,
        description="List of significant epidemiological findings.",
        validation_alias="points",
    )

# --- Extraction Engine ---

class ExtractionManager:
    """Routes Markdown to local Qwen via Ollama and validates against Pydantic models."""
    def __init__(
        self,
        db: DatabaseManager,
        config_path: str = "config.yaml",
        worker_id: str = "extractor",
        folder_filters: list[str] | None = None,
    ):
        self.db = db
        self.worker_id = worker_id
        self.folder_filters = folder_filters or []
        
        # Load constraints
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
            extraction_cfg = cfg.get('extraction', {})
            paths_cfg = cfg.get('paths', {})
            self.llm_model = extraction_cfg.get('model', 'qwen3.5:9b')
            self.markdown_dir = Path(paths_cfg.get('markdown', 'data/processed_md'))
            self.chunk_chars = int(extraction_cfg.get('chunk_chars', 8000))
            self.max_chunks_per_doc = int(extraction_cfg.get('max_chunks_per_doc', 40))
            self.enable_llm_fallback = bool(extraction_cfg.get('enable_llm_fallback', False))
            self.llm_timeout_seconds = int(extraction_cfg.get('llm_timeout_seconds', 45))
            self.llm_retries = int(extraction_cfg.get('llm_retries', 1))
            self.reset_model_on_timeout = bool(extraction_cfg.get('reset_model_on_timeout', True))
        self.ollama_client = ollama.Client(timeout=self.llm_timeout_seconds)
            
    def process_pending_documents(self, limit=None):
        """Processes 'parsed' markdown files."""
        claim_limit = limit or 1
        docs = self.db.claim_documents(
            "parsed",
            "extracting",
            claim_limit,
            self.worker_id,
            folder_filters=self.folder_filters,
        )
        logger.info(f"Claimed {len(docs)} documents pending local extraction.")
        
        if not docs:
            return 0
        
        # Start a run boundary
        run_id = self.db.start_run(model_used=self.llm_model)
        processed_count = 0
        
        for doc in docs:
            result = self.extract_document(
                doc,
                run_id=run_id,
                update_status_on_success=True,
                update_status_on_failure=True,
            )
            if result["success"]:
                processed_count += 1
                
        self.db.finish_run(run_id, processed_count)
        return processed_count

    def extract_document(
        self,
        doc,
        run_id: int,
        update_status_on_success: bool = False,
        update_status_on_failure: bool = False,
    ) -> dict:
        doc_id = doc["id"]
        md_path = self.markdown_dir / f"{Path(doc['local_path']).stem}.md"

        if not md_path.exists():
            logger.error(f"Missing markdown cache for doc {doc_id} at {md_path}")
            if update_status_on_failure:
                self.db.update_document_status(doc_id, "failed")
            return {"success": False, "points_saved": 0, "run_id": run_id, "error": "missing_markdown"}

        try:
            with open(md_path, 'r', encoding='utf-8') as f:
                text_content = f.read()

            logger.info(f"Discovery Mode: Extracting from doc {doc_id} using {self.llm_model}...")
            self.db.touch_document_claim(doc_id, self.worker_id)

            chunks = self._chunk_markdown(text_content)
            logger.info("Doc %s split into %s extraction chunks.", doc_id, len(chunks))
            findings = []
            for chunk_index, chunk_text in enumerate(chunks, start=1):
                self.db.touch_document_claim(doc_id, self.worker_id)
                chunk_start = time.time()
                chunk_findings, deterministic_strategy = self._deterministic_chunk_findings(
                    chunk_text,
                    doc["url"],
                )
                if deterministic_strategy:
                    logger.info(
                        "Chunk %s/%s for doc %s handled by deterministic '%s' extraction (%s findings).",
                        chunk_index,
                        len(chunks),
                        doc_id,
                        deterministic_strategy,
                        len(chunk_findings),
                    )
                else:
                    logger.info("Extracting chunk %s/%s for doc %s...", chunk_index, len(chunks), doc_id)
                    payload = self._extract_chunk_with_recovery(
                        chunk_text,
                        doc["url"],
                        doc_id,
                        chunk_index,
                        len(chunks),
                    )
                    chunk_findings = payload.findings or self._fallback_table_findings(chunk_text, doc["url"])

                findings.extend(chunk_findings)
                self.db.touch_document_claim(doc_id, self.worker_id)
                logger.info(
                    "Chunk %s/%s for doc %s completed in %.1fs (%s findings).",
                    chunk_index,
                    len(chunks),
                    doc_id,
                    time.time() - chunk_start,
                    len(chunk_findings),
                )

            points_saved = self._insert_findings(doc_id, run_id, text_content, findings)
            logger.info(f"Saved {points_saved} verified findings from doc {doc_id}.")
            if update_status_on_success:
                self.db.update_document_status(doc_id, "extracted")
            return {"success": True, "points_saved": points_saved, "run_id": run_id}

        except Exception as e:
            logger.error(f"Extraction failed for doc {doc_id}: {e}")
            if update_status_on_failure:
                self.db.update_document_status(doc_id, "failed")
            return {"success": False, "points_saved": 0, "run_id": run_id, "error": str(e)}

    def _insert_findings(
        self,
        doc_id: int,
        run_id: int,
        text_content: str,
        findings: list[KnowledgePoint],
    ) -> int:
        points_saved = 0
        seen_keys = set()
        for kp in findings:
            normalized_claim = self._normalize_claim_text(kp.claim)
            dedupe_key = (
                normalized_claim.strip().lower(),
                kp.citation.page_index,
                kp.citation.snippet.strip().lower(),
            )
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)

            if not self._verify_verbatim(text_content, kp.citation.snippet):
                logger.warning(
                    "Hallucination blocked: snippet for claim '%s' not found in source.",
                    self._safe_log_preview(kp.claim),
                )
                continue

            db_payload = {
                "claim": normalized_claim,
                "category": kp.metadata.get("category", "General Discovery"),
                "value": str(kp.metadata.get("value", kp.significance_score)),
                "citation": kp.citation.model_dump(),
                "confidence": kp.confidence
            }

            pid = self.db.insert_knowledge_point(doc_id, run_id, db_payload)
            if pid:
                points_saved += 1
        return points_saved

    def _build_deterministic_knowledge_point(
        self,
        *,
        reasoning_trace: str,
        claim: str,
        significance_score: float,
        metadata: dict,
        source_url: str,
        page_index: int,
        snippet: str,
        confidence: float,
        line_offset: int | None = None,
    ) -> KnowledgePoint | None:
        normalized_claim = self._normalize_claim_text(claim).strip()
        normalized_snippet = (snippet or "").strip()

        if len(normalized_claim) < 15:
            logger.warning(
                "Skipping invalid deterministic claim on page %s: too short after cleanup (%s)",
                page_index + 1,
                self._safe_log_preview(normalized_claim or claim),
            )
            return None

        if len(normalized_snippet) < 50:
            logger.warning(
                "Skipping deterministic claim on page %s: verbatim snippet too short (%s)",
                page_index + 1,
                self._safe_log_preview(normalized_claim),
            )
            return None

        try:
            return KnowledgePoint(
                reasoning_trace=reasoning_trace,
                claim=normalized_claim,
                significance_score=significance_score,
                metadata=metadata,
                citation=Citation(
                    source_url=source_url,
                    page_index=page_index,
                    line_offset=line_offset,
                    snippet=normalized_snippet[:500],
                ),
                confidence=confidence,
            )
        except ValidationError as exc:
            logger.warning(
                "Skipping invalid deterministic claim on page %s: %s (%s)",
                page_index + 1,
                exc.errors()[0].get("msg", str(exc)) if exc.errors() else str(exc),
                self._safe_log_preview(normalized_claim),
            )
            return None

    def _normalize_claim_text(self, text: str) -> str:
        if not text:
            return ""

        cleaned = text.translate(CLAIM_PUNCT_TRANSLATION)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        cleaned = re.sub(r"^\?\s+", "- ", cleaned)
        for pattern, replacement in CLAIM_PHRASE_REPAIRS:
            cleaned = re.sub(pattern, replacement, cleaned)

        cleaned = re.sub(r"\bHIVpositive\b", "HIV-positive", cleaned)
        cleaned = re.sub(r"\bHIVnegative\b", "HIV-negative", cleaned)
        cleaned = re.sub(r"\bHIVinfected\b", "HIV-infected", cleaned)
        cleaned = re.sub(r"\bHIVinfection\b", "HIV infection", cleaned)
        cleaned = re.sub(r"\bHIVproficient\b", "HIV-proficient", cleaned)
        cleaned = re.sub(r"\bHIVrelated\b", "HIV-related", cleaned)
        cleaned = re.sub(r"\bHIVexposed\b", "HIV-exposed", cleaned)
        cleaned = re.sub(r"\bHIVpositiveOFWs\b", "HIV-positive OFWs", cleaned)
        cleaned = re.sub(r"\baccording tothe\b", "according to the", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bNRL-SLH/SACCLorits\b", "NRL-SLH/SACCL or its", cleaned)
        cleaned = re.sub(r"\bDOHEBFormA/A-MC\b", "DOH EB Form A/A-MC", cleaned)
        cleaned = re.sub(r"\bDOH-EBFormA/A-MC\b", "DOH-EB Form A/A-MC", cleaned)
        cleaned = re.sub(r"\bDOHEBForm\b", "DOH EB Form", cleaned)
        cleaned = re.sub(r"\bFigure(?=\d)", "Figure ", cleaned)
        cleaned = re.sub(r"\bTable(?=\d)", "Table ", cleaned)
        cleaned = re.sub(r"(?<=[A-Za-z])&(?=[A-Za-z])", " & ", cleaned)
        cleaned = re.sub(r"(?<=[a-z])(?=[A-Z][a-z])", " ", cleaned)
        cleaned = re.sub(r"(?<=[a-z])(?=[A-Z]{2,}\b)", " ", cleaned)
        cleaned = re.sub(r"(?<=[A-Za-z])(?=\d{4}\b)", " ", cleaned)
        cleaned = re.sub(r"(?<=\d)(?=[a-z]{3,}\b)", " ", cleaned)
        cleaned = re.sub(r"\b(\d+)\s+(st|nd|rd|th)\b", r"\1\2", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\b(\d{1,2})\s+([A-Z])\b", r"\1\2", cleaned)
        cleaned = re.sub(r"(?<![\d,])(\d{1,3}),\s+(\d{3}\b)", r"\1,\2", cleaned)
        cleaned = re.sub(r"(?<=,)(?=[A-Za-z])", " ", cleaned)
        cleaned = re.sub(r"(?<=[;:])(?=[A-Za-z0-9])", " ", cleaned)
        cleaned = re.sub(r"(?<=[a-z0-9])\.(?=[A-Z])", ". ", cleaned)
        cleaned = re.sub(r"(?<=[\]\)])\.(?=[A-Z])", ". ", cleaned)
        cleaned = re.sub(r"(?<=\))(?=[A-Za-z0-9])", " ", cleaned)
        cleaned = re.sub(r"\bNo\.(?=\d)", "No. ", cleaned)
        cleaned = re.sub(r"\bHIV[- ]?positive\b", "HIV-positive", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bHIV[- ]?negative\b", "HIV-negative", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bHIV[- ]?infected\b", "HIV-infected", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bHIV-positive(?=[A-Za-z])", "HIV-positive ", cleaned)
        cleaned = re.sub(r"\bHIV-negative(?=[A-Za-z])", "HIV-negative ", cleaned)
        cleaned = re.sub(r"\bHIV-infected(?=[A-Za-z])", "HIV-infected ", cleaned)
        cleaned = re.sub(r"\b([A-Za-z-]+)percent\b", r"\1 percent", cleaned)
        cleaned = re.sub(r"percent(?=\()", "percent ", cleaned)
        cleaned = re.sub(r"\bweremaleand(?=\d)", "were male and ", cleaned)
        cleaned = re.sub(r"\bBy age group(?=\d)", "By age group ", cleaned)
        cleaned = re.sub(r"\btotalof(?=\d)", "total of ", cleaned)
        cleaned = re.sub(r"\bof(?=\d)", "of ", cleaned)
        cleaned = re.sub(r"\bto(?=\d)", "to ", cleaned)
        cleaned = re.sub(r"\bbeen(?=\d)", "been ", cleaned)
        cleaned = re.sub(r"\baged(?=\d)", "aged ", cleaned)
        cleaned = re.sub(r"\btothe\b", "to the", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;:")
        return cleaned

    def _chunk_markdown(self, markdown_text: str) -> list[str]:
        body = markdown_text.strip()
        if not body:
            return []

        page_splits = re.split(r"(?=^## \[Page \d+\])", body, flags=re.MULTILINE)
        chunks = []
        for part in page_splits:
            part = part.strip()
            if not part:
                continue
            if len(part) <= self.chunk_chars:
                chunks.append(part)
                continue

            lines = part.splitlines()
            current = []
            current_len = 0
            for line in lines:
                line_len = len(line) + 1
                if current and current_len + line_len > self.chunk_chars:
                    chunks.append("\n".join(current))
                    current = [line]
                    current_len = line_len
                else:
                    current.append(line)
                    current_len += line_len
            if current:
                chunks.append("\n".join(current))

        return chunks[: self.max_chunks_per_doc] or [body[: self.chunk_chars]]
        
    def _extract_chunk(self, markdown_text: str, source_url: str) -> ExtractedPayload:
        """Calls local Ollama with structured output formatting."""
        schema = ExtractedPayload.model_json_schema()
        
        system_prompt = f"""
        You are extracting structured HIV/STI evidence from ONE document page or page fragment.
        Prefer fewer, higher-quality findings over broad summaries.
        
        TRUST BUT VERIFY MANDATE:
        - The 'reasoning_trace' can be brief.
        - The 'snippet' field MUST be EXACTLY verbatim from the text. DO NOT change words, fix typos, or summarize. 
        - If you hallucinate a snippet, it will be detected and the data will be discarded.
        - If a chunk contains no useful HIV/STI evidence, return exactly: {{"points": []}}
        
        WHAT TO EXTRACT:
        - Statistics on HIV, AIDS, ART, testing, viral load, mortality, transmission, demographics, or region.
        - Policy or guideline statements.
        - Table rows and chart facts if they are clearly stated in the text.

        RULES:
        - Extract at most 6 findings from this chunk.
        - For table-like content, each claim should preserve the region / cohort / metric / value relationship.
        - Use metadata keys like category, metric, region, period, subgroup, value when available.
        
        OUTPUT FORMAT: 
        JSON object strictly following this schema:
        {json.dumps(schema, indent=2)}
        """
        
        response = self.ollama_client.chat(
            model=self.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Discover all significant HIV/STI findings in this text chunk. Use the provided source URL: {source_url}\n\nTEXT:\n{markdown_text}"}
            ],
            format='json',
        )
        
        raw_content = response['message']['content']
        return self._coerce_payload(raw_content)

    def _extract_chunk_with_recovery(
        self,
        markdown_text: str,
        source_url: str,
        doc_id: int,
        chunk_index: int,
        total_chunks: int,
    ) -> ExtractedPayload:
        attempts = max(1, self.llm_retries + 1)
        for attempt in range(1, attempts + 1):
            try:
                return self._extract_chunk(markdown_text, source_url)
            except Exception as exc:
                logger.warning(
                    "LLM extraction failed for doc %s chunk %s/%s on attempt %s/%s: %s",
                    doc_id,
                    chunk_index,
                    total_chunks,
                    attempt,
                    attempts,
                    exc,
                )
                if attempt >= attempts:
                    break
                if self.reset_model_on_timeout:
                    self._reset_model_runner()

        logger.warning(
            "Skipping doc %s chunk %s/%s after repeated LLM failures.",
            doc_id,
            chunk_index,
            total_chunks,
        )
        return ExtractedPayload(findings=[])

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
        self.ollama_client = ollama.Client(timeout=self.llm_timeout_seconds)

    def _coerce_payload(self, raw_content: str) -> ExtractedPayload:
        if not raw_content or not raw_content.strip():
            return ExtractedPayload(findings=[])

        try:
            payload = json.loads(raw_content)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", raw_content, re.DOTALL)
            if not match:
                return ExtractedPayload(findings=[])
            payload = json.loads(match.group(0))

        if isinstance(payload, list):
            payload = {"points": payload}
        elif not isinstance(payload, dict):
            payload = {"points": []}

        if "points" not in payload and "findings" in payload:
            payload["points"] = payload["findings"]
        if "points" not in payload:
            payload["points"] = []

        return ExtractedPayload.model_validate(payload)

    def _deterministic_chunk_findings(self, markdown_text: str, source_url: str) -> tuple[list[KnowledgePoint], str | None]:
        stripped = markdown_text.lstrip()
        if stripped.startswith("--- SOURCE INFO ---"):
            return [], "source-skip"

        if self._looks_skippable_chunk(markdown_text):
            return [], "skip"

        block_findings = self._fallback_block_table_findings(markdown_text, source_url)
        if block_findings:
            return block_findings, "block-table"

        if self._is_structured_table_chunk(markdown_text):
            return self._fallback_table_findings(markdown_text, source_url), "pipe-table"

        line_findings = self._fallback_quantitative_line_findings(markdown_text, source_url)
        if line_findings:
            return line_findings, "quant-line"

        narrative_findings = self._fallback_narrative_findings(markdown_text, source_url)
        if narrative_findings:
            return narrative_findings, "narrative"

        if self._should_skip_low_signal_chunk(markdown_text):
            return [], "skip-low-signal"

        if not self._should_route_to_llm(markdown_text):
            return [], "skip-low-signal"

        return [], None

    def _is_structured_table_chunk(self, markdown_text: str) -> bool:
        candidate_rows = 0
        for line in markdown_text.splitlines():
            cells = [cell.strip() for cell in line.split("|") if cell.strip()]
            if len(cells) < 4:
                continue
            numeric_cells = [cell for cell in cells[1:] if any(ch.isdigit() for ch in cell)]
            if len(numeric_cells) >= 2:
                candidate_rows += 1
        return candidate_rows >= 4

    def _fallback_table_findings(self, markdown_text: str, source_url: str) -> list[KnowledgePoint]:
        lines = [line.strip() for line in markdown_text.splitlines() if line.strip()]
        page_match = re.search(r"## \[Page (\d+)\]", markdown_text)
        page_index = int(page_match.group(1)) - 1 if page_match else 0
        context = ""
        findings = []

        for line in lines:
            lowered = line.lower()
            if lowered.startswith("table ") or lowered.startswith("figure "):
                context = self._sanitize_table_context(line)
                continue
            if "|" not in line:
                if self._looks_like_table_context(line):
                    context = line
                continue

            cells = [cell.strip() for cell in line.split("|") if cell.strip()]
            if len(cells) < 4:
                continue

            row_label = cells[0]
            value_cells = self._extract_table_value_cells(cells[1:])
            if len(value_cells) < 2:
                continue
            if row_label.lower() in {"region of", "treatment", "facility", "alive on", "tested for vl"}:
                continue
            if row_label.startswith("%"):
                continue

            context_prefix = context or "Structured surveillance table"
            claim = f"{context_prefix}: {row_label} has values {', '.join(value_cells)}."
            snippet = self._verbatim_table_snippet(markdown_text, line)
            kp = self._build_deterministic_knowledge_point(
                reasoning_trace="Deterministic extraction from a pipe-delimited table row.",
                claim=claim,
                significance_score=0.85,
                metadata={
                    "category": "Structured Table Row",
                    "table_context": context_prefix,
                    "row_label": row_label,
                    "value": " | ".join(value_cells),
                },
                source_url=source_url,
                page_index=page_index,
                snippet=snippet,
                confidence=0.95,
            )
            if kp:
                findings.append(kp)

            if len(findings) >= 20:
                break

        return findings

    def _fallback_quantitative_line_findings(self, markdown_text: str, source_url: str) -> list[KnowledgePoint]:
        page_index = self._extract_page_index(markdown_text)
        lines = self._clean_chunk_lines(markdown_text)
        findings = []
        context_line = ""

        for line in lines:
            lowered = line.lower()
            if self._looks_like_table_context(line) or self._contains_hiv_marker(line):
                context_line = line

            if not self._is_quantitative_evidence_line(line):
                continue

            snippet_seed = line
            claim_text = line
            if (
                context_line
                and context_line != line
                and not self._contains_hiv_marker(line)
                and not self._looks_like_table_context(line)
            ):
                snippet_seed = f"{context_line} {line}"
                claim_text = snippet_seed

            snippet = self._verbatim_narrative_snippet(markdown_text, snippet_seed)
            if len(snippet) < 50:
                continue

            kp = self._build_deterministic_knowledge_point(
                reasoning_trace="Deterministic extraction from a quantitative prose line.",
                claim=self._sanitize_narrative_sentence(claim_text)[:500],
                significance_score=0.82,
                metadata={
                    "category": "Quantitative Narrative",
                    "value": "narrative-line",
                },
                source_url=source_url,
                page_index=page_index,
                snippet=snippet,
                confidence=0.93,
            )
            if kp:
                findings.append(kp)

            if len(findings) >= 8:
                break

        return findings

    def _fallback_narrative_findings(self, markdown_text: str, source_url: str) -> list[KnowledgePoint]:
        page_index = self._extract_page_index(markdown_text)
        filtered_lines = self._clean_chunk_lines(markdown_text)
        compact_text = re.sub(r"\s+", " ", " ".join(filtered_lines)).strip()
        if not compact_text:
            return []

        findings = []
        sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9<(])", compact_text)
        for sentence in sentences:
            claim_text = self._sanitize_narrative_sentence(sentence)
            if not self._looks_like_stat_sentence(claim_text):
                continue
            snippet = self._verbatim_narrative_snippet(markdown_text, sentence)
            if len(snippet) < 50:
                continue

            kp = self._build_deterministic_knowledge_point(
                reasoning_trace="Deterministic extraction from a numeric narrative sentence.",
                claim=claim_text,
                significance_score=0.8,
                metadata={
                    "category": "Narrative Statistic",
                    "value": "narrative",
                },
                source_url=source_url,
                page_index=page_index,
                snippet=snippet,
                confidence=0.92,
            )
            if kp:
                findings.append(kp)
            if len(findings) >= 8:
                break

        return findings

    def _fallback_block_table_findings(self, markdown_text: str, source_url: str) -> list[KnowledgePoint]:
        page_index = self._extract_page_index(markdown_text)
        context_headers = {"AGE GROUP", "KEY POPULATION", "REGION"}
        ignored_headers = {
            "ESTIMATED",
            "PLHIV",
            "DIAGNOSED",
            "1st 95)",
            "ON ART",
            "2nd 95",
            "VL TESTED",
            "VL TESTING",
            "COVERAGE (VL",
            "Tested/On ART)",
            "VL",
            "SUPPRESSED",
            "SUPPRESSION",
            "AMONG",
            "TESTED",
            "3rd 95",
            "(VL",
            "(VL Tested/",
            "(VL Tested/ On",
            "Suppressed",
            "Suppressed/",
            "/ On ART)",
            "On ART)",
            "ART)",
            "Dx = Diagnosed",
            "- Annex",
            "KEY",
            "POPULATION",
        }

        lines = [line.strip() for line in markdown_text.splitlines() if line.strip()]
        if "Care Cascade by" not in markdown_text and "REGION" not in lines:
            return []

        findings = []
        current_context = ""
        i = 0
        while i < len(lines):
            line = lines[i]
            lowered = line.lower()
            if lowered == "care cascade by age group" or lowered == "age group":
                current_context = "AGE GROUP"
                i += 1
                continue
            if lowered == "care cascade by key population":
                current_context = "KEY POPULATION"
                i += 1
                continue
            if lowered == "care cascade by region":
                current_context = "REGION"
                i += 1
                continue
            if line == "KEY" and i + 1 < len(lines) and lines[i + 1] == "POPULATION":
                current_context = "KEY POPULATION"
                i += 2
                continue
            if line in context_headers:
                current_context = line
                i += 1
                continue
            if not current_context or line in ignored_headers or line.startswith("Note:") or lowered.startswith("footnote:"):
                i += 1
                continue

            row_label = ""
            row_lines = []
            if current_context == "REGION" and self._looks_like_region_label(line):
                row_label = line
                row_lines = [line]
                i += 1
            else:
                if self._looks_like_table_value(line):
                    i += 1
                    continue
                label_parts = [line]
                row_lines = [line]
                i += 1
                while i < len(lines):
                    candidate = lines[i]
                    candidate_lower = candidate.lower()
                    if candidate in context_headers or candidate in ignored_headers or candidate.startswith("Note:") or candidate_lower.startswith("footnote:"):
                        break
                    if self._looks_like_parenthetical_label(candidate):
                        label_parts.append(candidate)
                        row_lines.append(candidate)
                        i += 1
                        continue
                    if self._looks_like_table_value(candidate):
                        break
                    if current_context == "REGION" and self._looks_like_region_label(candidate):
                        break
                    label_parts.append(candidate)
                    row_lines.append(candidate)
                    i += 1
                row_label = " ".join(label_parts)

            if not row_label:
                continue

            value_cells = []
            while i < len(lines):
                candidate = lines[i]
                candidate_lower = candidate.lower()
                if candidate in context_headers or candidate.startswith("Note:") or candidate_lower.startswith("footnote:"):
                    break
                if (
                    current_context == "REGION"
                    and self._looks_like_region_label(candidate)
                    and len(value_cells) >= 8
                ):
                    break
                if (
                    current_context == "AGE GROUP"
                    and self._looks_like_age_group_start(candidate)
                    and len(value_cells) >= 8
                ):
                    break
                if (
                    current_context == "KEY POPULATION"
                    and self._looks_like_key_population_start(candidate)
                    and len(value_cells) >= 8
                ):
                    break
                if not self._looks_like_table_value(candidate):
                    break
                value_cells.append(candidate)
                row_lines.append(candidate)
                i += 1
                if len(value_cells) >= 10:
                    break

            if len(value_cells) < 5:
                continue

            snippet = self._verbatim_block_snippet(markdown_text, row_lines)
            claim = f"Care Cascade {current_context.title()}: {row_label} has values {', '.join(value_cells)}."
            kp = self._build_deterministic_knowledge_point(
                reasoning_trace="Deterministic extraction from an annex care cascade table row.",
                claim=claim,
                significance_score=0.86,
                metadata={
                    "category": f"Care Cascade {current_context.title()}",
                    "row_label": row_label,
                    "value": " | ".join(value_cells),
                },
                source_url=source_url,
                page_index=page_index,
                snippet=snippet,
                confidence=0.95,
            )
            if kp:
                findings.append(kp)

            if len(findings) >= 25:
                break

        return findings

    def _extract_table_value_cells(self, cells: list[str]) -> list[str]:
        values = []
        for cell in cells:
            if self._looks_like_table_value(cell):
                values.append(cell)
                continue
            if values:
                break
        return values

    def _looks_like_table_value(self, cell: str) -> bool:
        text = cell.strip()
        if not text or not any(ch.isdigit() for ch in text):
            return False

        lowered = text.lower()
        narrative_markers = (
            " as of ",
            "were ",
            "these",
            "among",
            "individuals",
            "treatment",
            "follow-up",
            "reported",
            "migrating",
        )
        if any(marker in lowered for marker in narrative_markers):
            return False

        alpha_count = sum(ch.isalpha() for ch in text)
        if len(text) > 24 and alpha_count > 6:
            return False

        return True

    def _looks_like_region_label(self, text: str) -> bool:
        return bool(re.fullmatch(r"(?:[0-9]{1,2}[A-Z]?|NCR|NIR|CAR|CARAGA|BARMM)", text.strip()))

    def _looks_like_parenthetical_label(self, text: str) -> bool:
        stripped = text.strip()
        if not re.fullmatch(r"\([^)]{1,40}\)", stripped):
            return False
        return not self._looks_like_region_label(stripped.strip("()"))

    def _looks_like_table_context(self, text: str) -> bool:
        stripped = text.strip()
        lowered = stripped.lower()
        if stripped.startswith("Table ") or stripped.startswith("Figure "):
            return True
        if "|" in stripped:
            return False
        keywords = (
            "age group",
            "mode of transmission",
            "treatment outcome",
            "viral load",
            "care cascade",
            "region of",
            "sex assigned at birth",
            "current age",
            "percent increase",
            "number of",
        )
        return any(keyword in lowered for keyword in keywords)

    def _looks_like_age_group_start(self, text: str) -> bool:
        stripped = text.strip().upper()
        return stripped.startswith(("CHILDREN", "ADOLESCENTS", "YOUTH", "ADULTS"))

    def _looks_like_key_population_start(self, text: str) -> bool:
        stripped = text.strip().upper()
        return stripped.startswith(("MALES", "PERSONS", "OTHER MALES", "OTHER FEMALES"))

    def _looks_like_heading_line(self, text: str) -> bool:
        stripped = text.strip()
        if not stripped or len(stripped) > 80:
            return False
        if any(ch.isdigit() for ch in stripped):
            return False
        if any(punct in stripped for punct in ".!?;"):
            return False

        words = stripped.replace("/", " ").split()
        if not words or len(words) > 8:
            return False

        alpha_chars = [ch for ch in stripped if ch.isalpha()]
        if not alpha_chars:
            return False

        uppercase_ratio = sum(ch.isupper() for ch in alpha_chars) / len(alpha_chars)
        stopwords = {"and", "of", "with", "the", "by", "in", "on", "for", "to"}
        title_like = all(
            word.lower() in stopwords or word[:1].isupper()
            for word in words
            if word[:1].isalpha()
        )
        return uppercase_ratio > 0.65 or title_like

    def _sanitize_narrative_sentence(self, text: str) -> str:
        snippet = re.sub(r"\|\s*(Figure|Table)\s+\d+.*$", "", text).strip()
        snippet = re.sub(r"^OCT-DEC\s+\d{4}\s+", "", snippet).strip()
        snippet = re.sub(r"\s+", " ", snippet)

        starters = (
            "The ",
            "As ",
            "In ",
            "Since ",
            "Moreover,",
            "Further,",
            "Among ",
            "Compared ",
            "From ",
            "Of these,",
            "All ",
            "There ",
            "However,",
            "Specifically,",
            "During ",
            "Cumulatively,",
            "Diagnosed ",
        )
        earliest = len(snippet)
        for starter in starters:
            idx = snippet.find(starter)
            if idx > 0:
                earliest = min(earliest, idx)

        if earliest < len(snippet):
            prefix = snippet[:earliest]
            if any(token in prefix for token in ("Figure", "Table", "Number of", "Molina", "Officer", "Team")) or len(prefix.split()) <= 8:
                snippet = snippet[earliest:].strip()

        return snippet

    def _looks_like_stat_sentence(self, text: str) -> bool:
        if len(text) < 50 or len(text) > 500:
            return False

        lowered = text.lower()
        hiv_markers = (
            "hiv",
            "aids",
            "plhiv",
            "art",
            "viral load",
            "diagnosed",
            "diagnosis",
            "suppressed",
            "pregnant",
            "transgender",
            "mortality",
            "deaths",
            "care cascade",
            "transactional sex",
        )
        if not any(marker in lowered for marker in hiv_markers):
            return False

        if "@" in text or "http" in lowered or "tinyurl" in lowered:
            return False

        numeric_tokens = re.findall(r"(?:<)?\d[\d,]*(?:\.\d+)?%?", text)
        return len(numeric_tokens) >= 2

    def _clean_chunk_lines(self, markdown_text: str) -> list[str]:
        cleaned_text = re.sub(r"^## \[Page \d+\]\s*$", "", markdown_text, flags=re.MULTILINE)
        cleaned_text = re.sub(r"^#### \[[^\]]+\]\s*$", "", cleaned_text, flags=re.MULTILINE)
        cleaned_text = re.sub(r"--- SOURCE INFO ---.*?------------------", "", cleaned_text, flags=re.DOTALL)

        filtered_lines = []
        for raw_line in cleaned_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if self._is_page_artifact_line(line):
                continue
            if line.startswith("Figure ") or line.startswith("Table "):
                continue
            if self._looks_like_heading_line(line):
                continue
            filtered_lines.append(line)
        return self._coalesce_wrapped_lines(filtered_lines)

    def _coalesce_wrapped_lines(self, lines: list[str]) -> list[str]:
        if not lines:
            return []

        merged: list[str] = []
        buffer = lines[0]
        max_line_length = 360

        for line in lines[1:]:
            if self._should_merge_wrapped_line(buffer, line) and len(buffer) + len(line) + 1 <= max_line_length:
                if buffer.endswith("-"):
                    buffer = f"{buffer[:-1]}{line}"
                else:
                    buffer = f"{buffer} {line}"
                buffer = re.sub(r"\s+", " ", buffer).strip()
                continue

            merged.append(buffer)
            buffer = line

        if buffer:
            merged.append(buffer)
        return merged

    def _should_merge_wrapped_line(self, current: str, next_line: str) -> bool:
        current = current.strip()
        next_line = next_line.strip()
        if not current or not next_line:
            return False
        if "|" in current or "|" in next_line:
            return False
        if self._looks_like_heading_line(next_line):
            return False
        if self._looks_like_table_value(current) or self._looks_like_table_value(next_line):
            return False
        if next_line.startswith(("Figure ", "Table ", "Source:", "Note:")):
            return False
        if re.match(r"^(?:[0-9]+|[IVXivx]+|[A-Za-z])[.)]\s", next_line):
            return False
        if current.endswith((".", "?", "!", ";", ":")):
            return False
        if len(next_line.split()) <= 2 and next_line.upper() == next_line:
            return False
        if next_line[:1].islower():
            return True
        current_is_evidencey = (
            self._numeric_token_count(current) > 0
            or self._contains_hiv_marker(current)
            or self._contains_policy_marker(current)
        )
        if not current_is_evidencey and len(current.split()) < 14:
            return False
        return True

    def _numeric_token_count(self, text: str) -> int:
        return len(re.findall(r"(?:<)?\d[\d,]*(?:\.\d+)?%?", text))

    def _percent_token_count(self, text: str) -> int:
        return len(re.findall(r"\d[\d,]*(?:\.\d+)?%", text))

    def _contains_hiv_marker(self, text: str) -> bool:
        lowered = text.lower()
        markers = (
            "hiv",
            "aids",
            "plhiv",
            "art",
            "viral load",
            "diagnosed",
            "diagnosis",
            "suppressed",
            "testing",
            "infection",
            "infections",
            "case",
            "cases",
            "deaths",
            "mortality",
            "workers",
            "workplace",
        )
        return any(marker in lowered for marker in markers)

    def _contains_policy_marker(self, text: str) -> bool:
        lowered = text.lower()
        markers = (
            "policy",
            "policies",
            "guideline",
            "guidelines",
            "mandated",
            "must",
            "shall",
            "required",
            "principle",
            "compliance",
            "workplace response",
        )
        return any(marker in lowered for marker in markers)

    def _is_page_artifact_line(self, text: str) -> bool:
        stripped = text.strip()
        if not stripped:
            return True
        if stripped in {"Page !", "of !"}:
            return True
        if re.fullmatch(r"\d{1,4}", stripped):
            return True
        if re.fullmatch(r"Page\s+[!0-9 ]+of\s+[!0-9 ]+", stripped, flags=re.IGNORECASE):
            return True
        return False

    def _is_quantitative_evidence_line(self, text: str) -> bool:
        stripped = text.strip()
        if len(stripped) < 35 or len(stripped) > 420:
            return False
        if self._is_page_artifact_line(stripped):
            return False
        if stripped.startswith(("Date accessed:", "Web Link:", "Source:")):
            return False
        if "|" in stripped:
            return False

        numeric_tokens = self._numeric_token_count(stripped)
        if numeric_tokens == 0:
            return False

        lowered = stripped.lower()
        evidence_markers = (
            "%",
            "diagnosed",
            "infections",
            "cases",
            "workers",
            "establishments",
            "coverage",
            "tested",
            "facilities",
            "rate",
            "region",
            "regions",
            "ncr",
            "deaths",
        )
        if numeric_tokens >= 2 and any(marker in lowered for marker in evidence_markers):
            return True
        if self._contains_hiv_marker(stripped) and numeric_tokens >= 1:
            return True
        return False

    def _should_skip_low_signal_chunk(self, markdown_text: str) -> bool:
        lines = self._clean_chunk_lines(markdown_text)
        if not lines:
            return True

        compact_text = " ".join(lines)
        lowered = compact_text.lower()
        numeric_tokens = self._numeric_token_count(compact_text)
        percent_tokens = self._percent_token_count(compact_text)

        boilerplate_markers = (
            "illustrated below",
            "research and analysis framework",
            "study commissioned",
            "future policies and programs",
            "key informant",
            "date accessed",
            "web link",
        )
        if any(marker in lowered for marker in boilerplate_markers) and percent_tokens == 0:
            return True

        if numeric_tokens == 0 and not self._contains_policy_marker(compact_text):
            return True

        if len(compact_text) < 700 and percent_tokens == 0 and numeric_tokens <= 2:
            return True

        return False

    def _should_route_to_llm(self, markdown_text: str) -> bool:
        if not self.enable_llm_fallback:
            return False

        compact_text = " ".join(self._clean_chunk_lines(markdown_text))
        numeric_tokens = self._numeric_token_count(compact_text)
        percent_tokens = self._percent_token_count(compact_text)

        if numeric_tokens >= 8 and (percent_tokens >= 2 or self._contains_hiv_marker(compact_text)):
            return True

        if self._contains_policy_marker(compact_text) and len(compact_text) <= 1800:
            return True

        return False

    def _looks_skippable_chunk(self, markdown_text: str) -> bool:
        lowered = markdown_text.lower()
        if "editorial team" in lowered and "for further details or data requests" in lowered and "table " not in lowered:
            return True
        return False

    def _extract_page_index(self, markdown_text: str) -> int:
        page_match = re.search(r"## \[Page (\d+)\]", markdown_text)
        return int(page_match.group(1)) - 1 if page_match else 0

    def _sanitize_table_context(self, line: str) -> str:
        if "|" not in line:
            return line.strip()

        parts = [part.strip() for part in line.split("|") if part.strip()]
        if not parts:
            return line.strip()

        kept = []
        for part in parts:
            lowered = part.lower()
            if kept and any(marker in lowered for marker in ("additionally", "among", "moreover", "as of", "since", "from ")):
                break
            kept.append(part)

        return " ".join(kept).strip() or parts[0]

    def _verbatim_narrative_snippet(self, markdown_text: str, seed_text: str) -> str:
        normalized_text = markdown_text.replace("\r\n", "\n")
        target = seed_text.strip()
        if not target:
            return ""

        start = normalized_text.find(target)
        if start >= 0:
            end = min(len(normalized_text), start + max(len(target), 220))
            snippet = normalized_text[start:end].strip()
            if len(snippet) >= 50:
                return snippet[:500]

        compact_text = re.sub(r"\s+", " ", normalized_text)
        compact_target = re.sub(r"\s+", " ", target)
        start = compact_text.find(compact_target)
        if start >= 0:
            end = min(len(compact_text), start + max(len(compact_target), 220))
            snippet = compact_text[start:end].strip()
            if len(snippet) >= 50:
                return snippet[:500]

        normalized_span = self._find_normalized_span(normalized_text, target)
        if normalized_span is None:
            return ""

        start, end = normalized_span
        end = self._expand_narrative_end(normalized_text, start, end)
        snippet = normalized_text[start:end].strip()
        return snippet[:500]

    def _find_normalized_span(self, source_text: str, target_text: str) -> tuple[int, int] | None:
        source_chars = []
        source_map = []
        for idx, ch in enumerate(source_text):
            if ch.isalnum():
                source_chars.append(ch.lower())
                source_map.append(idx)

        target_chars = [ch.lower() for ch in target_text if ch.isalnum()]
        if not source_chars or not target_chars:
            return None

        joined_source = "".join(source_chars)
        joined_target = "".join(target_chars)
        start = joined_source.find(joined_target)
        if start < 0:
            return None

        end = start + len(joined_target) - 1
        return source_map[start], source_map[end] + 1

    def _expand_narrative_end(self, source_text: str, start: int, end: int, min_chars: int = 220) -> int:
        target_end = min(len(source_text), max(end, start + min_chars))
        while target_end < len(source_text) and source_text[target_end - 1] not in ".!?":
            target_end += 1
            if target_end - start >= 500:
                break
        return min(target_end, len(source_text))

    def _verbatim_block_snippet(self, markdown_text: str, row_lines: list[str]) -> str:
        normalized_text = markdown_text.replace("\r\n", "\n")
        if not row_lines:
            return ""

        search_candidates = []
        for width in (3, 2, 1):
            candidate = "\n".join(row_lines[: min(width, len(row_lines))]).strip()
            if candidate:
                search_candidates.append(candidate)

        for candidate in search_candidates:
            start = normalized_text.find(candidate)
            if start >= 0:
                window_start = max(0, start - 80)
                end = min(len(normalized_text), start + max(140, len(candidate) + 80))
                snippet = normalized_text[window_start:end].strip()
                if len(snippet) >= 50:
                    return snippet[:500]

        compact_text = re.sub(r"\s+", " ", normalized_text)
        compact_candidate = re.sub(r"\s+", " ", " ".join(row_lines)).strip()
        start = compact_text.find(compact_candidate)
        if start >= 0:
            window_start = max(0, start - 80)
            end = min(len(compact_text), start + max(140, len(compact_candidate) + 80))
            snippet = compact_text[window_start:end].strip()
            if len(snippet) >= 50:
                return snippet[:500]

        fallback = " ".join(row_lines).strip()
        if len(fallback) < 50:
            fallback = f"{normalized_text[max(0, len(normalized_text) - 160):].strip()}".strip()
        return fallback[:500]

    def _verbatim_table_snippet(self, markdown_text: str, row_line: str) -> str:
        normalized_text = markdown_text.replace("\r\n", "\n")
        target = row_line.strip()
        start = normalized_text.find(target)
        if start < 0:
            compact_text = re.sub(r"\s+", " ", normalized_text)
            compact_target = re.sub(r"\s+", " ", target)
            start = compact_text.find(compact_target)
            if start >= 0:
                end = min(len(compact_text), start + max(len(compact_target), 120))
                snippet = compact_text[start:end].strip()
                return snippet[:500]
            return target if len(target) >= 50 else (target + " [table row]")

        if len(target) >= 50:
            return target

        window_start = max(0, start - 30)
        window_end = min(len(normalized_text), start + len(target) + 120)
        snippet = normalized_text[window_start:window_end].strip()
        if len(snippet) < 50:
            snippet = normalized_text[start:min(len(normalized_text), start + len(target) + 180)].strip()
        return snippet[:500]

    def _safe_log_preview(self, text: str, limit: int = 50) -> str:
        preview = text[:limit]
        return preview.encode("ascii", "replace").decode("ascii")

    def _verify_verbatim(self, source_text: str, snippet: str) -> bool:
        """Normalized string matching to verify LLM snippets against source text."""
        import re
        
        def normalize(s: str) -> str:
            # Remove all whitespace and non-alphanumeric to focus on content
            return re.sub(r'[^a-zA-Z0-9]', '', s).lower()
        
        norm_source = normalize(source_text)
        norm_snippet = normalize(snippet)
        
        if not norm_snippet: return False
        return norm_snippet in norm_source
