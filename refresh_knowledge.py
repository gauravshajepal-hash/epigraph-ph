import argparse
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path

from modules.db import DatabaseManager
from modules.extract import ExtractionManager
from modules.insights import InsightsManager
from modules.normalize import NormalizationManager
from modules.parse import ParsingManager
from modules.verify import VerificationManager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def reparse_document(parse_manager: ParsingManager, doc: dict) -> tuple[bool, Path | None, dict | None, str | None]:
    doc_id = int(doc["id"])
    pdf_path = Path(doc["local_path"])
    final_path = parse_manager.output_dir / f"{pdf_path.stem}.md"
    partial_path = final_path.with_suffix(f"{final_path.suffix}.part")
    qa_path = parse_manager.qa_dir / f"{pdf_path.stem}.qa.json"
    backup_dir = parse_manager.output_dir / "_refresh_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    if final_path.exists():
        shutil.copy2(final_path, backup_dir / final_path.name)
    if qa_path.exists():
        shutil.copy2(qa_path, backup_dir / qa_path.name)
    if partial_path.exists():
        partial_path.unlink()

    parse_manager.db.reset_parse_progress(doc_id)
    parse_manager.db.touch_document_claim(doc_id, parse_manager.worker_id)

    worker_cmd = [
        sys.executable,
        "-m",
        "modules.parse_worker",
        "--doc-id",
        str(doc_id),
        "--db-path",
        str(parse_manager.db.db_path),
        "--config-path",
        parse_manager.config_path,
        "--worker-id",
        parse_manager.worker_id,
    ]

    try:
        result = subprocess.run(
            worker_cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=parse_manager.parse_worker_timeout_seconds,
            cwd=str(Path(__file__).resolve().parent),
        )
    except subprocess.TimeoutExpired:
        return False, None, None, "parse_timeout"

    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"parse_worker_exit_{result.returncode}"
        return False, None, None, detail

    if not final_path.exists() or final_path.stat().st_size < 50:
        return False, final_path, None, "missing_or_small_markdown"

    qa_report = parse_manager._write_parse_quality_report(final_path)
    return True, final_path, qa_report, None


def resolve_target_documents(db: DatabaseManager, doc_ids: list[int], pdfs: list[str]):
    targets = []
    seen = set()

    for doc_id in doc_ids:
        row = db.get_document(doc_id)
        if row and row["id"] not in seen:
            targets.append(row)
            seen.add(row["id"])

    for pdf_name in pdfs:
        row = db.get_document_by_local_path_suffix(pdf_name)
        if row and row["id"] not in seen:
            targets.append(row)
            seen.add(row["id"])

    return targets


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh the knowledge base for reparsed documents.")
    parser.add_argument("--doc-id", action="append", type=int, default=[], help="Document id to refresh.")
    parser.add_argument("--pdf", action="append", default=[], help="PDF filename suffix to refresh.")
    parser.add_argument("--reparse", action="store_true", help="Reparse the target PDFs with the current parser before extraction.")
    parser.add_argument("--rebuild-exports", action="store_true", help="Rebuild normalized and insight exports after refresh.")
    args = parser.parse_args()

    if not args.doc_id and not args.pdf:
        parser.error("Provide at least one --doc-id or --pdf target.")

    db = DatabaseManager()
    parse_manager = ParsingManager(db, worker_id="refresh-parse")
    extractor = ExtractionManager(db, worker_id="refresh-extract")
    verifier = VerificationManager(db, worker_id="refresh-verify")
    normalizer = NormalizationManager(db, folder_filters=["hiv_sti"])
    insights = InsightsManager()

    targets = resolve_target_documents(db, args.doc_id, args.pdf)
    if not targets:
        logger.error("No matching documents found for the requested refresh targets.")
        return 2

    run_id = db.start_run(model_used=f"{extractor.llm_model} [refresh]")
    refreshed = 0
    skipped = 0

    for doc in targets:
        doc_id = doc["id"]
        pdf_name = Path(doc["local_path"]).name
        markdown_path = extractor.markdown_dir / f"{Path(doc['local_path']).stem}.md"

        if args.reparse:
            logger.info("Reparsing %s with the current parser before refresh.", pdf_name)
            ok, reparsed_path, qa_report, error = reparse_document(parse_manager, doc)
            if not ok:
                logger.error("Skipping %s: reparse failed (%s)", pdf_name, error or "unknown parse error")
                skipped += 1
                continue
            markdown_path = reparsed_path or markdown_path
        else:
            if not markdown_path.exists():
                logger.error("Skipping %s: missing markdown %s", pdf_name, markdown_path)
                skipped += 1
                continue
            qa_report = parse_manager._load_parse_quality_report(markdown_path) or parse_manager._write_parse_quality_report(markdown_path)

        if qa_report.get("gate_status") == "fail":
            logger.error("Skipping %s: parse QA is %s (%s)", pdf_name, qa_report.get("gate_status"), "; ".join(qa_report.get("issues", [])))
            skipped += 1
            continue
        if qa_report.get("gate_status") == "warn":
            logger.warning("Proceeding with %s despite parse QA warning: %s", pdf_name, "; ".join(qa_report.get("issues", [])))

        prior_active_count = db.get_active_knowledge_point_count(doc_id)
        logger.info("Refreshing doc %s (%s). Existing active points: %s", doc_id, pdf_name, prior_active_count)

        extract_result = extractor.extract_document(
            doc,
            run_id=run_id,
            update_status_on_success=False,
            update_status_on_failure=False,
        )
        if not extract_result["success"]:
            db.retire_run_points(doc_id, run_id)
            db.resolve_quarantine_for_run(doc_id, run_id)
            logger.error("Refresh extraction failed for %s. Existing active points were kept.", pdf_name)
            skipped += 1
            continue

        db.retire_run_points(doc_id, run_id)

        verify_result = verifier.verify_document(
            doc,
            run_id=run_id,
            update_status_on_success=False,
            update_status_on_failure=False,
            allow_zero_points=(prior_active_count == 0),
        )
        if not verify_result["passed"]:
            db.retire_run_points(doc_id, run_id)
            db.resolve_quarantine_for_run(doc_id, run_id)
            logger.error("Refresh verification failed for %s. Existing active points were kept.", pdf_name)
            skipped += 1
            continue

        promoted_points = verify_result["point_count"]
        if promoted_points > 0:
            db.activate_run_points(doc_id, run_id)
            superseded = db.supersede_document_points(doc_id, superseding_run_id=run_id, keep_run_id=run_id)
            resolved_quarantine = db.resolve_inactive_quarantine(doc_id)
            logger.info(
                "Promoted refreshed run for %s. New active points: %s. Superseded old active points: %s. Resolved superseded quarantine rows: %s",
                pdf_name,
                promoted_points,
                superseded,
                resolved_quarantine,
            )
        else:
            logger.info("Refreshed %s with zero active points and no prior active points to replace.", pdf_name)

        db.update_document_status(doc_id, "verified")
        refreshed += 1

    db.finish_run(run_id, refreshed)

    if args.rebuild_exports:
        normalizer.build_exports()
        from modules.review_enricher import ReviewQueueEnricher
        ReviewQueueEnricher(use_zero_shot=False).build_exports()
        insights.build_exports()

    print(
        json.dumps(
            {
                "run_id": run_id,
                "refreshed": refreshed,
                "skipped": skipped,
                "targets": [Path(doc["local_path"]).name for doc in targets],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
