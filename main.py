import json
import os
import sys
import logging
import time
from pathlib import Path
import requests
import yaml
from dotenv import load_dotenv

from modules.db import DatabaseManager
from modules.ingest import IngestionManager
from modules.parse import ParsingManager
from modules.extract import ExtractionManager
from modules.verify import VerificationManager
from modules.normalize import NormalizationManager
from modules.review_enricher import ReviewQueueEnricher
from modules.insights import InsightsManager
from modules.output_aliases import OutputAliasManager
from modules.publication_assets import PublicationAssetBuilder
from modules.sync_sheets import SyncManager
from modules.backup import BackupManager

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_pipeline_settings(config_path: str = "config.yaml") -> dict:
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"Could not load config at {config_path}: {e}")
        return {}


def acquire_pipeline_lock(stale_minutes: int) -> Path:
    lock_path = Path("logs/pipeline.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    stale_seconds = max(stale_minutes, 1) * 60

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
                json.dump({"pid": os.getpid(), "started_at": time.time()}, lock_file)
            return lock_path
        except FileExistsError:
            try:
                with open(lock_path, "r", encoding="utf-8") as lock_file:
                    payload = json.load(lock_file)
                started_at = float(payload.get("started_at", lock_path.stat().st_mtime))
            except Exception:
                started_at = lock_path.stat().st_mtime

            if time.time() - started_at > stale_seconds:
                logger.warning("Found stale pipeline lock. Reclaiming it for this run.")
                lock_path.unlink(missing_ok=True)
                continue

            raise RuntimeError(
                f"Another pipeline run appears active. Remove {lock_path} if it is stale."
            )


def release_pipeline_lock(lock_path: Path | None):
    if not lock_path:
        return
    try:
        lock_path.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Failed to release pipeline lock {lock_path}: {e}")

def send_discord_alert(success: bool, msg: str):
    webhook = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook: return
    
    icon = "✅" if success else "❌"
    payload = {"content": f"**EpiGraph PH Run {icon}**\n\n```\n{msg}\n```"}
    try:
        requests.post(webhook, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")

def main():
    logger.info("=" * 60)
    logger.info("EpiGraph PH Pipeline Booting...")
    
    # 1. Load Environment
    load_dotenv()
    cfg = load_pipeline_settings()
    pipeline_cfg = cfg.get("pipeline", {})
    processing_cfg = cfg.get("processing", {})
    lock_stale_minutes = int(pipeline_cfg.get("lock_stale_minutes", 720))
    resume_in_progress = bool(pipeline_cfg.get("resume_in_progress_on_start", True))
    run_ingestion_on_start = bool(pipeline_cfg.get("run_ingestion_on_start", False))
    focus_folders = [str(folder) for folder in processing_cfg.get("include_folders", []) if str(folder).strip()]
    worker_id = f"pipeline-{os.getpid()}"
    lock_path = None
    
    try:
        lock_path = acquire_pipeline_lock(lock_stale_minutes)

        # 2. Init DB
        db = DatabaseManager()
        if resume_in_progress:
            recovered_docs = db.requeue_in_progress_documents()
            if recovered_docs:
                logger.info(f"Recovered {recovered_docs} in-progress documents for resumption.")
        
        # 3. Ingestion (Wayback Discovery + Google Drive + Manual Drops)
        if run_ingestion_on_start:
            ingestor = IngestionManager(db)
            ingestor.run_full_ingestion()
        else:
            logger.info("Skipping ingestion on startup; processing existing local corpus only.")
        
        # 4. Incremental Pipeline Loop
        # We loop until no more documents are in 'downloaded' or 'parsed' or 'extracted' status
        if focus_folders:
            logger.info("Processing scope limited to folders: %s", ", ".join(focus_folders))

        parser = ParsingManager(db, worker_id=f"{worker_id}-parse", folder_filters=focus_folders)
        extractor = ExtractionManager(db, worker_id=f"{worker_id}-extract", folder_filters=focus_folders)
        verifier = VerificationManager(db, worker_id=f"{worker_id}-verify", folder_filters=focus_folders)
        normalizer = NormalizationManager(db, folder_filters=focus_folders)
        review_enricher = ReviewQueueEnricher(use_zero_shot=False)
        insights = InsightsManager()
        alias_manager = OutputAliasManager(db, folder_filters=focus_folders)
        publication_assets = PublicationAssetBuilder()
        syncer = SyncManager(db)
        backuper = BackupManager()
        
        iter_count = 0
        while True:
            iter_count += 1
            logger.info(f"--- Pipeline Iteration {iter_count} ---")
            
            # Check for work
            status_counts = db.get_status_counts(folder_filters=focus_folders or None)
            pending_parse = status_counts.get("downloaded", 0)
            pending_extract = status_counts.get("parsed", 0)
            pending_verify = status_counts.get("extracted", 0)
            in_progress = sum(
                status_counts.get(status, 0)
                for status in ("parsing", "extracting", "verifying")
            )
            logger.info(
                "Queue status: downloaded=%s parsed=%s extracted=%s in_progress=%s failed=%s",
                pending_parse,
                pending_extract,
                pending_verify,
                in_progress,
                status_counts.get("failed", 0),
            )
            
            if pending_parse == 0 and pending_extract == 0 and pending_verify == 0 and in_progress == 0:
                logger.info("No more pending documents to process.")
                break
                
            # PRIORITIZE EXTRACTION: Use a larger batch to clear backlog
            if pending_extract > 0:
                extractor.process_pending_documents(limit=5)
                 # Local: No cooling needed, but 1s for log readability
                time.sleep(1)
                
            # THEN Verification
            if pending_verify > 0:
                verifier.verify_pending_documents(limit=5)
                
            # THEN Parse (slow entry of new docs)
            if pending_parse > 0:
                parser.process_pending_documents(limit=1)
                 # Local: No cooling needed
                time.sleep(1)
                
            # Periodic Sync/Backup
            if iter_count % 5 == 0:
                syncer.sync_data()
                backuper.run_backup()
                
        # Final Sync/Backup
        parser.backfill_quality_reports()
        normalizer.build_exports()
        review_enricher.build_exports()
        insights.build_exports()
        publication_assets.build()
        alias_manager.materialize()
        syncer.sync_data()
        backuper.run_backup()
        
        logger.info("Pipeline completed successfully.")
        send_discord_alert(True, f"Pipeline finished cleanly. {iter_count} batches processed.")
        
    except Exception as e:
        logger.error(f"CRITICAL FIX CRASH: {e}", exc_info=True)
        send_discord_alert(False, f"Pipeline crashed ungracefully:\n{str(e)}")
        sys.exit(1)
    finally:
        release_pipeline_lock(lock_path)

if __name__ == "__main__":
    main()
