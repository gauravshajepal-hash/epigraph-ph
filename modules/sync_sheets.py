import os
import json
import logging
import yaml
from pathlib import Path
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials
from .db import DatabaseManager

logger = logging.getLogger(__name__)

class SyncManager:
    """Exports SQLite data to Google Drive as JSON, and pulses a Sheet for Heartbeat."""
    NORMALIZED_EXPORT_FILES = (
        "dashboard_feed.json",
        "insights.json",
        "summary.json",
        "claims.jsonl",
        "observations.jsonl",
        "review_queue.jsonl",
        "review_queue_enriched.jsonl",
        "review_queue_families.json",
        "publication_assets.json",
    )
    
    SCOPES = [
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    
    def __init__(self, db: DatabaseManager, config_path: str = "config.yaml"):
        self.db = db
        
        with open(config_path, 'r') as f:
            cfg = yaml.safe_load(f)
            self.folder_id = cfg['cloud']['drive_sync_folder_id']
            self.sheet_id = cfg['cloud']['dashboard_sheet_id']
            self.threshold = cfg['extraction']['confidence_threshold']
            
        cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not cred_path or not Path(cred_path).exists():
            logger.warning("GOOGLE_APPLICATION_CREDENTIALS not set or file missing. Sync will be skipped.")
            self.drive_service = None
            self.sheets_service = None
        else:
            creds = Credentials.from_service_account_file(cred_path, scopes=self.SCOPES)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
    def sync_data(self):
        """Main orchestration for the sync layer."""
        if not self.drive_service:
            logger.error("Cannot sync data without a valid Google Service Account.")
            return False
            
        docs = self.db.get_pending_documents("verified")
        if docs:
            logger.info(f"Syncing {len(docs)} newly verified documents to cloud...")
        else:
            logger.info("No newly verified documents pending. Refreshing cloud exports only.")
            
        try:
            # 1. Fetch data from local SQLite
            claims = self.db.get_synced_claims(confidence_threshold=self.threshold)
            stats = self.db.get_stats()
            metadata = {
                "last_run": datetime.utcnow().isoformat() + "Z",
                "total_claims_synced": len(claims),
                "quarantined_claims": stats["quarantined_count"]
            }
            
            # 2. Write locally first to /data/json_exports
            export_dir = Path("data/json_exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            self._write_json(export_dir / "knowledge_points.json", claims)
            self._write_json(export_dir / "stats.json", stats)
            self._write_json(export_dir / "metadata.json", metadata)
            exported_files = [
                ("knowledge_points.json", export_dir / "knowledge_points.json"),
                ("stats.json", export_dir / "stats.json"),
                ("metadata.json", export_dir / "metadata.json"),
            ]

            normalized_dir = Path("data/normalized")
            for filename in self.NORMALIZED_EXPORT_FILES:
                source_path = normalized_dir / filename
                if not source_path.exists():
                    logger.warning("Skipping missing normalized export: %s", source_path)
                    continue
                target_path = export_dir / filename
                self._copy_file(source_path, target_path)
                exported_files.append((filename, target_path))
            
            # 3. Upload/Overwrite into Google Drive
            if self.folder_id:
                for target_name, local_path in exported_files:
                    self._upload_to_drive(local_path, target_name)
                
            # 4. Pulse the monitoring Sheet
            if self.sheet_id:
                self._pulse_heartbeat()
                
            # 5. Mark docs as 'synced'
            for doc in docs:
                self.db.update_document_status(doc["id"], "synced")
                
            logger.info("Sync to cloud complete. Exported %s files.", len(exported_files))
            return True
            
        except Exception as e:
            logger.error(f"Sync process failed: {e}")
            if self.sheet_id:
                self._log_to_sheets(f"SYNC FAILURE: {e}")
            return False

    def _write_json(self, path: Path, data):
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)

    def _copy_file(self, source_path: Path, target_path: Path):
        target_path.write_bytes(source_path.read_bytes())
            
    def _upload_to_drive(self, local_file: Path, target_name: str):
        """Uploads a file to Drive, overwriting if a file with the same name exists in the folder."""
        # Find existing file to overwrite
        query = f"name='{target_name}' and '{self.folder_id}' in parents and trashed=false"
        results = self.drive_service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        
        media = MediaFileUpload(str(local_file), mimetype='application/json', resumable=True)
        
        if files:
            file_id = files[0]['id']
            logger.debug(f"Overwriting {target_name} on Drive ({file_id})")
            self.drive_service.files().update(fileId=file_id, media_body=media).execute()
        else:
            logger.debug(f"Creating new {target_name} on Drive")
            file_metadata = {'name': target_name, 'parents': [self.folder_id]}
            self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    def _pulse_heartbeat(self):
        """Writes ISO timestamp to Heartbeat!B1."""
        now_str = datetime.utcnow().isoformat() + "Z"
        body = {'values': [[now_str]]}
        try:
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id, range='Heartbeat!B1',
                valueInputOption='RAW', body=body
            ).execute()
        except Exception as e:
            logger.warning(f"Could not write Heartbeat to sheets: {e}")

    def _log_to_sheets(self, message: str):
        """Appends an error to the Logs tab."""
        if not self.sheets_service: return
        now_str = datetime.utcnow().isoformat() + "Z"
        body = {'values': [[now_str, "ERROR", message]]}
        try:
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=self.sheet_id, range='Logs!A:C',
                valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body
            ).execute()
        except Exception as e:
            logger.warning(f"Could not write to Logs sheet: {e}")
