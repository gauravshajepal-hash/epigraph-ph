import os
import shutil
import logging
import yaml
from pathlib import Path
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

class BackupManager:
    """Zips local SQLite database and pushes to a Google Drive backup folder."""
    
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    
    def __init__(self, config_path: str = "config.yaml"):
        # We reuse the same folder ID as the JSON exports, or could specify a separate one
        with open(config_path, 'r') as f:
            cfg = yaml.safe_load(f)
            self.folder_id = cfg['cloud']['drive_sync_folder_id']
            
        cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not cred_path or not Path(cred_path).exists():
            self.drive_service = None
        else:
            creds = Credentials.from_service_account_file(cred_path, scopes=self.SCOPES)
            self.drive_service = build('drive', 'v3', credentials=creds)

    def run_backup(self) -> bool:
        db_path = Path("data/epigraph.db")
        if not db_path.exists():
            logger.warning("No database found to backup.")
            return False
            
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        zip_name = f"epigraph_backup_{timestamp}"
        zip_path = Path("backups") / zip_name
        
        try:
            # Create local zip (shutil automatically appends .zip)
            shutil.make_archive(str(zip_path), 'zip', "data", "epigraph.db")
            final_zip_path = zip_path.with_suffix('.zip')
            logger.info(f"Created local backup: {final_zip_path.name}")
            
            # Upload to Drive
            if self.drive_service and self.folder_id:
                logger.debug(f"Uploading {final_zip_path.name} to Drive...")
                file_metadata = {
                    'name': final_zip_path.name, 
                    'parents': [self.folder_id],
                    'mimeType': 'application/zip'
                }
                media = MediaFileUpload(str(final_zip_path), mimetype='application/zip', resumable=True)
                
                self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                logger.info(f"Successfully uploaded backup to Drive.")
                
            # Cleanup old local backups (> 30 days) to save disk space
            self._cleanup_local_backups()
            return True
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False
            
    def _cleanup_local_backups(self):
        """Keeps local disk usage constrained to recent archives only."""
        backup_dir = Path("backups")
        if not backup_dir.exists(): return
        
        now = datetime.now().timestamp()
        retention_seconds = 30 * 24 * 60 * 60 # 30 days
        
        for z in backup_dir.glob("*.zip"):
            if now - z.stat().st_mtime > retention_seconds:
                logger.debug(f"Deleting old backup {z.name}")
                z.unlink()
