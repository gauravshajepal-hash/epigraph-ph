from pathlib import Path
from modules.db import DatabaseManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reset_junk")

def reset_junk_files():
    db = DatabaseManager()
    md_dir = Path("data/processed_md")
    
    count_deleted = 0
    count_reset = 0
    
    # Files to check
    files = list(md_dir.glob("*.md"))
    logger.info(f"Scanning {len(files)} markdown files...")
    
    for md_path in files:
        with open(md_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Criteria for junk:
        # 1. Contains "OCR Failed"
        # 2. Is extremely small (< 200 chars)
        is_junk = False
        if "OCR Failed" in content:
            is_junk = True
        elif len(content.strip()) < 200:
            is_junk = True
            
        if is_junk:
            # 1. Delete the file
            md_path.unlink()
            count_deleted += 1
            
            # 2. Reset the DB status
            # We need to find the document whose name matches the stem
            # Note: Stem might be slightly different if filenames were mangled
            # But usually it's stem == pdf stem
            stem = md_path.stem
            
            # We search for the doc in the DB
            with db.get_connection() as conn:
                cursor = conn.execute("SELECT id FROM documents WHERE local_path LIKE ?", (f"%{stem}.pdf",))
                row = cursor.fetchone()
                if row:
                    db.update_document_status(row['id'], "downloaded")
                    count_reset += 1
                
    logger.info(f"Cleaned up {count_deleted} junk files.")
    logger.info(f"Reset {count_reset} documents in DB for local parsing.")

if __name__ == "__main__":
    reset_junk_files()
