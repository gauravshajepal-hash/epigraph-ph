import os
import sqlite3
import logging
from pathlib import Path
from modules.db import DatabaseManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("reset_purge")

def purge_and_reset():
    db = DatabaseManager()
    
    # 1. Purge Processed Markdown
    md_dir = Path("data/processed_md")
    if md_dir.exists():
        logger.info(f"Purging all files in {md_dir}...")
        for file in md_dir.glob("*"):
            if file.is_file():
                file.unlink()
        logger.info("Markdown purge complete.")

    # 2. Reset Database
    try:
        with db.get_connection() as conn:
            logger.info("Clearing knowledge_points and quarantine tables...")
            conn.execute("DELETE FROM quarantine")
            conn.execute("DELETE FROM knowledge_fts")
            conn.execute("DELETE FROM knowledge_points")
            
            logger.info("Resetting all documents to 'downloaded' status...")
            conn.execute("UPDATE documents SET status = 'downloaded'")
            
            # Optional: Clear runs history if desired
            # conn.execute("DELETE FROM runs")
            
            conn.commit()
        logger.info("Database reset complete.")
        
    except Exception as e:
        logger.error(f"Failed to reset database: {e}")

if __name__ == "__main__":
    purge_and_reset()
