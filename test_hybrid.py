import logging
import sys
from pathlib import Path

import fitz

from modules.db import DatabaseManager
from modules.parse import ParsingManager

# Setup basic logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("test_hybrid")

def test_single_doc():
    db = DatabaseManager()
    parser = ParsingManager(db)
    
    # Find a document that is in 'downloaded' state
    docs = db.get_pending_documents("downloaded")
    if not docs:
        # Check for 'failed' docs to retry
        docs = db.get_pending_documents("failed")
        
    if not docs:
        logger.error("No documents found for testing.")
        return
        
    test_doc = docs[0]
    pdf_path = Path(test_doc["local_path"])
    
    if not pdf_path.exists():
        logger.error(f"Test PDF not found at {pdf_path}")
        return
        
    logger.info(f"Testing hybrid parsing on: {pdf_path.name}")
    
    try:
        with fitz.open(str(pdf_path)) as pdf:
            md_content = parser._render_page_markdown(pdf.load_page(0), 0, len(pdf))
        
        logger.info("Parsing successful!")
        logger.info(f"First 500 characters of output:\n{md_content[:500]}...")
        
        if "AI Visual & Tabular Insights" in md_content:
            logger.info("SUCCESS: Vision fallback was triggered for the first page.")
        else:
            logger.info("NOTE: Native text was sufficient for the first page.")
            
    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)

if __name__ == "__main__":
    test_single_doc()
