import logging
import sys
from modules.db import DatabaseManager
from modules.extract import ExtractionManager

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def test_single_extraction():
    db = DatabaseManager()
    extractor = ExtractionManager(db)
    
    # Force process doc 148 (HIV_STI_2014_March.pdf)
    # We'll fetch it from the DB to ensure data consistency
    conn = db.get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM documents WHERE id = 148")
    doc = dict(c.fetchone())
    
    print(f"Targeting Doc 148: {doc['local_path']}")
    
    # Temporarily reset status to 'parsed' if it was anything else (it should be 'parsed')
    db.update_document_status(148, "parsed")
    
    # Run extractor with limit=1 (it will pick up 148 because it's the only one we just matched)
    # Actually, process_pending_documents fetches all 'parsed' ones.
    # We can override it or just let it pick up 148 among others.
    # To be precise, let's modify the code or just use the _extract_chunk directly for testing.
    
    markdown_path = f"data/processed_md/HIV_STI_2014_March.md"
    with open(markdown_path, 'r', encoding='utf-8') as f:
        md_text = f.read()
        
    print(f"Extracted markdown length: {len(md_text)}")
    
    try:
        payload = extractor._extract_chunk(md_text, doc['url'])
        print(f"Extraction Successful! Found {len(payload.findings)} points.")
        for i, kp in enumerate(payload.findings):
            print(f"[{i+1}] {kp.claim}")
            print(
                "    Category: "
                f"{kp.metadata.get('category', 'General Discovery')} | "
                f"Value: {kp.metadata.get('value', kp.significance_score)}"
            )
    except Exception as e:
        print(f"Extraction Failed: {e}")

if __name__ == "__main__":
    test_single_extraction()
