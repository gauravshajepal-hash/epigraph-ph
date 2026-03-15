import sqlite3

def reset_failed():
    conn = sqlite3.connect('data/epigraph.db')
    c = conn.cursor()
    
    # 1. Reset documents that failed parsing (no MD) back to 'downloaded'
    # Actually, it's safer to just reset ALL failed documents to 'downloaded' 
    # and let the pipeline logic decide if it needs to re-parse.
    # If the MD exists, the parser will skip it (if implemented correctly) 
    # or we can move them to 'parsed'.
    
    print("Resetting failed documents...")
    
    # Docs without MD -> downloaded
    # Docs with MD -> parsed
    import os
    md_dir = "data/processed_md"
    
    c.execute("SELECT id, local_path FROM documents WHERE status = 'failed'")
    failed_docs = c.fetchall()
    
    reset_to_downloaded = 0
    reset_to_parsed = 0
    
    for doc_id, local_path in failed_docs:
        md_name = os.path.splitext(os.path.basename(local_path))[0] + ".md"
        md_path = os.path.join(md_dir, md_name)
        if os.path.exists(md_path):
            c.execute("UPDATE documents SET status = 'parsed' WHERE id = ?", (doc_id,))
            reset_to_parsed += 1
        else:
            c.execute("UPDATE documents SET status = 'downloaded' WHERE id = ?", (doc_id,))
            reset_to_downloaded += 1
            
    conn.commit()
    print(f"Cleanup Complete:")
    print(f"Moved {reset_to_downloaded} docs to 'downloaded' (Parsing required)")
    print(f"Moved {reset_to_parsed} docs to 'parsed' (Extraction required)")
    conn.close()

if __name__ == "__main__":
    reset_failed()
