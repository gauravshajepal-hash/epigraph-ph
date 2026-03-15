import sqlite3
import os

def detailed_check():
    conn = sqlite3.connect('data/epigraph.db')
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    print("--- Detailed Status Distribution ---")
    c.execute("SELECT status, COUNT(*) as count FROM documents GROUP BY status")
    stats = {row['status']: row['count'] for row in c.fetchall()}
    for status, count in stats.items():
        print(f"{status}: {count}")

    # Check if failed documents have markdown files
    md_dir = "data/processed_md"
    failed_with_md = 0
    failed_without_md = 0
    
    c.execute("SELECT id, local_path FROM documents WHERE status = 'failed'")
    failed_docs = c.fetchall()
    
    for doc in failed_docs:
        md_name = os.path.splitext(os.path.basename(doc['local_path']))[0] + ".md"
        md_path = os.path.join(md_dir, md_name)
        if os.path.exists(md_path):
            failed_with_md += 1
        else:
            failed_without_md += 1
            
    print(f"\n--- Failure Analysis ---")
    print(f"Failed during Parsing (No MD): {failed_without_md}")
    print(f"Failed during Extraction (Has MD): {failed_with_md}")
    
    conn.close()

if __name__ == "__main__":
    detailed_check()
