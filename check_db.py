import sqlite3
from pathlib import Path

def check_db():
    conn = sqlite3.connect('data/epigraph.db')
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    print("--- Document Status Counts ---")
    c.execute("SELECT status, COUNT(*) as count FROM documents GROUP BY status")
    for row in c.fetchall():
        print(f"{row['status']}: {row['count']}")
        
    print("\n--- Failed Documents (Latest 10) ---")
    c.execute("SELECT local_path, status FROM documents WHERE status = 'failed' LIMIT 10")
    for row in c.fetchall():
        print(f"{row['local_path']}")
        
    print("\n--- Parsed but not Extracted (Sample) ---")
    c.execute("SELECT local_path FROM documents WHERE status = 'parsed' LIMIT 5")
    for row in c.fetchall():
        print(f"{row['local_path']}")
        
    c.execute("SELECT COUNT(*) FROM knowledge_points")
    print(f"\nTotal Knowledge Points: {c.fetchone()[0]}")
    
    conn.close()

if __name__ == "__main__":
    check_db()
