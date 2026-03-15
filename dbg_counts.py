from modules.db import DatabaseManager
import os

def check():
    db = DatabaseManager()
    with db.get_connection() as conn:
        print("--- DB COUNTS ---")
        rows = conn.execute("SELECT status, COUNT(*) FROM documents GROUP BY status").fetchall()
        for r in rows:
            print(f"{r[0]}: {r[1]}")
        
        print("\n--- SAMPLE DOCS ---")
        rows = conn.execute("SELECT id, status, local_path FROM documents LIMIT 5").fetchall()
        for r in rows:
            print(f"ID={r[0]} | STATUS={r[1]} | PATH={r[2]}")

if __name__ == "__main__":
    check()
