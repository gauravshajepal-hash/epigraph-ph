import sqlite3

def check_doc(pattern):
    conn = sqlite3.connect('data/epigraph.db')
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    query = "SELECT id, local_path, status FROM documents WHERE local_path LIKE ?"
    c.execute(query, (f"%{pattern}%",))
    rows = c.fetchall()
    
    if not rows:
        print(f"No documents found matching: {pattern}")
    else:
        for row in rows:
            print(f"ID: {row['id']} | Status: {row['status']} | Path: {row['local_path']}")
    
    conn.close()

if __name__ == "__main__":
    check_doc("HIV_STI_2024_January_-_March.pdf")
    check_doc("HIV_STI_2014_March.pdf")
