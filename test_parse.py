"""Quick HTML save helper - extracts drive links count from saved HTML."""
import re
from pathlib import Path

html_file = Path("data/hiv_sti_page.html")
if not html_file.exists():
    print("ERROR: data/hiv_sti_page.html not found!")
    exit(1)

html = html_file.read_text(encoding="utf-8", errors="replace")
pattern = r'href="https://drive\.google\.com/file/d/([^"/]+)/view[^"]*"[^>]*>([^<]+)</a>'
matches = re.findall(pattern, html)
print(f"Found {len(matches)} Google Drive links:")
for i, (fid, label) in enumerate(matches, 1):
    label = label.strip().replace("&#8211;", "-")
    print(f"  [{i:3d}] {label} -> {fid[:16]}...")
