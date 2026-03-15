import json
import os
import re
from pathlib import Path

# This script will be fed into the browser subagent or run manually
# to extract Drive IDs from WordPress posts.

def extract_subpages():
    html_dir = Path("data/html_sources")
    subpages = set()
    
    # Pattern for FHSIS sub-pages
    pattern = re.compile(r'href="(https://doh.gov.ph/fhsis-[^"]+)"')
    
    for html_file in html_dir.glob("view-source_https___doh.gov.ph_health-statistics_fhsis-*.html"):
        with open(html_file, "r", encoding="utf-8") as f:
            content = f.read()
            matches = pattern.findall(content)
            for m in matches:
                subpages.add(m)
    
    return sorted(list(subpages))

if __name__ == "__main__":
    pages = extract_subpages()
    print(f"Found {len(pages)} sub-pages:")
    for p in pages:
        print(p)
    
    # Save for the next step
    with open("data/fhsis_subpages.json", "w") as f:
        json.dump(pages, f, indent=4)
    print("\nSaved to data/fhsis_subpages.json")
