"""
Multi-Source HTML Report Downloader — v5
Parses multiple DOH HTML files provided by the user, categorizes them based on the filename,
extracts Google Drive links, and bulk-downloads them to their respective folders.
"""

import hashlib
import logging
import re
import requests
import sys
from pathlib import Path
from time import sleep

sys.path.insert(0, str(Path(__file__).parent))
from modules.db import DatabaseManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

HTML_DIR = Path("data/html_sources")
BASE_PDF_DIR = Path("data/raw_pdfs")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

GDRIVE_EXPORT = "https://drive.google.com/uc?export=download&id={}"
GDRIVE_CONFIRM = "https://drive.google.com/uc?export=download&confirm=t&id={}"


def compute_hash(fp: Path) -> str:
    h = hashlib.sha256()
    with open(fp, 'rb') as f:
        for c in iter(lambda: f.read(4096), b""):
            h.update(c)
    return h.hexdigest()


def get_category_from_filename(filename: str) -> str:
    """Determine the save directory based on the HTML filename."""
    name = filename.lower()
    if "fhsis-monthly" in name:
        return "fhsis_monthly"
    elif "fhsis-quarterly" in name:
        return "fhsis_quarterly"
    elif "eb-weekly-surveillance" in name:
        return "eb_weekly"
    elif "weekly-disease-surveillance" in name:
        return "pidsr"
    elif "hiv-sti" in name:
        return "hiv_sti"
    else:
        return "uncategorized"


def parse_html_for_drive_links(html_content: str, category: str) -> list[dict]:
    """Extract all Google Drive links and their labels from the HTML."""
    # Pattern designed to match the DOH accordion anchor tags
    pattern = r'<a\s+href="https://drive\.google\.com/file/d/([^"/]+)/view[^"]*"[^>]*>([^<]+)</a>'
    matches = re.findall(pattern, html_content)

    results = []
    for file_id, label in matches:
        label = label.strip().replace("&#8211;", "-").replace("&amp;", "&")
        # Clean the label into a filename
        safe_name = re.sub(r'[^\w\s-]', '', label).strip()
        safe_name = re.sub(r'[\s]+', '_', safe_name)
        
        prefix = category.upper().replace("_", "")
        
        results.append({
            "file_id": file_id,
            "label": label,
            "filename": f"{prefix}_{safe_name}",
        })
    return results


def download_from_drive(file_id: str, local_path: Path, logger) -> bool:
    """Download a file from Google Drive."""
    for url in [GDRIVE_EXPORT.format(file_id), GDRIVE_CONFIRM.format(file_id)]:
        try:
            resp = SESSION.get(url, timeout=30, allow_redirects=True)
            if resp.status_code != 200:
                continue

            content = resp.content
            if b'<!DOCTYPE html>' in content[:500] or b'<html' in content[:500]:
                if b'confirm=' in content or b'download anyway' in content.lower():
                    continue  # Needs confirm URL loop
                if len(content) < 5000:
                    continue  # Actual error page

            with open(local_path, 'wb') as f:
                f.write(content)

            size_kb = local_path.stat().st_size / 1024
            if size_kb < 3:
                local_path.unlink()
                continue

            return True
        except Exception as e:
            continue
    return False


def process_fhsis_deep_links(db, logger_manager):
    """Process links from fhsis_links.json if it exists."""
    links_file = Path("data/fhsis_links.json")
    if not links_file.exists():
        return 0, 0

    import json
    with open(links_file, "r") as f:
        data = json.load(f)

    downloaded = 0
    skipped = 0
    
    for page_url, drive_url in data.items():
        # Extact ID
        match = re.search(r'/d/([^/]+)', drive_url)
        if not match:
            # Check for folders or legacy links
            if "/folders/" in drive_url:
                logging.getLogger(__name__).warning(f"Skipping folder link (Discovery required): {drive_url}")
            continue
            
        file_id = match.group(1)
        
        # Categorize based on page URL
        category = "fhsis_monthly" if "monthly" in page_url or "fhsis-" in page_url else "fhsis_quarterly"
        if "quarter" in page_url:
            category = "fhsis_quarterly"
            
        target_dir = BASE_PDF_DIR / category
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a name from page URL
        slug = page_url.strip("/").split("/")[-1]
        filename = f"{category.upper().replace('_','')}_{slug.replace('-','_')}.pdf"
        local_path = target_dir / filename
        
        if local_path.exists():
            skipped += 1
            continue
            
        success = download_from_drive(file_id, local_path, logging.getLogger(__name__))
        if success:
            file_hash = compute_hash(local_path)
            db.insert_document(drive_url, file_hash, str(local_path))
            downloaded += 1
            logging.getLogger(__name__).info(f"Deep-Infill OK: {filename}")
        else:
            logging.getLogger(__name__).warning(f"Deep-Infill FAIL: {filename}")
            
        sleep(0.5)
        
    return downloaded, skipped


def main():
    db = DatabaseManager()

    # --- Phase 1: Deep-Infill from JSON ---
    print("--- Phase 1: Deep-Infilling FHSIS Discovery Links ---")
    d_infill, s_infill = process_fhsis_deep_links(db, None)

    # --- Phase 2: Standard HTML Parsing ---
    if not HTML_DIR.exists() or not any(HTML_DIR.iterdir()):
        print(f"Directory {HTML_DIR} is empty or missing.")
        return

    html_files = list(HTML_DIR.glob("*.html"))
    print(f"\n--- Phase 2: Parsing {len(html_files)} HTML source files ---\n")

    grand_total_downloaded = d_infill
    grand_total_skipped = s_infill

    for html_file in html_files:
        category = get_category_from_filename(html_file.name)
        log_adapter = logging.LoggerAdapter(logging.getLogger(__name__), {"category": category.upper()})
        
        target_dir = BASE_PDF_DIR / category
        target_dir.mkdir(parents=True, exist_ok=True)

        log_adapter.info(f"--- Parsing {html_file.name} ---")
        html_content = html_file.read_text(encoding="utf-8", errors="replace")
        
        drive_links = parse_html_for_drive_links(html_content, category)
        log_adapter.info(f"Extracted {len(drive_links)} unique download links.")

        if not drive_links:
            continue

        downloaded = 0
        skipped = 0
        failed = 0

        for dl in drive_links:
            local_path = target_dir / f"{dl['filename']}.pdf"

            if local_path.exists():
                skipped += 1
                continue

            success = download_from_drive(dl["file_id"], local_path, log_adapter)

            if success:
                size_kb = local_path.stat().st_size / 1024
                log_adapter.info(f"OK: {dl['filename']}.pdf ({size_kb:.0f} KB)")
                file_hash = compute_hash(local_path)
                source_url = f"https://drive.google.com/file/d/{dl['file_id']}/view"
                db.insert_document(source_url, file_hash, str(local_path))
                downloaded += 1
            else:
                log_adapter.warning(f"FAIL: {dl['label']} (ID: {dl['file_id']})")
                failed += 1

            sleep(0.5)

        log_adapter.info(f"Category {category} complete: {downloaded} downloaded, {skipped} skipped, {failed} failed.")
        grand_total_downloaded += downloaded
        grand_total_skipped += skipped

    print("\n" + "="*60)
    print(f"GRAND TOTAL: {grand_total_downloaded} files downloaded, {grand_total_skipped} skipped (already exist).")
    print("="*60)


if __name__ == "__main__":
    main()
