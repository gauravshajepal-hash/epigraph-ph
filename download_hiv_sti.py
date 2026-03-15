"""
HIV/STI Report Downloader — v4 (HTML Source Parse + Google Drive Direct Download)

Parses the DOH HIV/STI page HTML (provided by the user) to extract ALL Google Drive
file IDs, then downloads every report directly from Google Drive.
This bypasses Cloudflare entirely — the human provided the source, the machine does the rest.
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
logger = logging.getLogger(__name__)

PDF_DIR = Path("data/raw_pdfs/hiv_sti")
PDF_DIR.mkdir(parents=True, exist_ok=True)

HTML_FILE = Path("data/hiv_sti_page.html")

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


def parse_html_for_drive_links(html_content: str) -> list[dict]:
    """Extract all Google Drive links and their labels from the HTML."""
    pattern = r'<a\s+href="https://drive\.google\.com/file/d/([^"/]+)/view[^"]*"[^>]*>([^<]+)</a>'
    matches = re.findall(pattern, html_content)

    results = []
    for file_id, label in matches:
        label = label.strip().replace("&#8211;", "-").replace("&amp;", "&")
        # Clean the label into a filename
        safe_name = re.sub(r'[^\w\s-]', '', label).strip()
        safe_name = re.sub(r'[\s]+', '_', safe_name)
        results.append({
            "file_id": file_id,
            "label": label,
            "filename": f"HIV_STI_{safe_name}",
        })

    return results


def download_from_drive(file_id: str, local_path: Path) -> bool:
    """Download a file from Google Drive, handling the confirmation page."""
    for url in [GDRIVE_EXPORT.format(file_id), GDRIVE_CONFIRM.format(file_id)]:
        try:
            resp = SESSION.get(url, timeout=30, allow_redirects=True)
            if resp.status_code != 200:
                continue

            content = resp.content
            # Check if we got an HTML page instead of a file
            if b'<!DOCTYPE html>' in content[:500] or b'<html' in content[:500]:
                if b'confirm=' in content or b'download anyway' in content.lower():
                    continue  # Try the confirm URL
                if len(content) < 5000:
                    continue  # Error page

            with open(local_path, 'wb') as f:
                f.write(content)

            size_kb = local_path.stat().st_size / 1024
            if size_kb < 3:
                local_path.unlink()
                continue

            return True
        except Exception:
            continue

    return False


def main():
    db = DatabaseManager()

    # Read the saved HTML
    if not HTML_FILE.exists():
        logger.error(f"HTML file not found at {HTML_FILE}. Save the DOH page source there first.")
        return

    html_content = HTML_FILE.read_text(encoding="utf-8", errors="replace")

    # Parse out all Drive links
    drive_links = parse_html_for_drive_links(html_content)
    logger.info("=" * 60)
    logger.info(f"HIV/STI Downloader v4 - Parsed {len(drive_links)} Google Drive links from HTML")
    logger.info("=" * 60)

    for i, dl in enumerate(drive_links, 1):
        logger.info(f"  [{i:3d}] {dl['label']} (ID: {dl['file_id'][:12]}...)")

    # Download each file
    downloaded = 0
    skipped = 0
    failed = 0

    for dl in drive_links:
        local_path = PDF_DIR / f"{dl['filename']}.pdf"

        if local_path.exists():
            logger.debug(f"  SKIP: {dl['filename']} (exists)")
            skipped += 1
            continue

        success = download_from_drive(dl["file_id"], local_path)

        if success:
            size_kb = local_path.stat().st_size / 1024
            logger.info(f"  OK: {dl['filename']}.pdf ({size_kb:.0f} KB)")
            file_hash = compute_hash(local_path)
            source_url = f"https://drive.google.com/file/d/{dl['file_id']}/view"
            db.insert_document(source_url, file_hash, str(local_path))
            downloaded += 1
        else:
            logger.warning(f"  FAIL: {dl['label']} (ID: {dl['file_id']})")
            failed += 1

        sleep(0.5)  # Be polite to Google Drive

    logger.info("=" * 60)
    logger.info(f"RESULTS: {downloaded} downloaded, {skipped} skipped, {failed} failed")
    logger.info(f"Total files in hiv_sti folder: {len(list(PDF_DIR.glob('*')))}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
