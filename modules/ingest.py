"""
EpiGraph PH Ingestion Module — Revised Architecture

Strategy:
  1. Crawl Wayback Machine snapshots of DOH sub-pages to discover Google Drive file IDs.
  2. Download PDFs directly from Google Drive using the /export?format=pdf endpoint.
  3. Fall back to manual drops in data/raw_pdfs/.

This completely bypasses the Cloudflare 403 wall on the live DOH site.
"""

import re
import hashlib
import logging
import requests
from pathlib import Path
from datetime import datetime
from .db import DatabaseManager

logger = logging.getLogger(__name__)

# Known DOH sub-pages (archived via Wayback Machine)
WAYBACK_BASE = "https://web.archive.org/web/2024"
DOH_SUB_PAGES = [
    f"{WAYBACK_BASE}/https://doh.gov.ph/health-statistics/epidemic-prone-disease-case-surveillance-annual-report/",
    f"{WAYBACK_BASE}/https://doh.gov.ph/health-statistics/fhsis-report/",
    f"{WAYBACK_BASE}/https://doh.gov.ph/health-statistics/hiv-sti/",
    f"{WAYBACK_BASE}/https://doh.gov.ph/health-statistics/event-based-surveillance-and-response/",
    f"{WAYBACK_BASE}/https://doh.gov.ph/health-statistics/weekly-disease-surveillance-report/",
]

# Known Google Drive file IDs from the DOH page (seed list from Wayback crawl)
KNOWN_DRIVE_FILES = {
    "2019_PIDSR_Annual_Report": "1aSoDdDUrw0vwRCZI4WFZEWrAlcKnX8uU",
    "2020_PIDSR_Annual_Report": "1AU3ucBTWDr7JOzi-KDyupJ4pqG8-8juq",
    "2021_PIDSR_Annual_Report": "1dCTtleEqwq8Mc-rYtZOWChT08pCL-OCA",
}

# Regex to extract Google Drive file IDs from any URL pattern
GDRIVE_ID_PATTERN = re.compile(
    r'(?:drive\.google\.com/file/d/|drive\.google\.com/open\?id=|docs\.google\.com/\w+/d/)'
    r'([a-zA-Z0-9_-]{20,})'
)


class IngestionManager:
    """Downloads DOH epidemiological PDFs via Wayback Machine discovery + Google Drive export."""

    def __init__(self, db: DatabaseManager, pdf_dir: str = "data/raw_pdfs"):
        self.db = db
        self.pdf_dir = Path(pdf_dir)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def _compute_hash(self, file_path: Path) -> str:
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    # ── Tier 3: Manual Drop ──────────────────────────────────────────

    def process_manual_drops(self):
        """Scans data/raw_pdfs/ for locally placed PDFs and registers them."""
        logger.info("Checking for manual PDF drops (Tier 3)...")
        new_files = 0
        for pdf_path in self.pdf_dir.glob("**/*.pdf"):
            file_hash = self._compute_hash(pdf_path)
            _, created = self.db.register_document(
                url=f"manual://{pdf_path.name}",
                file_hash=file_hash,
                local_path=str(pdf_path)
            )
            if created:
                new_files += 1
        if new_files > 0:
            logger.info(f"Registered {new_files} manual dropped PDFs.")
        else:
            logger.info("No new manual drops found.")

    # ── Tier 1: Wayback Machine Discovery ────────────────────────────

    def discover_drive_links_from_wayback(self) -> dict[str, str]:
        """Crawls Wayback Machine snapshots of DOH sub-pages to find Google Drive file IDs."""
        discovered = dict(KNOWN_DRIVE_FILES)  # Start with the seed list
        
        for url in DOH_SUB_PAGES:
            logger.info(f"Scanning Wayback snapshot: {url}")
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.warning(f"Wayback returned {resp.status_code} for {url}")
                    continue

                # Extract all Google Drive file IDs from the page HTML
                matches = GDRIVE_ID_PATTERN.findall(resp.text)
                for file_id in matches:
                    if file_id not in discovered.values():
                        # Generate a name from context or use the ID
                        name = f"doh_report_{file_id[:8]}"
                        discovered[name] = file_id
                        logger.info(f"Discovered new Drive file: {file_id}")

            except Exception as e:
                logger.warning(f"Failed to scan {url}: {e}")

        total_new = len(discovered) - len(KNOWN_DRIVE_FILES)
        logger.info(f"Wayback discovery complete. Known: {len(KNOWN_DRIVE_FILES)}, Newly found: {total_new}")
        return discovered

    # ── Tier 2: Google Drive Direct Download ─────────────────────────

    def download_from_google_drive(self, file_id: str, filename: str) -> Path | None:
        """Downloads a PDF from Google Drive using the export endpoint."""
        # For public Drive files, this direct download URL works without auth
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

        target_dir = self.pdf_dir / "drive_downloads"
        target_dir.mkdir(parents=True, exist_ok=True)
        local_path = target_dir / f"{filename}.pdf"

        if local_path.exists():
            logger.debug(f"{filename}.pdf already exists, skipping download.")
            return local_path

        logger.info(f"Downloading {filename} from Google Drive (ID: {file_id})...")

        try:
            resp = self.session.get(download_url, stream=True, timeout=60)

            # Google Drive sometimes shows a "virus scan" confirmation page for large files
            if "confirm=" not in resp.url and b"Google Drive - Virus scan warning" in resp.content[:2000]:
                confirm_token = self._extract_confirm_token(resp)
                if confirm_token:
                    download_url = f"{download_url}&confirm={confirm_token}"
                    resp = self.session.get(download_url, stream=True, timeout=60)

            # Validate we got a PDF (not an HTML error page)
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type and resp.status_code == 200:
                # Likely a sharing restriction or confirmation page
                logger.warning(f"Drive returned HTML instead of PDF for {file_id}. File may require auth.")
                return None

            resp.raise_for_status()

            with open(local_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Verify file is actually a PDF
            with open(local_path, 'rb') as f:
                header = f.read(5)
                if header != b'%PDF-':
                    logger.error(f"{filename} is not a valid PDF (header: {header}). Removing.")
                    local_path.unlink()
                    return None

            logger.info(f"Successfully downloaded {filename}.pdf ({local_path.stat().st_size / 1024:.1f} KB)")
            return local_path

        except Exception as e:
            logger.error(f"Failed to download {file_id}: {e}")
            if local_path.exists():
                local_path.unlink()
            return None

    def _extract_confirm_token(self, response) -> str | None:
        """Extracts the confirmation token from the Google Drive virus scan page."""
        for key, value in response.cookies.items():
            if key.startswith("download_warning"):
                return value
        # Try to find it in the HTML
        match = re.search(r'confirm=([0-9A-Za-z_-]+)', response.text)
        return match.group(1) if match else None

    # ── Main Orchestration ───────────────────────────────────────────

    def run_full_ingestion(self) -> bool:
        """
        Full ingestion pipeline:
        1. Check manual drops
        2. Discover Drive links from Wayback Machine
        3. Download all discovered PDFs from Google Drive
        4. Register everything in the database
        """
        # Step 1: Manual drops
        self.process_manual_drops()

        # Step 2: Wayback discovery
        logger.info("Starting Wayback Machine link discovery...")
        drive_files = self.discover_drive_links_from_wayback()

        if not drive_files:
            logger.warning("No Google Drive files discovered. Relying on manual drops only.")
            return True

        # Step 3: Download from Google Drive
        logger.info(f"Attempting to download {len(drive_files)} files from Google Drive...")
        downloaded_count = 0

        for name, file_id in drive_files.items():
            local_path = self.download_from_google_drive(file_id, name)
            if local_path and local_path.exists():
                file_hash = self._compute_hash(local_path)
                source_url = f"https://drive.google.com/file/d/{file_id}/view"
                _, created = self.db.register_document(source_url, file_hash, str(local_path))
                if created:
                    downloaded_count += 1

        logger.info(f"Ingestion complete. Downloaded and registered {downloaded_count} new PDFs.")
        return downloaded_count > 0 or len(list(self.pdf_dir.glob("**/*.pdf"))) > 0
