import logging
import random
import difflib
import yaml
from pathlib import Path
from .db import DatabaseManager

logger = logging.getLogger(__name__)

class VerificationManager:
    """Spot-checks extracted claims against local markdown caches to prevent hallucination."""
    
    def __init__(
        self,
        db: DatabaseManager,
        config_path: str = "config.yaml",
        worker_id: str = "verifier",
        folder_filters: list[str] | None = None,
    ):
        self.db = db
        self.worker_id = worker_id
        self.folder_filters = folder_filters or []
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
            pipeline_cfg = cfg.get('pipeline', {})
            paths_cfg = cfg.get('paths', {})
            self.sample_rate = pipeline_cfg.get('verification_sample_rate', 0.05)
            self.markdown_dir = Path(paths_cfg.get('markdown', 'data/processed_md'))
            
    def verify_pending_documents(self, limit=None):
        """Spot-checks claims from 'extracted' documents."""
        claim_limit = limit or 1
        docs = self.db.claim_documents(
            "extracted",
            "verifying",
            claim_limit,
            self.worker_id,
            folder_filters=self.folder_filters,
        )
        logger.info(f"Claimed {len(docs)} documents pending verification.")
        
        if not docs:
            return 0

        verified_count = 0
        for doc in docs:
            result = self.verify_document(
                doc,
                run_id=None,
                update_status_on_success=True,
                update_status_on_failure=True,
                allow_zero_points=True,
            )
            if result["passed"]:
                verified_count += 1
        return verified_count

    def verify_document(
        self,
        doc,
        run_id: int | None = None,
        update_status_on_success: bool = False,
        update_status_on_failure: bool = False,
        allow_zero_points: bool = True,
    ) -> dict:
        doc_id = doc["id"]
        md_path = self.markdown_dir / f"{Path(doc['local_path']).stem}.md"

        try:
            self.db.touch_document_claim(doc_id, self.worker_id)
            with open(md_path, 'r', encoding='utf-8') as f:
                source_text = f.read()

            points = self.db.get_document_points(
                doc_id,
                run_id=run_id,
                active_only=(run_id is None),
            )

            if not points:
                if allow_zero_points:
                    logger.warning(
                        "Doc %s has %s points for verification. Treating as verified.",
                        doc_id,
                        0,
                    )
                    if update_status_on_success:
                        self.db.update_document_status(doc_id, "verified")
                    return {"passed": True, "point_count": 0, "failed_point_ids": []}

                logger.error("Doc %s replacement run has 0 points; refusing to replace the existing knowledge base.", doc_id)
                if update_status_on_failure:
                    self.db.update_document_status(doc_id, "failed")
                return {"passed": False, "point_count": 0, "failed_point_ids": []}

            sample_size = max(1, int(len(points) * self.sample_rate))
            sample_points = random.sample(points, min(sample_size, len(points)))
            logger.debug(f"Spot-checking {len(sample_points)}/{len(points)} claims for doc {doc_id}...")

            failed_point_ids = []
            for pt in sample_points:
                kp_id, snippet = pt["id"], pt["snippet"]

                if not self._verify_snippet_exists(source_text, snippet):
                    logger.warning(f"HALLUCINATION DETECTED: Snippet not found natively in {md_path.name}.")
                    self.db.quarantine_point(kp_id, "Snippet string exact-match failure against canonical Markdown.")
                    failed_point_ids.append(kp_id)

            if failed_point_ids:
                logger.error(f"Doc {doc_id} failed verification! Flagging for manual review.")
                if update_status_on_failure:
                    self.db.update_document_status(doc_id, "failed")
                return {
                    "passed": False,
                    "point_count": len(points),
                    "failed_point_ids": failed_point_ids,
                }

            logger.info(f"Doc {doc_id} passed verification.")
            if update_status_on_success:
                self.db.update_document_status(doc_id, "verified")
            return {"passed": True, "point_count": len(points), "failed_point_ids": []}

        except Exception as e:
            logger.error(f"Verification pipeline crashed on doc {doc_id}: {e}")
            if update_status_on_failure:
                self.db.update_document_status(doc_id, "failed")
            return {"passed": False, "point_count": 0, "failed_point_ids": [], "error": str(e)}
                
    def _verify_snippet_exists(self, full_text: str, snippet: str) -> bool:
        """
        Check if the verbatim snippet exists in the original text.
        Tolerance explicitly allowed for whitespace/newline normalization, 
        but NOT word substitutions.
        """
        # Normalize simple spacing differences before checking
        norm_text = " ".join(full_text.split())
        norm_snippet = " ".join(snippet.split())
        
        if norm_snippet in norm_text:
            return True
            
        # Fallback to SequenceMatcher if it's very close (e.g. OCR artifacts)
        # 0.95 ratio is extremely strict - requires 95% char-for-char match
        # Useful for long 200 character snippets where 1 weird character could break `in`
        matcher = difflib.SequenceMatcher(None, norm_text, norm_snippet)
        match = matcher.find_longest_match(0, len(norm_text), 0, len(norm_snippet))
        
        if match.size / len(norm_snippet) > 0.95:
            return True
            
        return False
