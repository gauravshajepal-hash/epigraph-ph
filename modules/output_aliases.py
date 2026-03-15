import hashlib
import json
import logging
import shutil
from pathlib import Path

import yaml

from .db import DatabaseManager

logger = logging.getLogger(__name__)


class OutputAliasManager:
    """Materializes markdown and QA aliases for duplicate raw PDFs."""

    def __init__(
        self,
        db: DatabaseManager,
        config_path: str = "config.yaml",
        folder_filters: list[str] | None = None,
    ):
        self.db = db
        self.folder_filters = folder_filters or []

        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
            paths_cfg = cfg.get("paths", {})

        self.pdf_dir = Path(paths_cfg.get("pdfs", "data/raw_pdfs"))
        self.markdown_dir = Path(paths_cfg.get("markdown", "data/processed_md"))
        self.qa_dir = self.markdown_dir / "_qa"
        self.manifest_path = self.qa_dir / "duplicate_output_aliases.json"

    def materialize(self) -> dict:
        canonical_by_hash = self._canonical_documents_by_hash()
        alias_records = []
        aliased_markdown = 0
        aliased_qa = 0

        for pdf_path in sorted(self.pdf_dir.glob("**/*.pdf")):
            if not self._matches_scope(pdf_path):
                continue

            file_hash = self._compute_hash(pdf_path)
            canonical_doc = canonical_by_hash.get(file_hash)
            if not canonical_doc:
                continue

            canonical_stem = Path(canonical_doc["local_path"]).stem
            alias_stem = pdf_path.stem
            if canonical_stem == alias_stem:
                continue

            source_md = self.markdown_dir / f"{canonical_stem}.md"
            alias_md = self.markdown_dir / f"{alias_stem}.md"
            source_qa = self.qa_dir / f"{canonical_stem}.qa.json"
            alias_qa = self.qa_dir / f"{alias_stem}.qa.json"

            record = {
                "duplicate_pdf": str(pdf_path),
                "canonical_doc_id": canonical_doc["id"],
                "canonical_pdf": canonical_doc["local_path"],
                "canonical_markdown": str(source_md),
                "alias_markdown": str(alias_md),
                "canonical_qa": str(source_qa),
                "alias_qa": str(alias_qa),
                "hash": file_hash,
                "markdown_materialized": False,
                "qa_materialized": False,
            }

            if source_md.exists():
                record["markdown_materialized"] = self._copy_if_needed(source_md, alias_md)
                if record["markdown_materialized"]:
                    aliased_markdown += 1

            if source_qa.exists():
                record["qa_materialized"] = self._copy_if_needed(source_qa, alias_qa)
                if record["qa_materialized"]:
                    aliased_qa += 1

            alias_records.append(record)

        summary = {
            "scope": self.folder_filters,
            "duplicate_aliases": len(alias_records),
            "markdown_materialized": aliased_markdown,
            "qa_materialized": aliased_qa,
            "records": alias_records,
        }
        self.qa_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(
            json.dumps(summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(
            "Materialized %s duplicate markdown aliases and %s QA aliases.",
            aliased_markdown,
            aliased_qa,
        )
        return summary

    def _canonical_documents_by_hash(self) -> dict[str, dict]:
        with self.db.get_connection() as conn:
            rows = conn.execute(
                "SELECT id, file_hash, local_path FROM documents ORDER BY id DESC"
            ).fetchall()
        canonical_by_hash: dict[str, dict] = {}
        for row in rows:
            canonical_by_hash.setdefault(row["file_hash"], dict(row))
        return canonical_by_hash

    def _matches_scope(self, pdf_path: Path) -> bool:
        if not self.folder_filters:
            return True

        normalized = str(pdf_path).replace("/", "\\").lower()
        return any(f"\\{str(folder).strip().replace('/', '\\').strip('\\').lower()}\\" in normalized for folder in self.folder_filters)

    def _compute_hash(self, file_path: Path) -> str:
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _copy_if_needed(self, source: Path, target: Path) -> bool:
        if target.exists():
            same_size = source.stat().st_size == target.stat().st_size
            target_newer = target.stat().st_mtime >= source.stat().st_mtime
            if same_size and target_newer:
                return False

        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        return True
