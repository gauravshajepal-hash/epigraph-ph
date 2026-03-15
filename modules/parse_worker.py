import argparse
import json
import sys
from pathlib import Path

from .db import DatabaseManager
from .parse import ParsingManager


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one document parse in an isolated worker process.")
    parser.add_argument("--doc-id", type=int, required=True)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--config-path", default="config.yaml")
    parser.add_argument("--worker-id", default="parser-worker")
    args = parser.parse_args()

    db = DatabaseManager(db_path=args.db_path)
    doc = db.get_document(args.doc_id)
    if doc is None:
        print(json.dumps({"ok": False, "error": f"missing document {args.doc_id}"}))
        return 2

    pdf_path = Path(doc["local_path"])
    if not pdf_path.exists():
        print(json.dumps({"ok": False, "error": f"missing pdf {pdf_path}"}))
        return 2

    manager = ParsingManager(
        db,
        config_path=args.config_path,
        worker_id=args.worker_id,
        folder_filters=[],
    )
    out_path = manager._parse_document(doc, pdf_path)
    print(json.dumps({"ok": True, "out_path": str(out_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
