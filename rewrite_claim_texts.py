import argparse
import json
import logging
import sys

from modules.db import DatabaseManager
from modules.extract import ExtractionManager
from modules.insights import InsightsManager
from modules.normalize import NormalizationManager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize active claim_text values in place.")
    parser.add_argument("--rebuild-exports", action="store_true", help="Rebuild normalized and insight exports after rewriting claim text.")
    args = parser.parse_args()

    db = DatabaseManager()
    extractor = ExtractionManager(db)
    normalizer = NormalizationManager(db, folder_filters=["hiv_sti"])
    insights = InsightsManager()

    updated = 0
    examined = 0
    samples = []

    with db.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, claim_text, category, snippet
            FROM knowledge_points
            WHERE superseded_by_run_id IS NULL
            ORDER BY id
            """
        ).fetchall()

        for row in rows:
            examined += 1
            original = row["claim_text"]
            normalized = extractor._normalize_claim_text(original)
            if normalized == original:
                continue

            conn.execute(
                "UPDATE knowledge_points SET claim_text = ? WHERE id = ?",
                (normalized, row["id"]),
            )
            updated += 1
            if len(samples) < 20:
                samples.append({"id": row["id"], "before": original, "after": normalized})

        if updated:
            conn.execute("INSERT INTO knowledge_fts(knowledge_fts) VALUES('rebuild')")

    if args.rebuild_exports:
        normalizer.build_exports()
        insights.build_exports()

    print(
        json.dumps(
            {
                "examined": examined,
                "updated": updated,
                "samples": samples,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
