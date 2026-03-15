from __future__ import annotations

import shutil
from pathlib import Path

from modules.publication_assets import PublicationAssetBuilder


NORMALIZED_EXPORT_FILES = (
    "dashboard_feed.json",
    "insights.json",
    "publication_assets.json",
    "summary.json",
    "claims.jsonl",
    "observations.jsonl",
    "review_queue.jsonl",
    "review_queue_enriched.jsonl",
    "review_queue_families.json",
)


def main():
    base_dir = Path(__file__).resolve().parent
    dist_dir = base_dir / "dist"
    normalized_dir = base_dir / "data" / "normalized"
    index_src = base_dir / "apps_script" / "Index.html"
    index_dst = dist_dir / "index.html"
    data_dst = dist_dir / "data" / "normalized"

    PublicationAssetBuilder(base_dir).build()

    if dist_dir.exists():
        shutil.rmtree(dist_dir)
    data_dst.mkdir(parents=True, exist_ok=True)

    shutil.copy2(index_src, index_dst)
    (dist_dir / ".nojekyll").write_text("", encoding="utf-8")

    for filename in NORMALIZED_EXPORT_FILES:
        source = normalized_dir / filename
        if not source.exists():
            raise FileNotFoundError(f"Missing normalized export required for bundle: {source}")
        shutil.copy2(source, data_dst / filename)

    figure_src = normalized_dir / "publication_figures"
    figure_dst = data_dst / "publication_figures"
    if figure_src.exists():
        shutil.copytree(figure_src, figure_dst, dirs_exist_ok=True)

    print(f"Built static web bundle at {dist_dir}")


if __name__ == "__main__":
    main()
