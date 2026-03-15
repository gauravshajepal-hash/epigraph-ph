import argparse
import json

from modules.review_enricher import ReviewQueueEnricher


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich review_queue with template families.")
    parser.add_argument("--input", default="data/normalized/review_queue.csv")
    parser.add_argument("--output-dir", default="data/normalized")
    parser.add_argument("--model", default="MoritzLaurer/deberta-v3-base-zeroshot-v2.0")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--deterministic-only", action="store_true")
    args = parser.parse_args()

    enricher = ReviewQueueEnricher(
        input_path=args.input,
        output_dir=args.output_dir,
        model_name=args.model,
        batch_size=args.batch_size,
        use_zero_shot=not args.deterministic_only,
    )
    summary = enricher.build_exports()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
