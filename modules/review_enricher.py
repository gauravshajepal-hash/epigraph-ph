import csv
import json
import logging
import re
from collections import Counter, defaultdict
from pathlib import Path

logger = logging.getLogger(__name__)


class ReviewQueueEnricher:
    """Adds deterministic and model-assisted family labels to the review queue."""

    ZERO_SHOT_LABELS = {
        "surveillance_narrative_needs_structuring": [
            "historical_trend",
            "age_distribution",
            "ofw_profile",
            "regional_distribution",
            "transmission_breakdown",
            "treatment_program",
            "testing_summary",
            "non_chart_context",
        ],
        "structured_table_mapping_needed": [
            "quick_facts_table",
            "regional_distribution_table",
            "mode_of_transmission_table",
            "age_distribution_table",
            "ocr_scrambled_table",
            "non_chart_table",
        ],
        "policy_claim_needs_tagging": [
            "policy_rule",
            "workplace_policy",
            "policy_reference",
            "background_policy_context",
        ],
    }

    def __init__(
        self,
        input_path: str = "data/normalized/review_queue.csv",
        output_dir: str = "data/normalized",
        model_name: str = "MoritzLaurer/deberta-v3-base-zeroshot-v2.0",
        batch_size: int = 8,
        use_zero_shot: bool = True,
    ):
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model_name = model_name
        self.batch_size = batch_size
        self.use_zero_shot = use_zero_shot
        self.classifier = None
        self.classifier_name = "deterministic"

        if use_zero_shot:
            self._load_classifier()

    def build_exports(self) -> dict:
        rows = self._read_csv(self.input_path)
        enriched: list[dict] = []
        pending_by_reason: defaultdict[str, list[tuple[int, dict]]] = defaultdict(list)

        for index, row in enumerate(rows):
            deterministic = self._deterministic_family(row)
            if deterministic:
                enriched_row = dict(row)
                enriched_row.update(deterministic)
                enriched.append(enriched_row)
            else:
                pending_by_reason[row.get("review_reason") or ""].append((index, row))
                enriched.append({})

        for reason, items in pending_by_reason.items():
            classified_rows = self._classify_reason_batch(reason, items)
            for index, enriched_row in classified_rows:
                enriched[index] = enriched_row

        family_counts = Counter(row.get("template_family") or "unclassified" for row in enriched)
        source_counts = Counter(row.get("family_source") or "unknown" for row in enriched)

        self._write_csv(self.output_dir / "review_queue_enriched.csv", enriched)
        self._write_jsonl(self.output_dir / "review_queue_enriched.jsonl", enriched)

        summary = {
            "total_rows": len(enriched),
            "classifier": self.classifier_name,
            "family_counts": dict(family_counts),
            "family_source_counts": dict(source_counts),
        }
        (self.output_dir / "review_queue_families.json").write_text(
            json.dumps(summary, indent=2),
            encoding="utf-8",
        )
        return summary

    def _load_classifier(self) -> None:
        try:
            from transformers import pipeline

            self.classifier = pipeline(
                "zero-shot-classification",
                model=self.model_name,
                device=-1,
            )
            self.classifier_name = self.model_name
        except Exception as exc:
            logger.warning("Zero-shot classifier unavailable, falling back to deterministic only: %s", exc)
            self.classifier = None
            self.classifier_name = "deterministic"

    def _classify_reason_batch(self, reason: str, items: list[tuple[int, dict]]) -> list[tuple[int, dict]]:
        if not items:
            return []

        if not self.classifier:
            return [
                (
                    index,
                    self._fallback_row(row, reason),
                )
                for index, row in items
            ]

        labels = self.ZERO_SHOT_LABELS.get(reason) or ["non_chart_context"]
        texts = [self._review_text(row)[:1200] for _, row in items]
        try:
            results = self.classifier(
                texts,
                labels,
                hypothesis_template="This review item is about {}.",
                multi_label=False,
                batch_size=self.batch_size,
            )
        except Exception as exc:
            logger.warning("Zero-shot classification failed for %s, using fallback: %s", reason, exc)
            return [
                (
                    index,
                    self._fallback_row(row, reason),
                )
                for index, row in items
            ]

        if isinstance(results, dict):
            results = [results]

        enriched_rows: list[tuple[int, dict]] = []
        for (index, row), result in zip(items, results):
            labels_out = result.get("labels") or []
            scores_out = result.get("scores") or []
            top_label = labels_out[0] if labels_out else "unclassified"
            top_score = float(scores_out[0]) if scores_out else 0.0
            second_label = labels_out[1] if len(labels_out) > 1 else ""
            second_score = float(scores_out[1]) if len(scores_out) > 1 else 0.0

            enriched_row = dict(row)
            enriched_row.update(
                {
                    "template_family": top_label,
                    "template_confidence": round(top_score, 4),
                    "second_family": second_label,
                    "second_confidence": round(second_score, 4),
                    "family_source": "zero_shot",
                    "recommended_strategy": self._recommend_strategy(top_label, reason),
                }
            )
            enriched_rows.append((index, enriched_row))
        return enriched_rows

    def _deterministic_family(self, row: dict) -> dict | None:
        reason = row.get("review_reason") or ""
        text = self._review_text(row).lower()

        family = ""
        if reason == "surveillance_narrative_needs_structuring":
            if any(marker in text for marker in (" ofw", "ofws", "overseas filipino", "worked overseas")):
                family = "ofw_profile"
            elif any(
                marker in text
                for marker in (
                    "15-24 years old",
                    "25-34 year age group",
                    "35-49 year age group",
                    "50 years old and above",
                    "children less than 10",
                    "adolescents aged 10-19",
                    "age group",
                )
            ):
                family = "age_distribution"
            elif any(
                marker in text
                for marker in (
                    "top five regions",
                    "most number of reported cases",
                    "most number of newly diagnosed cases",
                    "most number of newly reported cases",
                )
            ):
                family = "regional_distribution"
            elif any(
                marker in text
                for marker in (
                    "predominant mode of transmission",
                    "male to male sex",
                    "male-male sex",
                    "sexual contact remained",
                    "mother-to-child transmission",
                    "sharing of infected needles",
                )
            ):
                family = "transmission_breakdown"
            elif any(
                marker in text
                for marker in (
                    "first case of hiv infection",
                    "since then, there have been",
                    "annual number of deaths",
                    "from 1984 to 2006",
                )
            ):
                family = "historical_trend"
            elif any(marker in text for marker in ("viral load", "plhiv on art", "treatment coverage", "life-saving anti-retroviral therapy")):
                family = "treatment_program"
            elif any(marker in text for marker in ("testing services", "screening", "hiv testing", "reactive result")):
                family = "testing_summary"
        elif reason == "structured_table_mapping_needed":
            if "quick facts" in text:
                family = "quick_facts_table"
            elif any(
                marker in text
                for marker in (
                    "mode of hiv transmission",
                    "types of sexual transmission",
                    "mode of transmission",
                    "male-male",
                    "male-female",
                    "mother-to-child",
                    "sharing of infected needles",
                )
            ) and "has values" in text:
                family = "mode_of_transmission_table"
            elif any(marker in text for marker in ("region of residence", "region of:", "care cascade region")):
                family = "regional_distribution_table"
            elif "region number of %" in text:
                family = "regional_distribution_table"
            elif any(marker in text for marker in ("age", "y/o", "years old", "less than 15", "15-24", "25-34", "35-49", "50 years")):
                family = "age_distribution_table"
            elif self._looks_ocr_scrambled(text):
                family = "ocr_scrambled_table"
        elif reason == "policy_claim_needs_tagging":
            if any(marker in text for marker in ("shall", "must", "only be released", "should undergo", "repeat hiv screening")):
                family = "policy_rule"
            elif "workplace policy" in text or "ilo code of practice" in text or "compliance rates" in text:
                family = "workplace_policy"
            elif any(marker in text for marker in ("ao ", "dc ", "dm ", "ra ", "ordinance", "mc 2013-29", "policy instrument")):
                family = "policy_reference"

        if not family:
            return None

        return {
            **row,
            "template_family": family,
            "template_confidence": 1.0,
            "second_family": "",
            "second_confidence": 0.0,
            "family_source": "deterministic",
            "recommended_strategy": self._recommend_strategy(family, reason),
        }

    def _fallback_row(self, row: dict, reason: str) -> dict:
        family = {
            "surveillance_narrative_needs_structuring": "non_chart_context",
            "structured_table_mapping_needed": "ocr_scrambled_table",
            "policy_claim_needs_tagging": "background_policy_context",
        }.get(reason, "unclassified")
        return {
            **row,
            "template_family": family,
            "template_confidence": 0.0,
            "second_family": "",
            "second_confidence": 0.0,
            "family_source": "fallback",
            "recommended_strategy": self._recommend_strategy(family, reason),
        }

    def _recommend_strategy(self, family: str, reason: str) -> str:
        if family in {
            "ofw_profile",
            "age_distribution",
            "regional_distribution",
            "transmission_breakdown",
            "treatment_program",
            "testing_summary",
            "quick_facts_table",
            "regional_distribution_table",
            "mode_of_transmission_table",
            "age_distribution_table",
        }:
            return "deterministic_mapper"
        if family in {"historical_trend", "workplace_policy", "policy_rule", "policy_reference", "background_policy_context"}:
            return "timeline_and_policy_tagging"
        if family in {"ocr_scrambled_table", "non_chart_table"}:
            return "upstream_table_reparse"
        if reason == "surveillance_narrative_needs_structuring":
            return "model_then_manual_review"
        return "manual_review"

    def _looks_ocr_scrambled(self, text: str) -> bool:
        alpha_tokens = re.findall(r"\b[A-Za-z]{3,}\b", text)
        numeric_tokens = re.findall(r"\b\d[\d,]*\b", text)
        if text.count("|") >= 6 and len(alpha_tokens) <= 4 and len(numeric_tokens) >= 10:
            return True
        if len(re.findall(r"\b\d{4}\b", text)) >= 4 and len(numeric_tokens) >= 8 and len(alpha_tokens) <= 6:
            return True
        if re.search(r"\b\d+\s+\d+\s+\d+\s+\d+\b", text) and len(alpha_tokens) <= 5:
            return True
        return False

    def _review_text(self, row: dict) -> str:
        parts = [
            row.get("filename") or "",
            row.get("document_type") or "",
            row.get("metric_type") or "",
            row.get("claim_text") or "",
            row.get("snippet") or "",
        ]
        return re.sub(r"\s+", " ", " ".join(part.strip() for part in parts if part and part.strip())).strip()

    def _read_csv(self, path: Path) -> list[dict]:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def _write_csv(self, path: Path, rows: list[dict]) -> None:
        fieldnames: list[str] = []
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _write_jsonl(self, path: Path, rows: list[dict]) -> None:
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=True))
                handle.write("\n")
