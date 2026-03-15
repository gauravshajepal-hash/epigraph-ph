import csv
import json
import logging
import re
from collections import OrderedDict
from pathlib import Path

import yaml

from .db import DatabaseManager

logger = logging.getLogger(__name__)


class NormalizationManager:
    """Builds normalized claim and observation exports from extracted knowledge points."""

    SCALAR_UNSAFE_CATEGORIES = {
        "Quantitative Narrative",
        "Narrative Statistic",
        "Structured Table Row",
        "Care Cascade Age Group",
        "Care Cascade Key Population",
        "Care Cascade Region",
        "Treatment Outcome",
    }
    SURVEILLANCE_FILENAME_HINTS = (
        "hiv_sti_",
        "surveillance",
        "aidsreg",
        "registry",
        "annual_report",
        "hiv_aids_surveillance",
    )
    POLICY_FILENAME_HINTS = (
        "guideline",
        "guidelines",
        "cpg",
        "administrative_order",
        "ao_",
        "dc_",
        "dm_",
        "memo",
        "pocket_card",
        "rhivda",
    )
    SITUATIONAL_FILENAME_HINTS = (
        "situational_report",
        "workplace_response",
    )
    ARTIFACT_PATTERNS = (
        re.compile(r"\b(?:hospital|street|avenue|building|road|rd\.|drive|room)\b", re.IGNORECASE),
        re.compile(r"\b(?:telephone|mobile|fax|email|website|facebook)\b", re.IGNORECASE),
        re.compile(r"\b(?:notre dame|san lazaro|department of health, bldg)\b", re.IGNORECASE),
        re.compile(r"\b\d{7,}\b"),
        re.compile(r"\b\d{3}\s*/\s*\d{8,}\b"),
    )
    SMALL_NUMBER_WORDS = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }
    MONTH_NAME_TO_NUM = OrderedDict(
        [
            ("january", 1),
            ("february", 2),
            ("march", 3),
            ("april", 4),
            ("may", 5),
            ("june", 6),
            ("july", 7),
            ("august", 8),
            ("september", 9),
            ("october", 10),
            ("november", 11),
            ("december", 12),
            ("jan", 1),
            ("feb", 2),
            ("mar", 3),
            ("apr", 4),
            ("jun", 6),
            ("jul", 7),
            ("aug", 8),
            ("sep", 9),
            ("oct", 10),
            ("nov", 11),
            ("dec", 12),
        ]
    )
    QUICK_FACTS_ROW_MAP = {
        "total reported cases": ("reported_cases", "total"),
        "asymptomatic cases": ("asymptomatic_cases", "total"),
        "aids cases": ("aids_cases", "total"),
        "male": ("reported_cases", "male"),
        "female": ("reported_cases", "female"),
        "less than 15 y/o": ("reported_cases", "age_lt_15"),
        "15-24 y/o": ("reported_cases", "age_15_24"),
        "25-34 y/o": ("reported_cases", "age_25_34"),
        "35-49 y/o": ("reported_cases", "age_35_49"),
        "50 y/o & above": ("reported_cases", "age_50_plus"),
        "reported deaths": ("reported_deaths", "total"),
        "newly started on art": ("new_art_starts", "total"),
        "total plhiv on art": ("plhiv_on_art", "total"),
    }

    DISEASE_PATTERNS = OrderedDict(
        [
            ("HIV", re.compile(r"\bhiv\b", re.IGNORECASE)),
            ("AIDS", re.compile(r"\baids?\b", re.IGNORECASE)),
            ("STI", re.compile(r"\bstis?\b|\bsexually transmitted infections?\b", re.IGNORECASE)),
            ("Syphilis", re.compile(r"\bsyphilis\b", re.IGNORECASE)),
            ("Gonorrhea", re.compile(r"\bgonorrh(?:ea|oea)\b", re.IGNORECASE)),
            ("Hepatitis C", re.compile(r"\bhepatitis\s*c\b|\bhep\s*c\b", re.IGNORECASE)),
        ]
    )
    METRIC_PATTERNS = OrderedDict(
        [
            ("new_cases", re.compile(r"\bnew cases?\b|\bnewly diagnosed\b", re.IGNORECASE)),
            ("cases", re.compile(r"\bcases?\b", re.IGNORECASE)),
            ("deaths", re.compile(r"\bdeaths?\b|\bmortality\b", re.IGNORECASE)),
            ("prevalence", re.compile(r"\bprevalence\b", re.IGNORECASE)),
            ("incidence", re.compile(r"\bincidence\b", re.IGNORECASE)),
            ("testing", re.compile(r"\btests?\b|\btesting\b|\bscreening\b", re.IGNORECASE)),
            ("treatment", re.compile(r"\bart\b|\bantiretroviral\b|\btreatment\b|\btherapy\b", re.IGNORECASE)),
            ("policy", re.compile(r"\bguideline\b|\badministrative order\b|\bmemo\b|\bpolicy\b", re.IGNORECASE)),
        ]
    )
    LOCATION_PATTERNS = OrderedDict(
        [
            ("Philippines", re.compile(r"\bphilippines\b|\bnational\b", re.IGNORECASE)),
            ("NCR", re.compile(r"\bncr\b|\bnational capital region\b", re.IGNORECASE)),
            ("CALABARZON", re.compile(r"\bcalabarzon\b|\bregion iv-a\b", re.IGNORECASE)),
            ("Central Luzon", re.compile(r"\bcentral luzon\b|\bregion iii\b", re.IGNORECASE)),
            ("Davao Region", re.compile(r"\bdavao\b|\bregion xi\b", re.IGNORECASE)),
            ("Central Visayas", re.compile(r"\bcentral visayas\b|\bregion vii\b", re.IGNORECASE)),
        ]
    )
    NUMBER_PATTERN = re.compile(
        r"(?P<value>\d[\d,]*(?:\.\d+)?)\s*(?P<unit>%|percent|cases?|deaths?|tests?)?",
        re.IGNORECASE,
    )
    YEAR_PATTERN = re.compile(r"(?<!\d)(20\d{2}|19\d{2})(?!\d)")
    QUARTER_PATTERN = re.compile(r"(?<![A-Z0-9])Q([1-4])(?!\d)", re.IGNORECASE)
    REGION_LABEL_PATTERN = re.compile(r"(?:[0-9]{1,2}[A-Z]?|NCR|NIR|CAR|CARAGA|BARMM)")
    ROMAN_REGION_MAP = {
        "i": "1",
        "ii": "2",
        "iii": "3",
        "iv": "4",
        "v": "5",
        "vi": "6",
        "vii": "7",
        "viii": "8",
        "ix": "9",
        "x": "10",
        "xi": "11",
        "xii": "12",
        "xiii": "13",
    }

    CARE_CASCADE_METRICS = [
        ("estimated_plhiv", "count"),
        ("diagnosed_plhiv", "count"),
        ("diagnosis_coverage", "percent"),
        ("on_art", "count"),
        ("art_coverage", "percent"),
        ("vl_tested", "count"),
        ("vl_testing_coverage", "percent"),
        ("vl_suppressed", "count"),
        ("vl_suppression_among_tested", "percent"),
        ("suppression_among_on_art", "percent"),
    ]
    CARE_CASCADE_METRICS_COMPACT = [
        ("estimated_plhiv", "count"),
        ("diagnosed_plhiv", "count"),
        ("diagnosis_coverage", "percent"),
        ("on_art", "count"),
        ("art_coverage", "percent"),
        ("vl_tested", "count"),
        ("vl_suppressed", "count"),
        ("vl_suppression_among_tested", "percent"),
        ("suppression_among_on_art", "percent"),
    ]

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
        extraction_cfg = cfg.get("extraction", {})
        self.output_dir = Path(paths_cfg.get("normalized", "data/normalized"))
        self.confidence_threshold = float(extraction_cfg.get("confidence_threshold", 0.8))
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def build_exports(self) -> dict:
        claims = self.db.get_active_knowledge_points(
            confidence_threshold=self.confidence_threshold,
            folder_filters=self.folder_filters or None,
        )
        normalized_claims: list[dict] = []
        observations: list[dict] = []
        review_queue: list[dict] = []
        for row in claims:
            claim = self._normalize_claim(row)
            claim_observations = self._claim_to_observations(claim)
            review_item = self._claim_to_review_item(claim, claim_observations)

            claim["observation_count"] = len(claim_observations)
            claim["chart_ready"] = bool(claim_observations)
            claim["review_needed"] = bool(review_item)
            claim["review_reason"] = review_item["review_reason"] if review_item else ""
            claim["review_priority"] = review_item["priority"] if review_item else ""

            normalized_claims.append(claim)
            observations.extend(claim_observations)
            if review_item:
                review_queue.append(review_item)

        observations = self._dedupe_observations(observations)
        entities, relations = self._build_graph_rows(normalized_claims)

        self._write_csv(self.output_dir / "claims.csv", normalized_claims)
        self._write_jsonl(self.output_dir / "claims.jsonl", normalized_claims)
        self._write_csv(self.output_dir / "observations.csv", observations)
        self._write_jsonl(self.output_dir / "observations.jsonl", observations)
        self._write_csv(self.output_dir / "review_queue.csv", review_queue)
        self._write_jsonl(self.output_dir / "review_queue.jsonl", review_queue)
        self._write_csv(self.output_dir / "entities.csv", entities)
        self._write_csv(self.output_dir / "relations.csv", relations)

        summary = {
            "claims_count": len(normalized_claims),
            "observations_count": len(observations),
            "review_queue_count": len(review_queue),
            "entities_count": len(entities),
            "relations_count": len(relations),
            "scope_folders": self.folder_filters,
        }
        self._write_json(self.output_dir / "summary.json", summary)
        logger.info(
            "Normalized exports written: %s claims, %s observations.",
            len(normalized_claims),
            len(observations),
        )
        return summary

    def _normalize_claim(self, row: dict) -> dict:
        local_path = Path(row["local_path"])
        claim_text = row["claim_text"] or ""
        snippet = row["snippet"] or ""
        category = row.get("category") or ""
        combined_text = f"{local_path.name}\n{claim_text}\n{snippet}"
        row_label = self._extract_row_label(claim_text)

        disease_tags = [
            disease
            for disease, pattern in self.DISEASE_PATTERNS.items()
            if pattern.search(combined_text)
        ]
        if disease_tags:
            primary_disease = disease_tags[0]
        elif local_path.parent.name == "hiv_sti" or "hiv" in local_path.name.lower():
            primary_disease = "HIV"
        else:
            primary_disease = "Unclassified"

        metric_type = self._infer_metric_type(category, combined_text)
        region = self._infer_region(row_label, combined_text)
        year = self._extract_year(local_path.name) or self._extract_year(combined_text)
        month = self._extract_month(local_path.name)
        quarter = self._extract_quarter(local_path.name) or self._extract_quarter(combined_text)
        period_granularity = self._infer_period_granularity(local_path.name, year, quarter, month)
        numeric_value, unit = self._extract_numeric_value(row.get("value"), claim_text, category)
        document_type = self._classify_document_type(local_path, category, claim_text)
        policy_tags = self._extract_policy_tags(claim_text, snippet)
        policy_tags = self._augment_policy_tags_from_filename(local_path.name, document_type, policy_tags)

        return {
            "claim_id": row["knowledge_point_id"],
            "document_id": row["document_id"],
            "folder": local_path.parent.name,
            "filename": local_path.name,
            "document_type": document_type,
            "source_url": row.get("source_url") or row.get("document_url") or "",
            "page_index": row["page_index"],
            "category": category,
            "metric_type": metric_type,
            "primary_disease": primary_disease,
            "disease_tags": "|".join(disease_tags),
            "region": region,
            "row_label": row_label,
            "year": year or "",
            "quarter": quarter or "",
            "month": month or "",
            "period_granularity": period_granularity,
            "period_scope": "snapshot",
            "period_label": self._build_period_label(year, quarter, month, period_granularity),
            "numeric_value": numeric_value if numeric_value is not None else "",
            "unit": unit,
            "raw_value": row.get("value") or "",
            "confidence": row["confidence"],
            "claim_text": claim_text,
            "snippet": snippet,
            "created_at": row["created_at"],
            "policy_type": policy_tags["policy_type"],
            "policy_reference": policy_tags["policy_reference"],
            "policy_issuer": policy_tags["policy_issuer"],
            "policy_title": policy_tags["policy_title"],
        }

    def _claim_to_observations(self, claim: dict) -> list[dict]:
        category = claim.get("category") or ""
        row_label = claim.get("row_label") or ""
        values = self._parse_value_cells(claim.get("raw_value") or "")

        if category == "Structured Table Row":
            targeted_table_rows = self._targeted_structured_table_observations(claim)
            if targeted_table_rows:
                return targeted_table_rows
            quick_fact_rows = self._quick_facts_observations(claim)
            if quick_fact_rows:
                return quick_fact_rows
            legacy_table_rows = self._legacy_transmission_table_observations(claim)
            if legacy_table_rows:
                return legacy_table_rows

        if category == "Care Cascade Age Group":
            return self._series_observations(
                claim,
                self._care_cascade_metric_schema(values),
                values,
                subgroup=row_label,
            )

        if category == "Care Cascade Key Population":
            return self._series_observations(
                claim,
                self._care_cascade_metric_schema(values),
                values,
                subgroup=row_label,
            )

        if category == "Care Cascade Region":
            return self._series_observations(
                claim,
                self._care_cascade_metric_schema(values),
                values,
                region=row_label,
            )

        if category == "Treatment Outcome":
            return self._series_observations(
                claim,
                [
                    ("alive_on_art", "count"),
                    ("dead", "count"),
                    ("lost_to_follow_up", "count"),
                    ("transferred_overseas", "count"),
                    ("stopped_treatment", "count"),
                    ("ever_enrolled_total", "count"),
                    ("lost_to_follow_up_rate", "percent"),
                ],
                values,
                region=row_label,
            )

        if category == "Structured Table Row" and "Percent increase between December 2020" in claim.get("claim_text", ""):
            return self._series_observations(
                claim,
                [
                    ("cases_2020", "count"),
                    ("cases_2025", "count"),
                    ("percent_increase", "percent"),
                ],
                values,
                subgroup=row_label,
            )

        if category in {"Quantitative Narrative", "Narrative Statistic", "general"}:
            narrative_rows = self._narrative_surveillance_observations(claim)
            if narrative_rows:
                return narrative_rows

        return []

    def _targeted_structured_table_observations(self, claim: dict) -> list[dict]:
        for mapper in (
            self._regional_vl_status_table_observations,
            self._regional_treatment_outcome_table_observations,
            self._regional_treatment_outcome_partial_observations,
            self._april_june_region_residence_table_observations,
        ):
            observations = mapper(claim)
            if observations:
                return observations
        return []

    def _quick_facts_observations(self, claim: dict) -> list[dict]:
        if claim.get("document_type") != "surveillance_report":
            return []
        if claim.get("period_granularity") != "month":
            return []

        row_key = (claim.get("row_label") or "").strip().lower()
        metric_mapping = self.QUICK_FACTS_ROW_MAP.get(row_key)
        if not metric_mapping:
            return []

        values = self._parse_clean_value_tokens(claim.get("raw_value") or "")
        if len(values) < 2:
            return []

        year = int(claim["year"]) if claim.get("year") else None
        month = int(claim["month"]) if claim.get("month") else None
        if not year or not month:
            return []

        if len(values) >= 4:
            scopes = [
                ("month", self._format_year_month(year, month)),
                ("year_to_date", f"{year} YTD"),
                ("recent_window", f"2010-{year}-{month:02d}"),
                ("cumulative", f"1984-{year}-{month:02d}"),
            ]
        elif len(values) == 3:
            scopes = [
                ("month", self._format_year_month(year, month)),
                ("recent_window", f"2011-{year}-{month:02d}"),
                ("cumulative", f"1984-{year}-{month:02d}"),
            ]
        else:
            scopes = [
                ("month", self._format_year_month(year, month)),
                ("cumulative", f"1984-{year}-{month:02d}"),
            ]

        metric_type, subgroup = metric_mapping
        observations = []
        for index, (scope, label) in enumerate(scopes):
            if index >= len(values):
                break
            observations.append(
                self._observation_row(
                    claim,
                    observation_id=f"{claim['claim_id']}:{metric_type}:{scope}",
                    metric_type=metric_type,
                    value=values[index],
                    unit="count",
                    subgroup=subgroup,
                    period_scope=scope,
                    period_label=label,
                    period_granularity="month" if scope == "month" else scope,
                    month=month if scope == "month" else "",
                )
            )
        return observations

    def _regional_vl_status_table_observations(self, claim: dict) -> list[dict]:
        region = self._structured_row_region(claim)
        if not region:
            return []

        raw_tokens = self._split_raw_value_tokens(claim.get("raw_value") or "")
        if len(raw_tokens) != 5:
            return []
        if sum("%" in token for token in raw_tokens) != 2:
            return []
        if claim.get("period_granularity") != "quarter":
            return []

        values = self._parse_clean_value_tokens(claim.get("raw_value") or "")
        if len(values) != 5:
            return []

        alive_on_art, vl_tested, vl_tested_pct, vl_suppressed, vl_suppressed_pct = values
        observations = [
            self._scoped_metric_observation(
                claim,
                "alive_on_art_count",
                alive_on_art,
                "count",
                "snapshot",
                claim.get("period_label") or "",
                region=region,
            ),
            self._scoped_metric_observation(
                claim,
                "viral_load_tested_count",
                vl_tested,
                "count",
                "snapshot",
                claim.get("period_label") or "",
                region=region,
            ),
            self._scoped_metric_observation(
                claim,
                "viral_load_tested_pct",
                vl_tested_pct,
                "percent",
                "snapshot",
                claim.get("period_label") or "",
                region=region,
            ),
            self._scoped_metric_observation(
                claim,
                "viral_load_suppressed_count",
                vl_suppressed,
                "count",
                "snapshot",
                claim.get("period_label") or "",
                region=region,
            ),
            self._scoped_metric_observation(
                claim,
                "viral_load_suppressed_pct",
                vl_suppressed_pct,
                "percent",
                "snapshot",
                claim.get("period_label") or "",
                region=region,
            ),
        ]
        return [row for row in observations if row]

    def _regional_treatment_outcome_table_observations(self, claim: dict) -> list[dict]:
        region = self._structured_row_region(claim)
        if not region:
            return []

        claim_text = claim.get("claim_text") or ""
        if not claim_text.startswith("Treatment Outcome:"):
            return []

        raw_tokens = self._split_raw_value_tokens(claim.get("raw_value") or "")
        if len(raw_tokens) != 7:
            return []
        if sum("%" in token for token in raw_tokens) != 1:
            return []

        values = self._parse_clean_value_tokens(claim.get("raw_value") or "")
        if len(values) != 7:
            return []

        filename = (claim.get("filename") or "").lower()
        if filename == "hiv_sti_2025_q4.pdf":
            schema = (
                ("alive_on_art_count", "count"),
                ("dead_count", "count"),
                ("lost_to_follow_up_count", "count"),
                ("migrated_overseas_count", "count"),
                ("refused_art_count", "count"),
                ("ever_enrolled_art_count", "count"),
                ("lost_to_follow_up_pct", "percent"),
            )
        else:
            schema = (
                ("alive_on_art_count", "count"),
                ("lost_to_follow_up_count", "count"),
                ("dead_count", "count"),
                ("migrated_overseas_count", "count"),
                ("refused_art_count", "count"),
                ("ever_enrolled_art_count", "count"),
                ("lost_to_follow_up_pct", "percent"),
            )

        observations = []
        for (metric_type, unit), value in zip(schema, values):
            observations.append(
                self._scoped_metric_observation(
                    claim,
                    metric_type,
                    value,
                    unit,
                    "snapshot",
                    claim.get("period_label") or "",
                    region=region,
                )
            )

        if len(values) >= 6:
            not_on_treatment = max(0.0, values[5] - values[0])
            observations.append(
                self._scoped_metric_observation(
                    claim,
                    "not_on_treatment_count",
                    not_on_treatment,
                    "count",
                    "snapshot",
                    claim.get("period_label") or "",
                    region=region,
                )
            )

        return [row for row in observations if row]

    def _regional_treatment_outcome_partial_observations(self, claim: dict) -> list[dict]:
        filename = (claim.get("filename") or "").lower()
        if filename != "hiv_sti_2024_april_june.pdf":
            return []
        if int(claim.get("page_index") or -1) != 3:
            return []

        region = self._structured_row_region(claim)
        row_label_region = self._coerce_region_label(claim.get("row_label") or "")
        raw_tokens = self._split_raw_value_tokens(claim.get("raw_value") or "")
        if raw_tokens:
            token_region = self._coerce_region_label(raw_tokens[0])
            if token_region and (not region or row_label_region != token_region):
                region = token_region
                raw_tokens = raw_tokens[1:]
        if not region and raw_tokens:
            snippet_region = self._region_before_value_sequence(
                claim.get("snippet") or "",
                raw_tokens[0],
            )
            if snippet_region:
                region = snippet_region
        if not region:
            return []
        if not 2 <= len(raw_tokens) <= 4:
            return []
        if any("%" in token for token in raw_tokens):
            return []

        values = self._parse_clean_value_tokens(claim.get("raw_value") or "")
        if len(values) != len(self._split_raw_value_tokens(claim.get("raw_value") or "")):
            return []
        if len(values) > len(raw_tokens):
            values = values[-len(raw_tokens):]
        if len(values) != len(raw_tokens):
            return []

        schema = (
            ("alive_on_art_count", "count"),
            ("lost_to_follow_up_count", "count"),
            ("migrated_overseas_count", "count"),
            ("refused_art_count", "count"),
        )
        observations = []
        for (metric_type, unit), value in zip(schema, values):
            observations.append(
                self._scoped_metric_observation(
                    claim,
                    metric_type,
                    value,
                    unit,
                    "snapshot",
                    claim.get("period_label") or "",
                    region=region,
                )
            )
        return [row for row in observations if row]

    def _april_june_region_residence_table_observations(self, claim: dict) -> list[dict]:
        filename = (claim.get("filename") or "").lower()
        if filename != "hiv_sti_2024_april_june.pdf":
            return []
        if int(claim.get("page_index") or -1) != 1:
            return []
        if not (claim.get("claim_text") or "").startswith("Region Number of %:"):
            return []

        primary_region, embedded_count = self._structured_region_label_with_count(
            claim.get("row_label") or ""
        )
        if not primary_region:
            return []

        tokens = self._structured_row_tokens(claim.get("raw_value") or "")
        if embedded_count is not None:
            tokens = [str(embedded_count), *tokens]
        if not tokens:
            return []

        observations: list[dict] = []
        if len(tokens) == 2 and self._is_count_token(tokens[0]) and self._is_percent_token(tokens[1]):
            observations.extend(
                self._scoped_count_pct_observations(
                    claim,
                    "new_cases",
                    self._parse_int(tokens[0]),
                    self._parse_float(tokens[1]),
                    "snapshot",
                    claim.get("period_label") or "",
                    region=primary_region,
                )
            )
            return [row for row in observations if row]

        table_region = ""
        table_pairs: list[str] = []

        if len(tokens) == 6 and self._looks_like_count_pct_pairs(tokens):
            table_region = primary_region
            table_pairs = tokens
        elif len(tokens) >= 9 and self._is_count_token(tokens[0]) and self._is_percent_token(tokens[1]):
            secondary_region = self._coerce_region_label(tokens[2])
            if secondary_region:
                observations.extend(
                    self._scoped_count_pct_observations(
                        claim,
                        "new_cases",
                        self._parse_int(tokens[0]),
                        self._parse_float(tokens[1]),
                        "snapshot",
                        claim.get("period_label") or "",
                        region=primary_region,
                    )
                )
                table_region = secondary_region
                table_pairs = tokens[3:9]
        elif len(tokens) >= 8 and self._is_count_token(tokens[0]):
            secondary_region = self._coerce_region_label(tokens[1])
            if secondary_region:
                table_region = secondary_region
                table_pairs = tokens[2:8]

        if table_region and self._looks_like_count_pct_pairs(table_pairs):
            scopes = (
                ("year_to_date", "2024 H1"),
                ("recent_window", "2019-2024 H1"),
                ("cumulative", "1984-2024 H1"),
            )
            for index, (scope, label) in enumerate(scopes):
                count_token = table_pairs[index * 2]
                pct_token = table_pairs[index * 2 + 1]
                observations.extend(
                    self._scoped_count_pct_observations(
                        claim,
                        "reported_cases",
                        self._parse_int(count_token),
                        self._parse_float(pct_token),
                        scope,
                        label,
                        region=table_region,
                    )
                )

        return [row for row in observations if row]

    def _legacy_transmission_table_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "mode of hiv transmission" not in lowered and "types of sexual transmission" not in lowered:
            return []

        label = (claim.get("row_label") or "").strip().lower()
        metric_base = {
            "mother-to-child": "mother_to_child_transmission",
            "needle prick injury": "needle_prick_injury_transmission",
            "injecting drug use": "needle_transmission",
            "blood/blood products": "blood_transmission",
            "sexual contact": "sexual_contact_cases",
            "homosexual contact": "male_male_sex_cases",
            "bisexual contact": "bisexual_contact_cases",
            "heterosexual contact": "male_female_sex_cases",
            "heterosexual contact - female": "male_female_sex_cases",
        }.get(label)
        if not metric_base:
            return []

        value_text = text.split("has values", 1)[1] if "has values" in text else text
        tokens = re.findall(r"(?:<)?\d[\d,]*(?:\.\d+)?%?", value_text)
        if not tokens:
            return []

        observations: list[dict] = []
        month_label = claim.get("period_label") or ""
        cumulative_label = self._cumulative_period_label(claim)

        count_pct_pairs = re.findall(r"(\d[\d,]*)\s*\((\d+)%\)", value_text)
        if len(count_pct_pairs) >= 2:
            month_count, month_pct = count_pct_pairs[0]
            cumulative_count, cumulative_pct = count_pct_pairs[1]
            observations.extend(
                self._scoped_count_pct_observations(
                    claim,
                    metric_base,
                    self._parse_int(month_count),
                    self._parse_float(month_pct),
                    "month",
                    month_label,
                )
            )
            observations.extend(
                self._scoped_count_pct_observations(
                    claim,
                    metric_base,
                    self._parse_int(cumulative_count),
                    self._parse_float(cumulative_pct),
                    "cumulative",
                    cumulative_label,
                )
            )
            return [row for row in observations if row]

        if len(tokens) >= 4 and tokens[1].endswith("%") and tokens[3].endswith("%"):
            observations.extend(
                self._scoped_count_pct_observations(
                    claim,
                    metric_base,
                    self._parse_int(tokens[0]),
                    self._parse_float(tokens[1]),
                    "month",
                    month_label,
                )
            )
            observations.extend(
                self._scoped_count_pct_observations(
                    claim,
                    metric_base,
                    self._parse_int(tokens[2]),
                    self._parse_float(tokens[3]),
                    "cumulative",
                    cumulative_label,
                )
            )
            return [row for row in observations if row]

        if len(tokens) >= 3 and not tokens[1].endswith("%") and tokens[2].endswith("%"):
            observations.append(
                self._scoped_metric_observation(
                    claim,
                    f"{metric_base}_count",
                    self._parse_int(tokens[0]),
                    "count",
                    "month",
                    month_label,
                )
            )
            observations.extend(
                self._scoped_count_pct_observations(
                    claim,
                    metric_base,
                    self._parse_int(tokens[1]),
                    self._parse_float(tokens[2]),
                    "cumulative",
                    cumulative_label,
                )
            )
            return [row for row in observations if row]

        if len(tokens) >= 2 and not any(token.endswith("%") for token in tokens[:2]):
            observations.append(
                self._scoped_metric_observation(
                    claim,
                    f"{metric_base}_count",
                    self._parse_int(tokens[0]),
                    "count",
                    "month",
                    month_label,
                )
            )
            observations.append(
                self._scoped_metric_observation(
                    claim,
                    f"{metric_base}_count",
                    self._parse_int(tokens[1]),
                    "count",
                    "cumulative",
                    cumulative_label,
                )
            )

        return [row for row in observations if row]

    def _narrative_surveillance_observations(self, claim: dict) -> list[dict]:
        if claim.get("document_type") not in {"surveillance_report", "situational_report"}:
            return []

        rows: list[dict] = []
        rows.extend(self._cohort_cascade_observations(claim))
        rows.extend(self._historical_art_snapshot_observations(claim))
        rows.extend(self._national_diagnosed_observations(claim))
        rows.extend(self._national_art_vl_observations(claim))
        rows.extend(self._standalone_vl_summary_observations(claim))
        rows.extend(self._program_vl_observations(claim))
        rows.extend(self._compact_quarterly_surveillance_observations(claim))
        rows.extend(self._transactional_sex_observations(claim))
        rows.extend(self._art_regimen_observations(claim))
        rows.extend(self._pregnant_reported_observations(claim))
        rows.extend(self._tgw_transmission_observations(claim))
        rows.extend(self._national_transmission_observations(claim))
        rows.extend(self._art_ltfu_breakdown_observations(claim))
        rows.extend(self._art_current_status_observations(claim))
        rows.extend(self._ahd_summary_observations(claim))
        rows.extend(self._historical_epidemic_summary_observations(claim))
        rows.extend(self._quarter_new_cases_mot_observations(claim))
        rows.extend(self._narrative_transmission_breakdown_observations(claim))
        rows.extend(self._regional_case_summary_observations(claim))
        rows.extend(self._age_group_distribution_observations(claim))
        rows.extend(self._ofw_observations(claim))
        rows.extend(self._legacy_monthly_new_case_observations(claim))
        rows.extend(self._legacy_monthly_death_observations(claim))
        rows.extend(self._legacy_cumulative_death_observations(claim))
        rows.extend(self._legacy_transmission_summary_observations(claim))

        deduped: OrderedDict[str, dict] = OrderedDict()
        for row in rows:
            deduped.setdefault(row["observation_id"], row)
        return list(deduped.values())

    def _cohort_cascade_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        subgroup = self._detect_cohort_subgroup(lowered)
        if not subgroup:
            return []

        observations: list[dict] = []
        total_match = re.search(r"\(n=\s*(\d[\d,]*)\)", text, re.IGNORECASE)
        all_alive_match = re.search(
            r"all [^.]{0,120}?\(n=\s*(\d[\d,]*)\)[^.]{0,120}? were alive",
            text,
            re.IGNORECASE,
        )
        alive_match = re.search(
            r"(\d[\d,]*)\s*\((\d+)%\)\s*were (?:currently )?alive",
            text,
            re.IGNORECASE,
        )
        if all_alive_match:
            total = self._parse_int(all_alive_match.group(1))
            if total is not None:
                observations.extend(
                    self._count_pct_observations(claim, subgroup, "alive", total, 100.0)
                )
        elif alive_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    subgroup,
                    "alive",
                    self._parse_int(alive_match.group(1)),
                    self._parse_float(alive_match.group(2)),
                )
            )

        initiated_match = re.search(
            r"of these,\s*(\d[\d,]*)\s*\((\d+)%\)\s*were initiated (?:on|to) ART",
            text,
            re.IGNORECASE,
        )
        if initiated_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    subgroup,
                    "initiated_on_art",
                    self._parse_int(initiated_match.group(1)),
                    self._parse_float(initiated_match.group(2)),
                )
            )

        retained_match = re.search(
            r"only\s*(\d[\d,]*)\s*\((\d+)%\)\s*(?:among[^.]{0,80}?)?were retained on ART",
            text,
            re.IGNORECASE,
        )
        if retained_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    subgroup,
                    "retained_on_art",
                    self._parse_int(retained_match.group(1)),
                    self._parse_float(retained_match.group(2)),
                )
            )

        tested_match = re.search(
            r"only\s*(\d[\d,]*)\s*\((\d+)%\)\s*(?:underwent|were tested for)\s+viral load",
            text,
            re.IGNORECASE,
        )
        if tested_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    subgroup,
                    "viral_load_tested",
                    self._parse_int(tested_match.group(1)),
                    self._parse_float(tested_match.group(2)),
                )
            )

        suppressed_match = re.search(
            r"(?:of whom|with)\s*(\d[\d,]*)\s*\((\d+)%\)\s*(?:were )?(?:virally suppressed|viral load suppression)",
            text,
            re.IGNORECASE,
        )
        if not suppressed_match:
            suppressed_match = re.search(
                r"with\s*(\d+)%\s*\((\d[\d,]*)\)\s*(?:were )?(?:virally suppressed|viral load suppression)",
                text,
                re.IGNORECASE,
            )
            if suppressed_match:
                observations.extend(
                    self._count_pct_observations(
                        claim,
                        subgroup,
                        "viral_load_suppressed",
                        self._parse_int(suppressed_match.group(2)),
                        self._parse_float(suppressed_match.group(1)),
                    )
                )
                return observations

        if suppressed_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    subgroup,
                    "viral_load_suppressed",
                    self._parse_int(suppressed_match.group(1)),
                    self._parse_float(suppressed_match.group(2)),
                )
            )

        return observations

    def _national_art_vl_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if int(claim.get("page_index") or 0) != 0:
            return []
        if not (
            re.search(
                r"plhiv are currently on life[\s-]*saving\s+anti[\s-]*retroviral therapy",
                lowered,
                re.IGNORECASE,
            )
            or "diagnosed plhiv on treatment" in lowered
            or "tested for viral load" in lowered
            or "virally suppressed among plhiv on art" in lowered
            or "among plhiv on art" in lowered
            or "the plhiv on art" in lowered
        ):
            return []

        observations: list[dict] = []
        art_match = re.search(
            r"(\d[\d,]*)\s*\((\d+)%[^)]*\)\s*PLHIV are currently on [^.]{0,160}?ART",
            text,
            re.IGNORECASE,
        )
        if art_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "plhiv_on_art",
                    self._parse_int(art_match.group(1)),
                    self._parse_float(art_match.group(2)),
                )
            )
        else:
            art_count_match = re.search(
                r"(?:Further,\s*)?(\d[\d,\s]*)\s*PLHIV are currently on life[\s-]*saving\s+Anti[\s-]*retroviral Therapy\s*\(ART\)",
                text,
                re.IGNORECASE,
            ) or re.search(
                r"since 2002,\s*a total of\s*(\d[\d,\s]*)\s*PLHIV[^.]{0,120}?alive on ART",
                text,
                re.IGNORECASE,
            )
            if art_count_match:
                observations.extend(
                    self._count_pct_observations(
                        claim,
                        "national",
                        "plhiv_on_art",
                        self._parse_int(art_count_match.group(1)),
                        None,
                    )
                )

        treatment_pct_match = re.search(
            r"(?:Treatment coverage\s+(?:reached|was|is|stood at)\s*(\d+)%|(\d+)%\s+of the diagnosed\)?\s*PLHIV are currently on [^.]{0,120}?ART|Diagnosed\s+PLHIV\s+on\s+Treatment\s*(\d+)%)",
            text,
            re.IGNORECASE,
        )
        if treatment_pct_match and not any(row["metric_type"] == "plhiv_on_art_pct" for row in observations):
            treatment_pct = self._parse_float(
                treatment_pct_match.group(1)
                or treatment_pct_match.group(2)
                or treatment_pct_match.group(3)
            )
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "plhiv_on_art",
                    None,
                    treatment_pct,
                )
            )

        tested_match = re.search(
            r"of which,?\s*(\d[\d,]*)\s*\((\d+)%\)\s*PLHIV have been tested for viral load",
            text,
            re.IGNORECASE,
        )
        if tested_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "viral_load_tested",
                    self._parse_int(tested_match.group(1)),
                    self._parse_float(tested_match.group(2)),
                )
            )

        suppressed_match = re.search(
            r"Among those tested [^.]{0,120}?,\s*(\d[\d,]*)\s*\((\d+)%\)\s*were virally suppressed",
            text,
            re.IGNORECASE,
        )
        if suppressed_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "viral_load_suppressed",
                    self._parse_int(suppressed_match.group(1)),
                    self._parse_float(suppressed_match.group(2)),
                )
            )

        overall_supp_match = re.search(
            r"(?:however,\s*)?(?:only\s*)?(\d+)%\s*were virally suppressed[^.]{0,120}?among PLHIV on ART|Testing and Suppressing the viral load of 95% of\s*the\s*PLHIV on ART\s*(\d+)%",
            text,
            re.IGNORECASE,
        )
        if overall_supp_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "suppression_among_on_art_pct",
                    self._parse_float(overall_supp_match.group(1) or overall_supp_match.group(2)),
                    "percent",
                    "national",
                )
            )

        return [row for row in observations if row]

    def _historical_art_snapshot_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if (
            "presently on art" not in lowered
            and "presentlyon art" not in lowered
            and "presently on anti-retroviral therapy" not in lowered
            and "presently on antiretroviral therapy" not in lowered
        ):
            return []

        match = re.search(
            r"a total of\s*(\d[\d,]*)\*?\s*(?:people living with hiv\s*\(plhiv\)\s*)?were\s*presently\s*on\s*art\s*as\s*of",
            text,
            re.IGNORECASE,
        )
        if not match:
            match = re.search(
                r"as of\s+[A-Za-z]+\s+\d{4},\s*there (?:are|were)\s*(\d[\d,]*)\*?\s*(?:people living with hiv\s*\(plhiv\)\s*)?presently on\s*(?:anti[- ]retroviral therapy|art)",
                text,
                re.IGNORECASE,
            )
        if not match:
            return []

        count_value = self._parse_int(match.group(1))
        if count_value is None:
            return []

        return [
            self._metric_observation(
                claim,
                "plhiv_on_art_count",
                count_value,
                "count",
                "national",
            )
        ]

    def _national_diagnosed_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "estimated plhiv" not in lowered and "plhiv have been diagnosed" not in lowered:
            return []

        match = re.search(
            r"(\d[\d,]*)\s*\((\d+)%[^)]*\)\s*(?:cases|PLHIV)\s*have been diagnosed",
            text,
            re.IGNORECASE,
        )
        if not match:
            match = re.search(
                r"(\d[\d,]*)\s*\((\d+)%[^)]*\)\s*PLHIV have been diagnosed",
                text,
                re.IGNORECASE,
            )
        if not match:
            return []

        diagnosed_count = self._parse_int(match.group(1))
        diagnosed_pct = self._parse_float(match.group(2))
        on_art_match = re.search(
            r"Further,?\s*(\d[\d,]*)\s*PLHIV are currently on [^.]{0,80}?ART",
            text,
            re.IGNORECASE,
        )
        if on_art_match:
            on_art_count = self._parse_int(on_art_match.group(1))
            if (
                diagnosed_count is not None
                and on_art_count is not None
                and diagnosed_count < on_art_count
            ):
                diagnosed_count = None
        claim_year = self._parse_int(claim.get("year"))
        if (
            diagnosed_count is not None
            and diagnosed_pct is not None
            and claim_year is not None
            and claim_year >= 2020
            and diagnosed_pct >= 50.0
            and diagnosed_count < 50000
        ):
            diagnosed_count = None

        observations = [
            self._metric_observation(
                claim,
                "diagnosed_plhiv_count",
                diagnosed_count,
                "count",
                "national",
            ),
            self._metric_observation(
                claim,
                "diagnosed_plhiv_pct",
                diagnosed_pct,
                "percent",
                "national",
            ),
        ]
        return [row for row in observations if row]

    def _legacy_monthly_new_case_observations(self, claim: dict) -> list[dict]:
        if claim.get("period_granularity") != "month":
            return []

        text = self._claim_evidence_text(claim)
        match = re.search(
            r"(?:In|For the month of)\s+[A-Z][a-z]+\s+\d{4},\s+there (?:were|was)\s*(\d[\d,]*)\s*(?:newly diagnosed HIV cases|new HIV cases|new HIV Ab sero[- ]?positive individuals|new HIV antibody seropositive individuals)",
            text,
            re.IGNORECASE,
        )
        if not match:
            return []

        observations: list[dict] = []
        observations.append(
            self._metric_observation(
                claim,
                "new_cases_count",
                self._parse_int(match.group(1)),
                "count",
                "national",
            )
        )

        yoy_match = re.search(
            r"This was\s*(\d+)%\s*(higher|lower)\s*compared to the same period last year(?:\s*\(n=\s*(\d[\d,]*)\s*(?:in\s*\d{4})?\))?",
            text,
            re.IGNORECASE,
        )
        if yoy_match:
            yoy_pct = self._parse_float(yoy_match.group(1))
            if yoy_pct is not None and (yoy_match.group(2) or "").lower() == "lower":
                yoy_pct = -yoy_pct
            observations.append(
                self._metric_observation(
                    claim,
                    "new_cases_yoy_change_pct",
                    yoy_pct,
                    "percent",
                    "national",
                )
            )
            observations.append(
                self._metric_observation(
                    claim,
                    "new_cases_prior_year_same_month_count",
                    self._parse_int(yoy_match.group(3)),
                    "count",
                    "national",
                )
            )

        return [row for row in observations if row]

    def _legacy_monthly_death_observations(self, claim: dict) -> list[dict]:
        if claim.get("period_granularity") != "month":
            return []

        text = self._claim_evidence_text(claim)
        match = re.search(
            r"(?:For the month of|In)\s+[A-Z][a-z]+\s+\d{4},\s+there (?:were|was)\s*(\d[\d,]*)\*?\s*reported deaths(?: due to any cause)?",
            text,
            re.IGNORECASE,
        )
        if not match:
            return []

        observations: list[dict] = []
        total_count = self._parse_int(match.group(1))
        observations.append(
            self._metric_observation(
                claim,
                "reported_deaths_count",
                total_count,
                "count",
                "national",
            )
        )

        male_match = re.search(
            r"(\d+)%\s*\((\d[\d,]*)\)\s*were male while\s*(\d[\d,]*)\s*were female",
            text,
            re.IGNORECASE,
        )
        if male_match:
            male_pct = self._parse_float(male_match.group(1))
            male_count = self._parse_int(male_match.group(2))
            female_count = self._parse_int(male_match.group(3))
            female_pct = None
            if total_count and female_count is not None:
                female_pct = round((female_count / total_count) * 100, 1)
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "male",
                    "reported_deaths",
                    male_count,
                    male_pct,
                )
            )
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "female",
                    "reported_deaths",
                    female_count,
                    female_pct,
                )
            )

        return [row for row in observations if row]

    def _legacy_cumulative_death_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        match = re.search(
            r"a total of\s*(\d[\d,]*)\s*deaths were reported from January 1984 to",
            text,
            re.IGNORECASE,
        )
        if not match:
            return []

        return [
            row
            for row in [
                self._scoped_metric_observation(
                    claim,
                    "reported_deaths_count",
                    self._parse_int(match.group(1)),
                    "count",
                    "cumulative",
                    self._cumulative_period_label(claim),
                    region="Philippines",
                )
            ]
            if row
        ]

    def _legacy_transmission_summary_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if not any(marker in lowered for marker in ("sexual contact", "sharing of needles", "blood transfusion", "mother-to-child")):
            return []

        observations: list[dict] = []
        period_scope = "cumulative" if "from january 1984" in lowered or "from 1984 to" in lowered else claim.get("period_scope") or "snapshot"
        period_label = self._cumulative_period_label(claim) if period_scope == "cumulative" else claim.get("period_label") or ""

        pattern_specs = (
            ("sexual_contact_cases", r"(\d+)%\s*\((\d[\d,]*)\)\s*were (?:infected|acquired hiv) through sexual contact"),
            ("needle_transmission", r"(\d+)%\s*\(?(\d[\d,]*)\)?\s*through sharing of (?:infected )?needles"),
            ("blood_transmission", r"(\d+)%\s*\(?(\d[\d,]*)\)?\s*through blood(?:/blood products| transfusion)?"),
            ("mother_to_child_transmission", r"(\d+)%\s*\(?(\d[\d,]*)\)?\s*through mother[ -]to[ -]child transmission"),
        )
        for metric_base, pattern in pattern_specs:
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue
            observations.extend(
                self._scoped_count_pct_observations(
                    claim,
                    metric_base,
                    self._parse_int(match.group(2)),
                    self._parse_float(match.group(1)),
                    period_scope,
                    period_label,
                    region="Philippines",
                )
            )

        if observations:
            return [row for row in observations if row]

        fallback_specs = (
            ("blood_transmission_count", r"blood transfusion\s*\((\d[\d,]*)\)"),
            ("needle_prick_injury_transmission_count", r"needle prick injury\s*\((\d[\d,]*)\)"),
            ("mother_to_child_transmission_count", r"mother[ -]to[ -]child transmission\s*\((\d[\d,]*)\)"),
        )
        for metric_name, pattern in fallback_specs:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                observations.append(
                    self._scoped_metric_observation(
                        claim,
                        metric_name,
                        self._parse_int(match.group(1)),
                        "count",
                        period_scope,
                        period_label,
                        region="Philippines",
                    )
                )

        return [row for row in observations if row]

    def _regional_case_summary_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if not any(
            marker in lowered
            for marker in (
                "top five regions",
                "most number of reported cases",
                "most number of newly diagnosed cases",
                "most number of newly reported cases",
                "were from the national capital region",
                "most number of overseas filipino workers",
            )
        ):
            return []

        metric_base = self._narrative_metric_base(claim, lowered)
        subgroup = "migrant_workers" if any(marker in lowered for marker in (" ofw", "ofws", "overseas filipino workers", "worked overseas")) else ""
        period_scope = "cumulative" if any(marker in lowered for marker in ("from january 1984", "from 1984 to", "since 1984")) else ""
        period_label = self._cumulative_period_label(claim) if period_scope == "cumulative" else ""

        observations: list[dict] = []
        for region, count_value, pct_value in self._extract_region_count_pct_entries(text):
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    metric_base,
                    count_value,
                    pct_value,
                    region=region,
                    subgroup=subgroup,
                    period_scope=period_scope,
                    period_label=period_label,
                )
            )

        return [row for row in observations if row]

    def _age_group_distribution_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if not any(
            marker in lowered
            for marker in (
                "age group",
                "years old",
                "youth aged 15-24",
                "children less than 10",
                "adolescents aged 10-19",
                "50 years old and above",
            )
        ):
            return []

        metric_base = self._narrative_metric_base(claim, lowered)
        period_scope = "cumulative" if any(marker in lowered for marker in ("from january 1984", "from 1984 to", "since 1984")) else ""
        period_label = self._cumulative_period_label(claim) if period_scope == "cumulative" else ""
        age_specs = (
            (
                "age_lt_10",
                (
                    r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*(?:were )?children(?: less than| aged less than)?\s*10\s*years old",
                    r"(?P<count>\d[\d,]*)\s*(?:cases? )?were children(?: less than| aged less than)?\s*10\s*years old",
                ),
            ),
            (
                "age_10_19",
                (
                    r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*(?:were )?adolescents?(?: aged)?\s*10-19\s*years",
                    r"(?P<count>\d[\d,]*)\s*(?:cases? )?were adolescents?(?: aged)?\s*10-19\s*years",
                ),
            ),
            (
                "age_lt_15",
                (
                    r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*(?:were )?aged\s*<\s*15\s*years old",
                    r"(?P<count>\d[\d,]*)\s*(?:were )?aged\s*<\s*15\s*years old",
                ),
            ),
            (
                "age_15_24",
                (
                    r"\((?P<count>\d[\d,]*)\s*or\s*(?P<pct>\d+)%\)\s*(?:were |belong(?:ed)? to |from )?(?:the )?(?:youth aged\s*)?15-24\s*year(?:s)?(?: old)?\s*age group",
                    r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*(?:cases? were among|were|belong(?:ed)? to|from)?\s*(?:youth aged\s*)?15-24\s*years?(?: old)?",
                    r"(?P<count>\d[\d,]*)\s*(?:cases? were among|were)\s*youth aged\s*15-24\s*years",
                ),
            ),
            (
                "age_25_34",
                (
                    r"\((?P<count>\d[\d,]*)\s*or\s*(?P<pct>\d+)%\)\s*(?:were |belong(?:ed)? to |from )?(?:the )?25-34\s*year(?:s)?(?: old)?\s*age group",
                    r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*(?:were|belong(?:ed)? to|from)?\s*(?:the )?25-34\s*year(?:s)?(?: old)?\s*age group",
                    r"(?P<count>\d[\d,]*)\s*(?:were|belong(?:ed)? to)\s*(?:the )?25-34\s*year(?:s)?(?: old)?\s*age group",
                ),
            ),
            (
                "age_35_49",
                (
                    r"\((?P<count>\d[\d,]*)\s*or\s*(?P<pct>\d+)%\)\s*(?:were |belong(?:ed)? to |from )?(?:the )?35-49\s*year(?:s)?(?: old)?\s*age group",
                    r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*(?:were|belong(?:ed)? to|from)?\s*(?:the )?35-49\s*year(?:s)?(?: old)?\s*age group",
                    r"(?P<count>\d[\d,]*)\s*(?:were in|were|belong(?:ed)? to)\s*(?:the )?35-49\s*year(?:s)?(?: old)?\s*age group",
                ),
            ),
            (
                "age_50_plus",
                (
                    r"\((?P<count>\d[\d,]*)\s*or\s*(?P<pct>\d+)%\)\s*(?:were |belong(?:ed)? to |from )?(?:the )?50\s*years?(?: old)?\s*(?:and above|& older|older|old and above)",
                    r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*(?:were )?aged\s*50\s*years?(?: old)?\s*(?:and above|& older|older)",
                    r"(?P<count>\d[\d,]*)\s*(?:were )?aged\s*50\s*years?(?: old)?\s*(?:and above|& older|older)",
                ),
            ),
        )

        observations: list[dict] = []
        for subgroup, patterns in age_specs:
            count_value, pct_value = self._extract_first_count_pct(text, patterns)
            if count_value is None and pct_value is None:
                continue
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    metric_base,
                    count_value,
                    pct_value,
                    region="Philippines",
                    subgroup=subgroup,
                    period_scope=period_scope,
                    period_label=period_label,
                )
            )

        return [row for row in observations if row]

    def _ofw_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if not any(marker in lowered for marker in (" ofw", "ofws", "overseas filipino workers", "worked overseas", "migrant workers")):
            return []

        metric_base = "new_cases" if self._narrative_metric_base(claim, lowered) == "new_cases" else "reported_cases"
        period_scope = "cumulative" if any(marker in lowered for marker in ("from january 1984", "from 1984 to", "since 1984")) else ""
        period_label = self._cumulative_period_label(claim) if period_scope == "cumulative" else ""

        total_patterns = (
            r"out of the\s*\d[\d,]*\s*cases,\s*(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*were\s*hiv[- ]positive\s+ofws",
            r"there were\s*(?P<count>\d[\d,]*)\s*(?:hiv[- ]positive\s+)?ofws?\s+since\s+1984,\s*comprising\s*(?P<pct>\d+)%\s*of",
            r"(?P<count>\d[\d,]*)\s*ofws were reported to the harp[^.]{0,120}?comprising\s*(?P<pct>\d+)%\s*of",
            r"(?P<count>\d[\d,]*)\s*filipinos who worked overseas[^.]{0,120}?comprised\s*(?P<pct>\d+)%\s*of",
            r"since\s+1984,\s*a total of\s*(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*migrant workers among the diagnosed cases have been reported",
            r"since\s+1984,\s*(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*of diagnosed cases have been migrant workers",
            r"(?:[A-Za-z-]+\s*)?\((?P<count>\d[\d,]*)\)\s*ofws were reported(?: to the harp)?[^.]{0,120}?comprising\s*(?P<pct>\d+)%\s*of",
            r"(?:[A-Za-z-]+\s*)?\((?P<count>\d[\d,]*)\)\s*filipinos who worked overseas[^.]{0,120}?comprised\s*(?P<pct>\d+)%\s*of",
        )
        count_value, pct_value = self._extract_first_count_pct(text, total_patterns)
        if count_value is None:
            count_value, pct_value = self._extract_first_count_pct(
                text,
                (
                    r"there were\s*(?P<count>\d[\d,]*)\s*(?:hiv[- ]positive\s+)?ofws?\s+since\s+1984",
                    r"(?P<count>\d[\d,]*)\s*ofws were reported to the harp",
                    r"(?P<count>\d[\d,]*)\s*filipinos who worked overseas[^.]{0,120}?were newly reported",
                    r"since\s+1984,\s*a total of\s*(?P<count>\d[\d,]*)\s*migrant workers among the diagnosed cases have been reported",
                    r"(?:[A-Za-z-]+\s*)?\((?P<count>\d[\d,]*)\)\s*ofws were reported(?: to the harp)?",
                    r"(?:[A-Za-z-]+\s*)?\((?P<count>\d[\d,]*)\)\s*filipinos who worked overseas[^.]{0,120}?were newly reported",
                ),
            )

        observations: list[dict] = []
        if count_value is not None or pct_value is not None:
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    metric_base,
                    count_value,
                    pct_value,
                    region="Philippines",
                    subgroup="migrant_workers",
                    period_scope=period_scope,
                    period_label=period_label,
                )
            )

        for subgroup, pattern in (
            ("migrant_workers_male", r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*were male(?:s)?"),
            ("migrant_workers_female", r"(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*were female(?:s)?"),
        ):
            subgroup_count, subgroup_pct = self._extract_first_count_pct(text, (pattern,))
            if subgroup_count is None and subgroup_pct is None:
                continue
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    metric_base,
                    subgroup_count,
                    subgroup_pct,
                    region="Philippines",
                    subgroup=subgroup,
                    period_scope=period_scope,
                    period_label=period_label,
                )
            )

        if count_value is not None:
            for subgroup, pattern in (
                ("migrant_workers_male", r"(?P<pct>\d+)%\s*were male(?:s)?"),
                ("migrant_workers_female", r"(?P<pct>\d+)%\s*were female(?:s)?"),
            ):
                subgroup_count, subgroup_pct = self._extract_first_count_pct(text, (pattern,))
                if subgroup_count is not None or subgroup_pct is None:
                    continue
                derived_count = round((count_value * subgroup_pct) / 100)
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        metric_base,
                        derived_count,
                        subgroup_pct,
                        region="Philippines",
                        subgroup=subgroup,
                        period_scope=period_scope,
                        period_label=period_label,
                    )
                )

        if "most number of overseas filipino workers" in lowered or "ofw reported to the harp were" in lowered:
            for region, region_count, region_pct in self._extract_region_count_pct_entries(text):
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        metric_base,
                        region_count,
                        region_pct,
                        region=region,
                        subgroup="migrant_workers",
                        period_scope=period_scope,
                        period_label=period_label,
                    )
                )

        return [row for row in observations if row]

    def _program_vl_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "eligible for viral load testing" not in lowered:
            return []

        observations: list[dict] = []
        eligible_match = re.search(
            r"a total of\s*(\d[\d,]*)\s*individuals[^.]{0,140}?eligible for viral load testing",
            text,
            re.IGNORECASE,
        )
        if eligible_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "viral_load_eligible",
                    self._parse_int(eligible_match.group(1)),
                    "count",
                    "national",
                )
            )

        # Secondary program-summary pages often use a different denominator
        # ("eligible for VL testing") than the headline national cascade.
        # Keep the eligibility count, but do not emit competing headline-style
        # tested/suppressed national metrics from those pages.
        if int(claim.get("page_index") or 0) != 0:
            return [row for row in observations if row]

        tested_match = re.search(
            r"Of these eligible individuals,?\s*(\d[\d,]*)[^.]{0,40}?\((\d+)%\)\s*(?:PLHIV )?(?:underwent|were tested for)\s+viral load",
            text,
            re.IGNORECASE,
        )
        if tested_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "viral_load_tested",
                    self._parse_int(tested_match.group(1)),
                    self._parse_float(tested_match.group(2)),
                )
            )

        suppressed_match = re.search(
            r"among the\s*(\d[\d,]*)\s*PLHIV on ART who were tested[^.]{0,120}?,\s*(\d[\d,]*)\s*\((\d+)%\)\s*were virally suppressed",
            text,
            re.IGNORECASE,
        )
        if suppressed_match:
            tested_total = self._parse_int(suppressed_match.group(1))
            observations.append(
                self._metric_observation(
                    claim,
                    "viral_load_tested",
                    tested_total,
                    "count",
                    "national",
                )
            )
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "viral_load_suppressed",
                    self._parse_int(suppressed_match.group(2)),
                    self._parse_float(suppressed_match.group(3)),
                )
            )

        not_supp_match = re.search(
            r"while\s*(\d[\d,]*)\s*\((\d+)%\)\s*were not virally suppressed",
            text,
            re.IGNORECASE,
        )
        if not_supp_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "viral_load_not_suppressed",
                    self._parse_int(not_supp_match.group(1)),
                    self._parse_float(not_supp_match.group(2)),
                )
            )

        return [row for row in observations if row]

    def _compact_quarterly_surveillance_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        observations: list[dict] = []

        if "transgender women" in lowered and "15 - 24 years old" in lowered:
            total_match = re.search(
                r"there were\s*(?P<count>\d[\d,]*)\s*newly reported cases who identified as transgender women",
                text,
                re.IGNORECASE,
            )
            if total_match:
                observations.append(
                    self._custom_metric_observation(
                        claim,
                        "new_cases_count",
                        self._parse_int(total_match.group("count")),
                        "count",
                        region="Philippines",
                        subgroup="tgw",
                    )
                )

            for subgroup, patterns in (
                (
                    "tgw_age_15_24",
                    (
                        r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*were\s*15\s*-\s*24\s*years old",
                    ),
                ),
                (
                    "tgw_age_25_34",
                    (
                        r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*were\s*25\s*-\s*34\s*years old",
                    ),
                ),
                (
                    "tgw_age_35_49",
                    (
                        r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*were\s*35\s*-\s*49\s*years old",
                    ),
                ),
                (
                    "tgw_age_50_plus",
                    (
                        r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*were\s*50\s*years and older",
                    ),
                ),
            ):
                count_value, pct_value = self._extract_first_count_pct(text, patterns)
                if count_value is None and pct_value is None:
                    continue
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        "new_cases",
                        count_value,
                        pct_value,
                        region="Philippines",
                        subgroup=subgroup,
                    )
                )

        if "pregnant at the time of diagnosis" in lowered:
            snapshot_match = re.search(
                r"there were\s*(?P<count>\d[\d,]*)\s*hiv[- ]positive women[^.]{0,120}?pregnant at the time of diagnosis",
                text,
                re.IGNORECASE,
            )
            if snapshot_match:
                observations.append(
                    self._custom_metric_observation(
                        claim,
                        "pregnant_women_reported_count",
                        self._parse_int(snapshot_match.group("count")),
                        "count",
                        region="Philippines",
                        subgroup="pregnant_women",
                    )
                )

            yoy_match = re.search(
                r"this was a?\s*(?P<pct>\d+)%\s*increase compared to the same reporting period last year",
                text,
                re.IGNORECASE,
            )
            if yoy_match:
                observations.append(
                    self._custom_metric_observation(
                        claim,
                        "pregnant_women_reported_yoy_change_pct",
                        self._parse_float(yoy_match.group("pct")),
                        "percent",
                        region="Philippines",
                        subgroup="pregnant_women",
                    )
                )

        if "engaged in transactional sex" in lowered and "(n=" in lowered:
            caption_match = re.search(
                r"engaged in transactional sex.*?(?P<range>[A-Za-z]{3,9}\s+\d{4}\s*-\s*[A-Za-z]{3,9}\s+\d{4})\s*\(n=\s*(?P<count>\d[\d,]*)\)",
                text,
                re.IGNORECASE,
            )
            if caption_match:
                observations.append(
                    self._custom_metric_observation(
                        claim,
                        "reported_cases_count",
                        self._parse_int(caption_match.group("count")),
                        "count",
                        region="Philippines",
                        subgroup="transactional_sex",
                        period_scope="recent_window",
                        period_label=re.sub(r"\s+", " ", caption_match.group("range")).strip(),
                    )
                )

        if "newly reported cases had acquired hiv through sexual contact" in lowered:
            summary_match = re.search(
                r"in the .*?quarter.*?,\s*(?P<count>\d[\d,]*)\s*\((?P<pct>\d+)%\)\s*newly reported cases had acquired hiv through sexual contact",
                text,
                re.IGNORECASE,
            )
            if summary_match:
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        "new_cases_sexual_contact",
                        self._parse_int(summary_match.group("count")),
                        self._parse_float(summary_match.group("pct")),
                        region="Philippines",
                    )
                )

            for metric_type, pattern in (
                ("new_cases_male_male_sex_count", r"(?P<count>\d[\d,]*)\s*through male-male sex"),
                (
                    "new_cases_male_male_female_sex_count",
                    r"(?P<count>\d[\d,]*)\s*male\s*-\s*male\s*/\s*female(?:\s*\d+)?",
                ),
                ("new_cases_male_female_sex_count", r"(?P<count>\d[\d,]*)\s*male\s*-\s*female sex"),
            ):
                match = re.search(pattern, text, re.IGNORECASE)
                if not match:
                    continue
                observations.append(
                    self._custom_metric_observation(
                        claim,
                        metric_type,
                        self._parse_int(match.group("count")),
                        "count",
                        region="Philippines",
                    )
                )

        if "reported sharing of infected needles" in lowered:
            needle_match = re.search(
                r"(?P<count>\d[\d,]*)\s*\((?P<pct><?\d+)%\)\s*reported sharing of infected needles",
                text,
                re.IGNORECASE,
            )
            if needle_match:
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        "needle_transmission",
                        self._parse_int(needle_match.group("count")),
                        self._parse_float(needle_match.group("pct")),
                        region="Philippines",
                    )
                )

        if "mother-to-child transmission" in lowered:
            quarter_mtct_match = re.search(
                r"(?P<count>\d[\d,]*)\s*\((?P<pct><?\d+)%\)\s*through mother-to-child transmission",
                text,
                re.IGNORECASE,
            )
            if quarter_mtct_match:
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        "mother_to_child_transmission",
                        self._parse_int(quarter_mtct_match.group("count")),
                        self._parse_float(quarter_mtct_match.group("pct")),
                        region="Philippines",
                    )
                )

            cumulative_mtct_match = re.search(
                r"out of a total of\s*(?P<total>\d[\d,]*)\s*cases,\s*more than half\s*\((?P<pct>\d+)%\s*,\s*(?P<recent>\d[\d,]*)\)\s*were reported between 2019 and june 2024",
                text,
                re.IGNORECASE,
            )
            if cumulative_mtct_match:
                observations.append(
                    self._custom_metric_observation(
                        claim,
                        "mother_to_child_transmission_count",
                        self._parse_int(cumulative_mtct_match.group("total")),
                        "count",
                        region="Philippines",
                        period_scope="cumulative",
                        period_label=self._cumulative_period_label(claim),
                    )
                )
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        "mother_to_child_transmission",
                        self._parse_int(cumulative_mtct_match.group("recent")),
                        self._parse_float(cumulative_mtct_match.group("pct")),
                        region="Philippines",
                        period_scope="recent_window",
                        period_label="2019-2024 H1",
                    )
                )

        if "unknown mode of transmission" in lowered:
            unknown_match = re.search(
                r"(?P<count>\d[\d,]*)\s*cases\s*\((?P<pct>\d+)%\)\s*have an unknown mode of transmission",
                text,
                re.IGNORECASE,
            )
            if unknown_match:
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        "unknown_mot",
                        self._parse_int(unknown_match.group("count")),
                        self._parse_float(unknown_match.group("pct")),
                        region="Philippines",
                        period_scope="cumulative",
                        period_label=self._cumulative_period_label(claim),
                    )
                )

        if "blood/blood products" in lowered and "needlestick injuries" in lowered:
            combined_match = re.search(
                r"reported in\s*(?P<count>\d[\d,]*)\s*cases\s*\((?P<pct><?\d+)%\)",
                text,
                re.IGNORECASE,
            )
            if combined_match:
                observations.extend(
                    self._custom_count_pct_observations(
                        claim,
                        "blood_and_needlestick_transmission",
                        self._parse_int(combined_match.group("count")),
                        self._parse_float(combined_match.group("pct")),
                        region="Philippines",
                        period_scope="cumulative",
                        period_label=self._cumulative_period_label(claim),
                    )
                )

        if "virally suppressed among plhiv on art" in lowered:
            suppression_match = re.search(
                r"only\s*(?P<pct>\d+)%\s*were virally suppressed among plhiv on art",
                text,
                re.IGNORECASE,
            )
            if suppression_match:
                observations.append(
                    self._custom_metric_observation(
                        claim,
                        "suppression_among_on_art_pct",
                        self._parse_float(suppression_match.group("pct")),
                        "percent",
                        region="Philippines",
                    )
                )

        return [row for row in observations if row]

    def _standalone_vl_summary_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "virally suppressed" not in lowered and "tested for viral load" not in lowered:
            return []
        if int(claim.get("page_index") or 0) != 0:
            return []

        observations: list[dict] = []

        tested_match = re.search(
            r"(\d[\d,]*)\s*\((\d+)%\)\s*PLHIV have been tested for viral load",
            text,
            re.IGNORECASE,
        )
        if tested_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "viral_load_tested",
                    self._parse_int(tested_match.group(1)),
                    self._parse_float(tested_match.group(2)),
                )
            )
        else:
            tested_pct_match = re.search(
                r"\((\d+)%\)\s*PLHIV have been tested for viral load",
                text,
                re.IGNORECASE,
            )
            if tested_pct_match:
                observations.extend(
                    self._count_pct_observations(
                        claim,
                        "national",
                        "viral_load_tested",
                        None,
                        self._parse_float(tested_pct_match.group(1)),
                    )
                )

        suppressed_match = re.search(
            r"Among those tested for VL,?\s*(\d[\d,]*)\s*\((\d+)%\)\s*(?:are|were)\s*virally suppressed",
            text,
            re.IGNORECASE,
        )
        if suppressed_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "viral_load_suppressed",
                    self._parse_int(suppressed_match.group(1)),
                    self._parse_float(suppressed_match.group(2)),
                )
            )

        return [row for row in observations if row]

    def _transactional_sex_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "engaged in transactional sex" not in lowered or "newly diagnosed" not in lowered:
            return []

        match = re.search(
            r"(\d[\d,]*)\s*\((\d+)%\)\s*of the newly diagnosed engaged in transactional sex",
            text,
            re.IGNORECASE,
        )
        if not match:
            return []

        return self._count_pct_observations(
            claim,
            "transactional_sex",
            "new_cases",
            self._parse_int(match.group(1)),
            self._parse_float(match.group(2)),
        )

    def _art_regimen_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "first-line regimen" not in lowered:
            return []

        observations: list[dict] = []
        regimen_patterns = (
            ("first_line_regimen", r"(\d[\d,]*)\s*\((\d+)%\)\s*were on a first-line regimen"),
            ("second_line_regimen", r"(\d[\d,]*)\s*\((\d+)%\)\s*were on a second-line regimen"),
            ("salvage_regimen", r"(\d[\d,]*)\s*\((\d+)%\)\s*were on salvage therapy"),
        )
        for metric_base, pattern in regimen_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                observations.extend(
                    self._count_pct_observations(
                        claim,
                        "national",
                        metric_base,
                        self._parse_int(match.group(1)),
                        self._parse_float(match.group(2)),
                    )
                )
        return [row for row in observations if row]

    def _pregnant_reported_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "reported pregnant at the time of diagnosis" not in lowered:
            return []

        match = re.search(
            r"total of\s*(\d[\d,]*)\s*diagnosed women were reported pregnant at the time of diagnosis",
            text,
            re.IGNORECASE,
        )
        if not match:
            return []

        count_value = self._parse_int(match.group(1))
        if count_value is None:
            return []

        return [
            self._observation_row(
                claim,
                observation_id=f"{claim['claim_id']}:pregnant_women_reported_count:pregnant_women",
                metric_type="pregnant_women_reported_count",
                value=count_value,
                unit="count",
                region="Philippines",
                subgroup="pregnant_women",
                period_scope="cumulative",
                period_label=claim.get("period_label") or "",
            )
        ]

    def _tgw_transmission_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "tgw diagnosed" not in lowered or "acquired hiv through sexual contact" not in lowered:
            return []

        observations: list[dict] = []
        total_value = None

        total_match = re.search(r"Of the\s*(\d[\d,]*)\s*TGW diagnosed", text, re.IGNORECASE)
        if total_match:
            total_value = self._parse_int(total_match.group(1))
            if total_value is not None:
                observations.append(
                    self._metric_observation(
                        claim,
                        "tgw_diagnosed_count",
                        total_value,
                        "count",
                        "tgw",
                    )
                )

        sexual_match = re.search(
            r"(?:almost all\s*)?\(?\s*(\d[\d,\s]*)\s*(?:,)?\s*\(?<?(\d+)%\)?\s*acquired HIV through sexual contact",
            text,
            re.IGNORECASE,
        )
        if sexual_match:
            sexual_count = self._parse_int(sexual_match.group(1))
            sexual_pct = self._parse_float(sexual_match.group(2))
            if (
                total_value is not None
                and sexual_count is not None
                and (
                    sexual_count > total_value
                    or (sexual_pct is not None and sexual_pct >= 80.0 and sexual_count < (total_value * 0.5))
                )
            ):
                sexual_count = None
            if sexual_count is not None or sexual_pct is not None:
                observations.extend(
                    self._count_pct_observations(
                        claim,
                        "tgw",
                        "sexual_contact_cases",
                        sexual_count,
                        sexual_pct,
                    )
                )

        needle_match = re.search(
            r"([A-Za-z]+|\d[\d,]*)\s*\(?<?(\d+)?%?\)?\s*through sharing of infected needles",
            text,
            re.IGNORECASE,
        )
        if needle_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "needle_transmission_count",
                    self._parse_int(needle_match.group(1)),
                    "count",
                    "tgw",
                )
            )

        mtct_match = re.search(
            r"([A-Za-z]+|\d[\d,]*)\s*(?:through|case through|cases through)\s*mother[ -]to[ -]child transmission",
            text,
            re.IGNORECASE,
        )
        if mtct_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "mother_to_child_transmission_count",
                    self._parse_int(mtct_match.group(1)),
                    "count",
                    "tgw",
                )
            )

        missing_match = re.search(
            r"(\d[\d,]*)\s*\((\d+)%\)\s*had no data on MOT",
            text,
            re.IGNORECASE,
        )
        if missing_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "tgw",
                    "missing_mot",
                    self._parse_int(missing_match.group(1)),
                    self._parse_float(missing_match.group(2)),
                )
            )

        return [row for row in observations if row]

    def _national_transmission_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "reported cases" not in lowered or "acquired through sexual contact" not in lowered:
            return []

        match = re.search(
            r"of the\s*(\d[\d,]*)\s*reported cases,?\s*(\d[\d,]*)\s*\((\d+)%\)\s*were acquired through sexual contact",
            text,
            re.IGNORECASE,
        )
        if not match:
            return []

        return self._count_pct_observations(
            claim,
            "national",
            "sexual_contact_cases",
            self._parse_int(match.group(2)),
            self._parse_float(match.group(3)),
        )

    def _art_ltfu_breakdown_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "lost to follow-up" not in lowered:
            return []

        observations: list[dict] = []
        ltfu_match = re.search(
            r"includes\s*(\d[\d,]*)\s*individuals who were lost to follow-up",
            text,
            re.IGNORECASE,
        )
        if ltfu_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "lost_to_follow_up_count",
                    self._parse_int(ltfu_match.group(1)),
                    "count",
                    "national",
                )
            )

        refused_match = re.search(
            r"([A-Za-z]+|\d[\d,]*)\s*who refused to continue ART",
            text,
            re.IGNORECASE,
        )
        if refused_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "refused_art_count",
                    self._parse_int(refused_match.group(1)),
                    "count",
                    "national",
                )
            )

        migrated_match = re.search(
            r"([A-Za-z]+|\d[\d,]*)\s*who reported migrat(?:ing|ed) overseas",
            text,
            re.IGNORECASE,
        )
        if migrated_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "migrated_overseas_count",
                    self._parse_int(migrated_match.group(1)),
                    "count",
                    "national",
                )
            )

        return [row for row in observations if row]

    def _art_current_status_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        observations: list[dict] = []

        alive_match = re.search(
            r"Among the\s*(\d[\d,]*)\s*people living with HIV .*? a total of\s*(\d[\d,]*)\s*individuals .*? were alive on ART",
            text,
            re.IGNORECASE,
        )
        if alive_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "ever_enrolled_art_count",
                    self._parse_int(alive_match.group(1)),
                    "count",
                    "national",
                )
            )
            observations.append(
                self._metric_observation(
                    claim,
                    "alive_on_art_count",
                    self._parse_int(alive_match.group(2)),
                    "count",
                    "national",
                )
            )

        if "no longer receiving treatment" in lowered:
            not_on_tx_match = re.search(
                r"As of .*?,\s*(\d[\d,]*)\s*individuals?\s*\((\d+)%\)\s*who were previously on ART were no longer receiving treatment",
                text,
                re.IGNORECASE,
            )
            if not_on_tx_match:
                observations.extend(
                    self._count_pct_observations(
                        claim,
                        "national",
                        "not_on_treatment",
                        self._parse_int(not_on_tx_match.group(1)),
                        self._parse_float(not_on_tx_match.group(2)),
                    )
                )

        if "concentrated in the greater manila area" in lowered:
            gmm_match = re.search(
                r"(\d+)%\s*of the PLHIV on ART are concentrated in the Greater Manila Area",
                text,
                re.IGNORECASE,
            )
            if gmm_match:
                observations.append(
                    self._metric_observation(
                        claim,
                        "gmm_art_share_pct",
                        self._parse_float(gmm_match.group(1)),
                        "percent",
                        "national",
                    )
                )

        return [row for row in observations if row]

    def _ahd_summary_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        observations: list[dict] = []

        ahd_match = re.search(
            r"Among the total reported cases,?\s*(\d[\d,]*)\s*\((\d+)%\)\s*were diagnosed with Advanced HIV disease",
            text,
            re.IGNORECASE,
        )
        if ahd_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "advanced_hiv_disease",
                    self._parse_int(ahd_match.group(1)),
                    self._parse_float(ahd_match.group(2)),
                )
            )

        missing_match = re.search(
            r"unavailable for the remaining\s*(\d[\d,]*)\s*\((\d+)%\)\s*cases",
            text,
            re.IGNORECASE,
        )
        if missing_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "missing_immunologic_clinical_data",
                    self._parse_int(missing_match.group(1)),
                    self._parse_float(missing_match.group(2)),
                )
            )

        return [row for row in observations if row]

    def _quarter_new_cases_mot_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if "newly reported cases had acquired HIV through sexual contact" not in lowered:
            return []

        observations: list[dict] = []
        summary_match = re.search(
            r"In the .*?quarter.*?,\s*(\d[\d,]*)\s*\((\d+)%\)\s*newly reported cases had acquired HIV through sexual contact",
            text,
            re.IGNORECASE,
        )
        if summary_match:
            observations.extend(
                self._count_pct_observations(
                    claim,
                    "national",
                    "new_cases_sexual_contact",
                    self._parse_int(summary_match.group(1)),
                    self._parse_float(summary_match.group(2)),
                )
            )

        male_male_match = re.search(
            r"(\d[\d,]*)\s*through male-male sex",
            text,
            re.IGNORECASE,
        )
        if male_male_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "new_cases_male_male_sex_count",
                    self._parse_int(male_male_match.group(1)),
                    "count",
                    "national",
                )
            )

        male_male_female_match = re.search(
            r"(\d[\d,]*)\s*male\s*-\s*male\s*/\s*female",
            text,
            re.IGNORECASE,
        )
        if male_male_female_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "new_cases_male_male_female_sex_count",
                    self._parse_int(male_male_female_match.group(1)),
                    "count",
                    "national",
                )
            )

        male_female_match = re.search(
            r"(\d[\d,]*)\s*male\s*-\s*female sex",
            text,
            re.IGNORECASE,
        )
        if male_female_match:
            observations.append(
                self._metric_observation(
                    claim,
                    "new_cases_male_female_sex_count",
                    self._parse_int(male_female_match.group(1)),
                    "count",
                    "national",
                )
            )

        return [row for row in observations if row]

    def _historical_epidemic_summary_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if (
            "first case of hiv infection in the philippines was reported" not in lowered
            and "since then, there have been" not in lowered
            and "from january 1984 to" not in lowered
        ):
            return []

        observations: list[dict] = []

        cumulative_count, _ = self._extract_first_count_pct(
            text,
            (
                r"since then, there have been\s*(?P<count>\d[\d,]*)\s*(?:confirmed\s*)?hiv cases reported to the harp",
                r"from january 1984 to[^,.]{0,80},\s*there has been\s*(?P<count>\d[\d,]*)\s*(?:confirmed\s*)?hiv cases",
                r"from january 1984 to[^,.]{0,80},\s*there were\s*(?P<count>\d[\d,]*)\s*hiv ab seropositive cases reported",
                r"from january 1984 to[^,.]{0,80},\s*there were\s*(?P<count>\d[\d,]*)\s*(?:confirmed\s*)?hiv cases reported",
            ),
        )
        if cumulative_count is not None:
            observations.append(
                self._scoped_metric_observation(
                    claim,
                    "reported_cases_count",
                    cumulative_count,
                    "count",
                    "cumulative",
                    self._cumulative_period_label(claim),
                    region="Philippines",
                )
            )

        male_share_match = re.search(
            r"(\d+)%\s*\((\d[\d,]*)\)\s*of those diagnosed were malee?",
            text,
            re.IGNORECASE,
        )
        if male_share_match:
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    "reported_cases",
                    self._parse_int(male_share_match.group(2)),
                    self._parse_float(male_share_match.group(1)),
                    region="Philippines",
                    subgroup="male",
                    period_scope="cumulative",
                    period_label=self._cumulative_period_label(claim),
                )
            )

        female_share_match = re.search(
            r"(\d+)%\s*\((\d[\d,]*)\)\s*were female",
            text,
            re.IGNORECASE,
        )
        if female_share_match:
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    "reported_cases",
                    self._parse_int(female_share_match.group(2)),
                    self._parse_float(female_share_match.group(1)),
                    region="Philippines",
                    subgroup="female",
                    period_scope="cumulative",
                    period_label=self._cumulative_period_label(claim),
                )
            )

        msm_share_match = re.search(
            r"(?P<pct>\d+)%\s*\((?P<count>\d[\d,]*)\)\s*out of the total\s*\((?P<total>\d[\d,]*)\)\s*diagnosed cases were among msm",
            text,
            re.IGNORECASE,
        )
        if msm_share_match:
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    "reported_cases",
                    self._parse_int(msm_share_match.group("count")),
                    self._parse_float(msm_share_match.group("pct")),
                    region="Philippines",
                    subgroup="msm",
                    period_scope="cumulative",
                    period_label=self._cumulative_period_label(claim),
                )
            )

        return [row for row in observations if row]

    def _narrative_transmission_breakdown_observations(self, claim: dict) -> list[dict]:
        text = self._claim_evidence_text(claim)
        lowered = text.lower()
        if not any(
            marker in lowered
            for marker in (
                "male-male sex",
                "male to male sex",
                "homosexual",
                "bisexual",
                "sex with both males",
                "male-female sex",
                "heterosexual",
                "mother-to-child transmission",
                "sharing of infected needles",
            )
        ):
            return []

        subgroup = self._detect_narrative_subgroup(lowered)
        period_scope = "cumulative" if any(marker in lowered for marker in ("from january 1984", "from 1984 to", "since 1984")) else ""
        period_label = self._cumulative_period_label(claim) if period_scope == "cumulative" else ""
        region = "Philippines" if (claim.get("region") or "Unknown") == "Unknown" else claim.get("region")

        observations: list[dict] = []
        sexual_total_count = None
        sexual_total_pct = None

        total_patterns = (
            r"(?P<pct>\d+)%\s*\((?P<count>\d[\d,]*)\)\s*(?:were infected through|acquired the infection through|acquired hiv through)\s*sexual contact",
            r"sexual contact remained as the predominant mode of transmission\s*\((?P<pct>\d+)%\s*,\s*(?P<count>\d[\d,]*)\)",
            r"most\s*\((?P<pct>\d+)%\)\s*of the cases were infected through sexual contact",
            r"sexual contact\s*\((?P<count>\d[\d,]*)\s*or\s*(?P<pct>\d+)%\)",
        )
        sexual_total_count, sexual_total_pct = self._extract_first_count_pct(text, total_patterns)
        if sexual_total_count is not None or sexual_total_pct is not None:
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    "sexual_contact_cases",
                    sexual_total_count,
                    sexual_total_pct,
                    region=region,
                    subgroup=subgroup,
                    period_scope=period_scope,
                    period_label=period_label,
                )
            )

        metric_specs = (
            (
                "male_male_sex_cases",
                (
                    r"(?P<pct>\d+)%\s*\((?P<count>\d[\d,]*)\)\s*reported transmission through male to male sex",
                    r"homosexual\s*\((?P<count>[A-Za-z]+|\d[\d,]*)\)",
                    r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*male-male sex",
                    r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*reported transmission through male to male sex",
                    r"male-male sex\s*\((?P<count>\d[\d,]*)\s*or\s*(?P<pct>\d+)%\)",
                ),
            ),
            (
                "male_male_female_sex_cases",
                (
                    r"(?P<pct>\d+)%\s*\((?P<count>\d[\d,]*)\)\s*through males who have sex with both males and females",
                    r"bisexual(?: contact)?\s*\((?P<count>[A-Za-z]+|\d[\d,]*)\)",
                    r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*sex with both males?\s*&\s*females?\w*",
                    r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*had sex with both males? and females?",
                    r"sex with both males?\s*&\s*females?\s*\((?P<count>\d[\d,]*)\s*or\s*(?P<pct>\d+)%\)",
                ),
            ),
            (
                "male_female_sex_cases",
                (
                    r"(?P<pct>\d+)%\s*\((?P<count>\d[\d,]*)\)\s*(?:were\s*)?through male to female sex",
                    r"heterosexual\s*\((?P<count>[A-Za-z]+|\d[\d,]*)\)",
                    r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*male-female sex",
                    r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*through male to female sex",
                    r"male-female sex was the most common mot\s*\((?P<count>\d[\d,]*)\s*or\s*(?P<pct>\d+)%\)",
                ),
            ),
            (
                "needle_transmission",
                (
                    r"sharing of infected needles\s*\((?P<pct><?\d+)%\s*,\s*(?P<count>\d[\d,]*)\)",
                    r"(?P<pct>\d+)%\s*\((?P<count>\d[\d,]*)\)\s*through sharing of infected needles",
                    r"sharing of infected needles\s*\((?P<count>[A-Za-z]+|\d[\d,]*)\)",
                    r"sharing of infected needles\s*\((?P<count>\d[\d,]*)\s*or\s*(?P<pct><?\d+)%\)",
                ),
            ),
            (
                "mother_to_child_transmission",
                (
                    r"mother-to-child transmission\s*\((?P<pct><?\d+)%\s*,\s*(?P<count>\d[\d,]*)\)",
                    r"(?P<pct>\d+)%\s*\((?P<count>\d[\d,]*)\)\s*through mother-to-child transmission",
                    r"mother-to-child transmission\s*\((?P<count>[A-Za-z]+|\d[\d,]*)\)",
                    r"(?P<count>[A-Za-z]+|\d[\d,]*)\s*(?:were )?(?:acquired through|through)\s*mother-to-child transmission",
                ),
            ),
        )

        for metric_base, patterns in metric_specs:
            count_value, pct_value = self._extract_first_count_pct(text, patterns)
            if count_value is None and pct_value is None:
                continue
            if pct_value is None and count_value is not None and sexual_total_count and count_value <= sexual_total_count:
                pct_value = round((count_value / sexual_total_count) * 100, 1)
            observations.extend(
                self._custom_count_pct_observations(
                    claim,
                    metric_base,
                    count_value,
                    pct_value,
                    region=region,
                    subgroup=subgroup,
                    period_scope=period_scope,
                    period_label=period_label,
                )
            )

        return [row for row in observations if row]

    def _claim_evidence_text(self, claim: dict) -> str:
        text = " ".join(
            part.strip()
            for part in (
                claim.get("claim_text") or "",
                claim.get("snippet") or "",
            )
            if part and part.strip()
        )
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _detect_cohort_subgroup(self, lowered_text: str) -> str:
        if "pregnant women" in lowered_text or "reported pregnant" in lowered_text:
            return "pregnant_women"
        if "tgw" in lowered_text or "transgender women" in lowered_text:
            return "tgw"
        return ""

    def _detect_narrative_subgroup(self, lowered_text: str) -> str:
        if " ofw" in lowered_text or "ofws" in lowered_text or "overseas filipino" in lowered_text or "worked overseas" in lowered_text:
            return "migrant_workers"
        if " among msm" in lowered_text or " men who have sex with men" in lowered_text or " among men who have sex with men" in lowered_text:
            return "msm"
        if "pregnant women" in lowered_text or "reported pregnant" in lowered_text:
            return "pregnant_women"
        if "tgw" in lowered_text or "transgender women" in lowered_text:
            return "tgw"
        if "transactional sex" in lowered_text:
            return "transactional_sex"
        if "children" in lowered_text and "10 years old" in lowered_text:
            return "age_lt_10"
        if "adolescents" in lowered_text and "10-19" in lowered_text:
            return "age_10_19"
        if "youth aged 15-24" in lowered_text:
            return "age_15_24"
        return ""

    def _narrative_metric_base(self, claim: dict, lowered_text: str) -> str:
        metric_type = claim.get("metric_type") or ""
        if metric_type == "deaths" or "reported deaths" in lowered_text:
            return "reported_deaths"
        if metric_type == "new_cases" or any(
            marker in lowered_text
            for marker in ("newly diagnosed", "newly reported", "new hiv ab sero", "sero-positive individuals")
        ):
            return "new_cases"
        return "reported_cases"

    def _extract_first_count_pct(self, text: str, patterns: tuple[str, ...]) -> tuple[int | None, float | None]:
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue
            count_value = self._parse_int(match.groupdict().get("count"))
            pct_value = self._parse_float(match.groupdict().get("pct"))
            return count_value, pct_value
        return None, None

    def _custom_count_pct_observations(
        self,
        claim: dict,
        metric_base: str,
        count_value: int | None,
        pct_value: float | None,
        *,
        region: str | None = None,
        subgroup: str = "",
        period_scope: str = "",
        period_label: str = "",
    ) -> list[dict]:
        observations = []
        observations.append(
            self._custom_metric_observation(
                claim,
                f"{metric_base}_count",
                count_value,
                "count",
                region=region,
                subgroup=subgroup,
                period_scope=period_scope,
                period_label=period_label,
            )
        )
        observations.append(
            self._custom_metric_observation(
                claim,
                f"{metric_base}_pct",
                pct_value,
                "percent",
                region=region,
                subgroup=subgroup,
                period_scope=period_scope,
                period_label=period_label,
            )
        )
        return [row for row in observations if row]

    def _custom_metric_observation(
        self,
        claim: dict,
        metric_type: str,
        value: float | int | None,
        unit: str,
        *,
        region: str | None = None,
        subgroup: str = "",
        period_scope: str = "",
        period_label: str = "",
    ) -> dict | None:
        if value is None:
            return None

        resolved_period_scope = period_scope or claim.get("period_scope") or "snapshot"
        resolved_period_label = period_label or claim.get("period_label") or ""
        resolved_period_granularity = claim.get("period_granularity") or ""
        if resolved_period_scope == "cumulative":
            resolved_period_granularity = "cumulative"
        elif resolved_period_scope == "year_to_date":
            resolved_period_granularity = "year_to_date"
        elif resolved_period_scope == "recent_window":
            resolved_period_granularity = "recent_window"

        resolved_month = claim.get("month") if resolved_period_granularity == "month" else ""
        resolved_region = region or claim.get("region") or ""

        return self._observation_row(
            claim,
            observation_id=(
                f"{claim['claim_id']}:{metric_type}:{resolved_region or 'all'}:"
                f"{subgroup or 'all'}:{resolved_period_scope}:{resolved_period_label or 'na'}"
            ),
            metric_type=metric_type,
            value=value,
            unit=unit,
            region=resolved_region,
            subgroup=subgroup,
            period_scope=resolved_period_scope,
            period_label=resolved_period_label,
            period_granularity=resolved_period_granularity,
            month=resolved_month,
        )

    def _extract_region_count_pct_entries(self, text: str) -> list[tuple[str, int | None, float | None]]:
        region_pattern = (
            r"(?:National Capital Region\s*\(NCR\)|National Capital Region|"
            r"Cordillera Administrative Region\s*\(CAR\)|Cordillera Administrative Region|"
            r"Bangsamoro Autonomous Region in Muslim Mindanao\s*\(BARMM\)|"
            r"Autonomous Region in Muslim Mindanao\s*\(ARMM\)|"
            r"Region\s*[IVX0-9]+(?:\s*[-]?\s*[ABab])?|NCR|CARAGA|BARMM|ARMM|NIR|CAR)"
        )
        patterns = (
            rf"(?P<region>{region_pattern})\s+with\s+(?P<count>\d[\d,]*)\s*\((?P<pct><?\d+(?:\.\d+)?)%\)",
            rf"(?P<region>{region_pattern})\s*\((?P<pct><?\d+(?:\.\d+)?)%\s*,\s*(?P<count>\d[\d,]*)\)",
            rf"\((?P<pct><?\d+(?:\.\d+)?)%\s*,\s*(?P<count>\d[\d,]*)\)\s*were from the\s+(?P<region>{region_pattern})",
            rf"(?P<region>{region_pattern})\s+with\s+(?P<count>\d[\d,]*)\s*cases",
        )

        matches: list[tuple[str, int | None, float | None]] = []
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                region = self._normalize_region_token(match.group("region"))
                if not region:
                    continue
                matches.append(
                    (
                        region,
                        self._parse_int(match.groupdict().get("count")),
                        self._parse_float(match.groupdict().get("pct")),
                    )
                )

        deduped: OrderedDict[tuple[str, int | None, float | None], tuple[str, int | None, float | None]] = OrderedDict()
        for row in matches:
            deduped.setdefault(row, row)
        return list(deduped.values())

    def _normalize_region_token(self, token: str) -> str:
        text = re.sub(r"\s+", " ", (token or "")).strip()
        if not text:
            return ""

        lowered = text.lower()
        alias_specs = (
            ("NCR", r"\bncr\b|national capital region"),
            ("CAR", r"\bcar\b|cordillera administrative region"),
            ("CARAGA", r"\bcaraga\b|region xiii"),
            ("BARMM", r"\bbarmm\b|bangsamoro autonomous region in muslim mindanao"),
            ("ARMM", r"\barmm\b|autonomous region in muslim mindanao"),
            ("NIR", r"\bnir\b|negros island region"),
        )
        for region_code, pattern in alias_specs:
            if re.search(pattern, lowered, re.IGNORECASE):
                return region_code

        match = re.search(r"region\s*([ivx]+|\d{1,2})(?:\s*[-]?\s*([ab]))?", lowered, re.IGNORECASE)
        if not match:
            return ""

        base = match.group(1).lower()
        numeric = self.ROMAN_REGION_MAP.get(base, base.upper())
        suffix = (match.group(2) or "").upper()
        return f"{numeric}{suffix}"

    def _count_pct_observations(
        self,
        claim: dict,
        subgroup: str,
        metric_base: str,
        count_value: int | None,
        pct_value: float | None,
    ) -> list[dict]:
        observations = []
        if count_value is not None:
            observations.append(
                self._metric_observation(
                    claim,
                    f"{metric_base}_count",
                    count_value,
                    "count",
                    subgroup,
                )
            )
        if pct_value is not None:
            observations.append(
                self._metric_observation(
                    claim,
                    f"{metric_base}_pct",
                    pct_value,
                    "percent",
                    subgroup,
                )
            )
        return [row for row in observations if row]

    def _metric_observation(
        self,
        claim: dict,
        metric_type: str,
        value: float | int | None,
        unit: str,
        subgroup: str,
    ) -> dict | None:
        if value is None:
            return None
        region = claim["region"]
        subgroup_value = subgroup or ""
        if subgroup == "national":
            region = region if region and region != "Unknown" else "Philippines"
            subgroup_value = ""
        elif region == "Unknown" and subgroup in {
            "pregnant_women",
            "tgw",
            "transactional_sex",
            "migrant_workers",
        }:
            region = "Philippines"
        return self._observation_row(
            claim,
            observation_id=f"{claim['claim_id']}:{metric_type}:{subgroup or 'all'}",
            metric_type=metric_type,
            value=value,
            unit=unit,
            region=region,
            subgroup=subgroup_value,
        )

    def _scoped_count_pct_observations(
        self,
        claim: dict,
        metric_base: str,
        count_value: int | None,
        pct_value: float | None,
        period_scope: str,
        period_label: str,
        *,
        region: str | None = None,
        subgroup: str = "",
    ) -> list[dict]:
        observations = []
        observations.append(
            self._scoped_metric_observation(
                claim,
                f"{metric_base}_count",
                count_value,
                "count",
                period_scope,
                period_label,
                region=region,
                subgroup=subgroup,
            )
        )
        observations.append(
            self._scoped_metric_observation(
                claim,
                f"{metric_base}_pct",
                pct_value,
                "percent",
                period_scope,
                period_label,
                region=region,
                subgroup=subgroup,
            )
        )
        return [row for row in observations if row]

    def _scoped_metric_observation(
        self,
        claim: dict,
        metric_type: str,
        value: float | int | None,
        unit: str,
        period_scope: str,
        period_label: str,
        *,
        region: str | None = None,
        subgroup: str = "",
    ) -> dict | None:
        if value is None:
            return None
        return self._observation_row(
            claim,
            observation_id=f"{claim['claim_id']}:{metric_type}:{period_scope}:{subgroup or 'all'}",
            metric_type=metric_type,
            value=value,
            unit=unit,
            region=region or claim.get("region") or "",
            subgroup=subgroup,
            period_scope=period_scope,
            period_label=period_label,
            period_granularity="month" if period_scope == "month" else period_scope,
            month=claim.get("month") if period_scope == "month" else "",
        )

    def _cumulative_period_label(self, claim: dict, start_year: int = 1984) -> str:
        year = self._parse_int(claim.get("year"))
        quarter = claim.get("quarter") or ""
        month = self._parse_int(claim.get("month"))
        if year and month:
            return f"{start_year}-{year}-{month:02d}"
        if year and quarter:
            return f"{start_year}-{year} {quarter}"
        if year:
            return f"{start_year}-{year}"
        return claim.get("period_label") or ""

    def _augment_policy_tags_from_filename(self, filename: str, document_type: str, tags: dict) -> dict:
        policy_type = tags.get("policy_type") or ""
        policy_reference = tags.get("policy_reference") or ""
        policy_issuer = tags.get("policy_issuer") or ""
        policy_title = tags.get("policy_title") or ""

        stem = Path(filename).stem
        lowered = stem.lower()
        compact = re.sub(r"[^a-z0-9]+", "_", lowered)

        filename_patterns = (
            ("administrative_order", r"(?:^|_)ao(?:_?no)?_?(\d{4})[_-](\d{4})"),
            ("department_circular", r"(?:^|_)dc_?(\d{4})[_-](\d{4})"),
            ("department_memorandum", r"(?:^|_)dm_?(\d{4})[_-](\d{4})"),
            ("republic_act", r"(?:^|_)ra_?(\d{4,5})"),
        )
        if not policy_type or not policy_reference:
            for inferred_type, pattern in filename_patterns:
                match = re.search(pattern, compact, re.IGNORECASE)
                if not match:
                    continue
                policy_type = policy_type or inferred_type
                if inferred_type == "republic_act":
                    policy_reference = policy_reference or match.group(1)
                else:
                    policy_reference = policy_reference or f"{match.group(1)}-{match.group(2)}"
                break

        if document_type == "guideline_policy" and not policy_issuer:
            policy_issuer = "DOH"

        if document_type == "guideline_policy" and not policy_title:
            cleaned = re.sub(r"[_-]+", " ", stem)
            cleaned = re.sub(r"\b(?:ao|dc|dm)\s*\d{4}\s*\d{4}\b", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\bno\b\.?\s*\d{4}\s*\d{4}\b", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_")
            if cleaned:
                policy_title = cleaned

        return {
            "policy_type": policy_type,
            "policy_reference": policy_reference,
            "policy_issuer": policy_issuer,
            "policy_title": policy_title,
        }

    def _extract_policy_tags(self, claim_text: str, snippet: str) -> dict:
        text = " ".join(part.strip() for part in (claim_text, snippet) if part and part.strip())
        text = re.sub(r"\s+", " ", text).strip()
        lowered = text.lower()

        policy_type = ""
        policy_reference = ""
        policy_issuer = ""
        policy_title = ""

        pattern_specs = (
            ("department_memorandum", r"(?:DOH\s+)?Department Memorandum No\.?\s*([0-9]{4}[-–][0-9A-Za-z]+)"),
            ("administrative_order", r"(?:DOH\s+)?Administrative Order No\.?\s*([0-9]{4}[-–][0-9A-Za-z]+)"),
            ("department_circular", r"(?:DOH\s+)?Department Circular No\.?\s*([0-9]{4}[-–][0-9A-Za-z]+)"),
            ("memorandum", r"Memorandum No\.?\s*([0-9]{4}[-–][0-9A-Za-z]+)"),
            ("department_memorandum", r"\bDM\s*([0-9]{4}[-–][0-9A-Za-z]+)\b"),
            ("administrative_order", r"\bAO\s*([0-9]{4}[-–][0-9A-Za-z]+)\b"),
            ("department_circular", r"\bDC\s*([0-9]{4}[-–][0-9A-Za-z]+)\b"),
            ("republic_act", r"\bRA\s*([0-9]{4,5})\b"),
            ("guideline", r"Guidelines?"),
        )
        for candidate_type, pattern in pattern_specs:
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue
            policy_type = candidate_type
            if match.lastindex:
                policy_reference = (match.group(1) or "").replace("–", "-")
            break

        if "department of health" in lowered or re.search(r"\bDOH\b", text):
            policy_issuer = "DOH"

        title_match = re.search(
            r"(?:No\.?\s*[0-9]{4}[-–][0-9A-Za-z]+:\s*)(.{12,220}?)(?=(?:\bRegion Name\b|\bOTHER FACILITIES\b|\bTREATMENT HUBS\b|\bDOH Certified\b|$))",
            text,
            re.IGNORECASE,
        )
        if title_match:
            policy_title = title_match.group(1).strip(" -:;,.")
        elif policy_type == "guideline":
            title_match = re.search(r"((?:national |clinical )?guidelines?.{0,180})", text, re.IGNORECASE)
            if title_match:
                policy_title = title_match.group(1).strip(" -:;,.")

        return {
            "policy_type": policy_type,
            "policy_reference": policy_reference,
            "policy_issuer": policy_issuer,
            "policy_title": policy_title,
        }

    def _has_structured_policy_tags(self, claim: dict) -> bool:
        signal_count = sum(
            1
            for key in ("policy_type", "policy_reference", "policy_issuer", "policy_title")
            if claim.get(key)
        )
        return signal_count >= 2

    def _parse_int(self, value: str | int | float | None) -> int | None:
        if value in (None, ""):
            return None
        lowered = str(value).strip().lower()
        if lowered in self.SMALL_NUMBER_WORDS:
            return self.SMALL_NUMBER_WORDS[lowered]
        cleaned = re.sub(r"[^\d]", "", str(value))
        if not cleaned:
            return None
        try:
            return int(cleaned)
        except ValueError:
            return None

    def _parse_float(self, value: str | int | float | None) -> float | None:
        if value in (None, ""):
            return None
        cleaned = str(value).strip().replace(",", "")
        cleaned = cleaned.replace("%", "")
        match = re.search(r"\d+(?:\.\d+)?", cleaned)
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None

    def _claim_to_review_item(self, claim: dict, observations: list[dict]) -> dict | None:
        if observations or claim["primary_disease"] == "Unclassified":
            return None

        claim_text = claim.get("claim_text") or ""
        document_type = claim.get("document_type") or "other"
        category = claim.get("category") or ""
        metric_type = claim.get("metric_type") or ""

        if category == "Structured Table Row" and self._is_structured_table_header_artifact(claim):
            return None

        review_reason = ""
        proposed_action = ""
        priority = "low"

        if category == "Structured Table Row":
            review_reason = "structured_table_mapping_needed"
            proposed_action = "Map row values to a metric schema before charting."
            priority = "high" if document_type in {"surveillance_report", "situational_report"} else "medium"
        elif (
            category in {"Quantitative Narrative", "Narrative Statistic"}
            and document_type in {"surveillance_report", "situational_report"}
            and self._is_reviewable_surveillance_claim(claim)
        ):
            review_reason = "surveillance_narrative_needs_structuring"
            proposed_action = "Extract explicit metric, value, period, region, and subgroup from narrative text."
            priority = "high" if metric_type in {"new_cases", "cases", "deaths", "testing", "treatment"} else "medium"
        elif self._is_policy_claim(claim):
            if self._has_structured_policy_tags(claim):
                return None
            review_reason = "policy_claim_needs_tagging"
            proposed_action = "Tag issuer, policy type, effective date, and scope before downstream use."
            priority = "medium"
        else:
            return None

        notes = []
        if claim.get("region") == "Unknown" and document_type in {"surveillance_report", "situational_report"}:
            notes.append("region missing")
        if not claim.get("year"):
            notes.append("period missing")
        if self._looks_like_artifact_claim(claim_text):
            notes.append("possible artifact")

        return {
            "review_id": f"claim:{claim['claim_id']}",
            "claim_id": claim["claim_id"],
            "document_id": claim["document_id"],
            "filename": claim["filename"],
            "document_type": document_type,
            "category": category,
            "metric_type": metric_type,
            "primary_disease": claim["primary_disease"],
            "region": claim.get("region") or "",
            "year": claim.get("year") or "",
            "quarter": claim.get("quarter") or "",
            "period_label": claim.get("period_label") or "",
            "confidence": claim["confidence"],
            "priority": priority,
            "review_reason": review_reason,
            "proposed_action": proposed_action,
            "notes": "; ".join(notes),
            "source_url": claim.get("source_url") or "",
            "page_index": claim["page_index"],
            "claim_text": claim_text,
            "snippet": claim.get("snippet") or "",
        }

    def _care_cascade_metric_schema(self, values: list[float]) -> list[tuple[str, str]]:
        if len(values) >= 10:
            return self.CARE_CASCADE_METRICS
        return self.CARE_CASCADE_METRICS_COMPACT

    def _series_observations(
        self,
        claim: dict,
        metric_names: list[tuple[str, str]],
        values: list[float],
        region: str | None = None,
        subgroup: str | None = None,
    ) -> list[dict]:
        observations = []
        for (metric_name, unit), value in zip(metric_names, values):
            observations.append(
                self._observation_row(
                    claim,
                    observation_id=f"{claim['claim_id']}:{metric_name}",
                    metric_type=metric_name,
                    value=value,
                    unit=unit,
                    region=region or claim["region"],
                    subgroup=subgroup or "",
                )
            )
        return observations

    def _observation_row(
        self,
        claim: dict,
        *,
        observation_id: str,
        metric_type: str,
        value: float,
        unit: str,
        region: str | None = None,
        subgroup: str = "",
        period_scope: str | None = None,
        period_label: str | None = None,
        period_granularity: str | None = None,
        month: int | str | None = None,
    ) -> dict:
        obs_month = month if month not in (None, "") else claim.get("month", "")
        return {
            "observation_id": observation_id,
            "claim_id": claim["claim_id"],
            "document_id": claim["document_id"],
            "folder": claim["folder"],
            "filename": claim["filename"],
            "document_type": claim.get("document_type") or "",
            "primary_disease": claim["primary_disease"],
            "metric_type": metric_type,
            "region": region or claim["region"],
            "subgroup": subgroup or "",
            "year": claim["year"],
            "quarter": claim["quarter"],
            "month": obs_month,
            "period_granularity": period_granularity or claim.get("period_granularity") or "",
            "period_scope": period_scope or claim.get("period_scope") or "snapshot",
            "period_label": period_label or claim["period_label"],
            "value": value,
            "unit": unit,
            "confidence": claim["confidence"],
            "source_url": claim["source_url"],
            "page_index": claim["page_index"],
            "snippet": claim["snippet"],
        }

    def _infer_metric_type(self, category: str, combined_text: str) -> str:
        if category.startswith("Care Cascade"):
            return "care_cascade"
        if category == "Treatment Outcome":
            return "treatment_outcome"
        if category == "Structured Table Row" and "Percent increase between December 2020" in combined_text:
            return "age_group_growth"

        for metric_name, pattern in self.METRIC_PATTERNS.items():
            if pattern.search(combined_text) or pattern.search(category):
                return metric_name
        return "general"

    def _infer_region(self, row_label: str, combined_text: str) -> str:
        if row_label and self._is_region_label(row_label):
            return row_label

        for region_name, pattern in self.LOCATION_PATTERNS.items():
            if pattern.search(combined_text):
                return region_name
        return "Unknown"

    def _extract_row_label(self, claim_text: str) -> str:
        match = re.search(r":\s*(.+?)\s+has values\s", claim_text)
        return match.group(1).strip() if match else ""

    def _is_region_label(self, label: str) -> bool:
        return bool(self.REGION_LABEL_PATTERN.fullmatch(label.strip()))

    def _split_raw_value_tokens(self, raw_value: str) -> list[str]:
        return [token.strip() for token in str(raw_value).split("|") if token.strip()]

    def _structured_row_tokens(self, raw_value: str) -> list[str]:
        tokens: list[str] = []
        for segment in self._split_raw_value_tokens(raw_value):
            normalized_region = self._coerce_region_label(segment)
            if normalized_region:
                tokens.append(normalized_region)
                continue
            tokens.extend(re.findall(r"[<>]?\d[\d,]*%?", segment))
        return tokens

    def _structured_region_label_with_count(self, row_label: str) -> tuple[str, int | None]:
        cleaned = re.sub(r"\s+", " ", (row_label or "")).strip()
        match = re.fullmatch(
            r"(?P<region>CARAGA|BARMM|NCR|CAR|NIR|[0-9]{1,2}[A-Z]?)\s+(?P<count>\d[\d,]*)",
            cleaned,
            re.IGNORECASE,
        )
        if match:
            return self._coerce_region_label(match.group("region")), self._parse_int(
                match.group("count")
            )

        region = self._coerce_region_label(cleaned)
        if region:
            return region, None
        return "", None

    def _coerce_region_label(self, token: str) -> str:
        normalized = self._normalize_region_token(token)
        if normalized:
            return normalized
        cleaned = re.sub(r"\s+", " ", (token or "")).strip().upper()
        if cleaned in {"NCR", "NIR", "CAR", "CARAGA", "BARMM", "ARMM"}:
            return cleaned
        match = re.fullmatch(r"(?P<base>\d{1,2})(?P<suffix>[A-Z]?)", cleaned)
        if not match:
            return ""
        base = int(match.group("base"))
        suffix = match.group("suffix")
        if 1 <= base <= 12 and suffix in {"", "A", "B"}:
            return f"{base}{suffix}"
        return ""

    def _region_before_value_sequence(self, snippet: str, first_value_token: str) -> str:
        if not snippet or not first_value_token:
            return ""
        pattern = re.compile(
            rf"(?P<region>CARAGA|BARMM|NCR|CAR|NIR|[0-9]{{1,2}}[A-Z]?)\s*\|\s*{re.escape(first_value_token)}\b",
            re.IGNORECASE,
        )
        match = pattern.search(snippet)
        if not match:
            return ""
        return self._coerce_region_label(match.group("region"))

    def _is_percent_token(self, token: str) -> bool:
        return bool(re.fullmatch(r"[<>]?\d[\d,]*(?:\.\d+)?%", str(token).strip()))

    def _is_count_token(self, token: str) -> bool:
        return self._parse_clean_numeric_token(token) is not None and not self._is_percent_token(token)

    def _looks_like_count_pct_pairs(self, tokens: list[str]) -> bool:
        if len(tokens) != 6:
            return False
        return all(
            self._is_count_token(tokens[index]) and self._is_percent_token(tokens[index + 1])
            for index in range(0, 6, 2)
        )

    def _structured_row_region(self, claim: dict) -> str:
        row_label = (claim.get("row_label") or "").strip()
        region = self._normalize_region_token(row_label)
        if region:
            return region
        if self._is_region_label(row_label):
            return row_label
        claim_region = (claim.get("region") or "").strip()
        if claim_region and claim_region != "Unknown":
            normalized = self._normalize_region_token(claim_region)
            return normalized or claim_region
        return ""

    def _parse_value_cells(self, raw_value: str) -> list[float]:
        values = []
        for token in str(raw_value).split("|"):
            token = token.strip()
            numeric = self._try_parse_number(token)
            if numeric is not None:
                values.append(numeric)
        return values

    def _parse_clean_value_tokens(self, raw_value: str) -> list[float]:
        values = []
        for token in str(raw_value).split("|"):
            token = token.strip()
            if not token:
                continue
            numeric = self._parse_clean_numeric_token(token)
            if numeric is None:
                return []
            values.append(numeric)
        return values

    def _dedupe_observations(self, rows: list[dict]) -> list[dict]:
        deduped: OrderedDict[tuple, dict] = OrderedDict()
        for row in rows:
            key = (
                row.get("document_id"),
                row.get("metric_type"),
                row.get("region") or "",
                row.get("subgroup") or "",
                row.get("period_label") or "",
                row.get("period_scope") or "",
                row.get("value"),
                row.get("unit") or "",
            )
            deduped.setdefault(key, row)
        return list(deduped.values())

    def _build_graph_rows(self, claims: list[dict]) -> tuple[list[dict], list[dict]]:
        entities: OrderedDict[str, dict] = OrderedDict()
        relations: list[dict] = []

        for claim in claims:
            doc_key = f"document:{claim['document_id']}"
            disease_key = f"disease:{claim['primary_disease']}"
            metric_key = f"metric:{claim['metric_type']}"

            entities.setdefault(
                doc_key,
                {
                    "entity_id": doc_key,
                    "entity_type": "document",
                    "entity_key": str(claim["document_id"]),
                    "display_name": claim["filename"],
                },
            )
            entities.setdefault(
                disease_key,
                {
                    "entity_id": disease_key,
                    "entity_type": "disease",
                    "entity_key": claim["primary_disease"],
                    "display_name": claim["primary_disease"],
                },
            )
            entities.setdefault(
                metric_key,
                {
                    "entity_id": metric_key,
                    "entity_type": "metric",
                    "entity_key": claim["metric_type"],
                    "display_name": claim["metric_type"],
                },
            )

            relations.append(
                {
                    "relation_type": "document_mentions_disease",
                    "source_entity": doc_key,
                    "target_entity": disease_key,
                    "claim_id": claim["claim_id"],
                    "confidence": claim["confidence"],
                }
            )
            relations.append(
                {
                    "relation_type": "claim_uses_metric",
                    "source_entity": disease_key,
                    "target_entity": metric_key,
                    "claim_id": claim["claim_id"],
                    "confidence": claim["confidence"],
                }
            )

        return list(entities.values()), relations

    def _extract_year(self, *texts: str) -> int | None:
        for text in texts:
            if not text:
                continue
            match = self.YEAR_PATTERN.search(text)
            if match:
                return int(match.group(1))
        return None

    def _extract_quarter(self, *texts: str) -> str | None:
        for text in texts:
            if not text:
                continue
            match = self.QUARTER_PATTERN.search(text)
            if match:
                return f"Q{match.group(1)}"
            inferred = self._extract_quarter_from_month_range(text)
            if inferred:
                return inferred
        return None

    def _extract_month(self, *texts: str) -> int | None:
        for text in texts:
            if not text:
                continue
            normalized = str(text).replace("_", " ").replace("-", " ").lower()
            tokens = re.findall(r"[a-z]+", normalized)
            months = [self.MONTH_NAME_TO_NUM[token] for token in tokens if token in self.MONTH_NAME_TO_NUM]
            if len(months) == 1:
                return months[0]
        return None

    def _extract_quarter_from_month_range(self, text: str) -> str | None:
        normalized = str(text).replace("_", " ").replace("-", " ").lower()
        tokens = [self.MONTH_NAME_TO_NUM[token] for token in re.findall(r"[a-z]+", normalized) if token in self.MONTH_NAME_TO_NUM]
        unique = []
        for month in tokens:
            if month not in unique:
                unique.append(month)
        if len(unique) < 2:
            return None
        month_set = set(unique)
        if month_set == {1, 2, 3}:
            return "Q1"
        if month_set == {4, 5, 6}:
            return "Q2"
        if month_set == {7, 8, 9}:
            return "Q3"
        if month_set == {10, 11, 12}:
            return "Q4"
        if len(unique) == 2:
            start_end = (unique[0], unique[-1])
            if start_end == (1, 3):
                return "Q1"
            if start_end == (4, 6):
                return "Q2"
            if start_end == (7, 9):
                return "Q3"
            if start_end == (10, 12):
                return "Q4"
        return None

    def _infer_period_granularity(
        self,
        filename: str,
        year: int | None,
        quarter: str | None,
        month: int | None,
    ) -> str:
        if quarter:
            return "quarter"
        if month:
            return "month"
        if year:
            return "year"
        return "unknown"

    def _build_period_label(
        self,
        year: int | None,
        quarter: str | None,
        month: int | None,
        period_granularity: str | None,
    ) -> str:
        if year and period_granularity == "month" and month:
            return self._format_year_month(year, month)
        if year and quarter:
            return f"{year} {quarter}"
        if year:
            return str(year)
        if quarter:
            return quarter
        return ""

    def _format_year_month(self, year: int, month: int) -> str:
        return f"{year}-{int(month):02d}"

    def _extract_numeric_value(self, value_field: str | None, text: str, category: str = "") -> tuple[float | None, str]:
        if category in self.SCALAR_UNSAFE_CATEGORIES:
            return None, ""
        if value_field:
            numeric = self._try_parse_number(value_field)
            if numeric is not None and not self._looks_like_reference_number(text, numeric):
                return numeric, self._infer_unit(value_field)

        match = self.NUMBER_PATTERN.search(text)
        if not match:
            return None, ""

        numeric = self._try_parse_number(match.group("value"))
        if numeric is not None and self._looks_like_reference_number(text, numeric):
            return None, ""
        unit = self._infer_unit(match.group("unit") or text)
        return numeric, unit

    def _try_parse_number(self, raw_value: str) -> float | None:
        match = self.NUMBER_PATTERN.search(str(raw_value))
        if not match:
            return None
        try:
            return float(match.group("value").replace(",", ""))
        except ValueError:
            return None

    def _parse_clean_numeric_token(self, raw_value: str) -> float | None:
        cleaned = str(raw_value).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        cleaned = re.sub(r"([0-9])([A-Za-z])$", r"\1", cleaned)
        if not re.fullmatch(r"[<>]?\d[\d,]*(?:\.\d+)?%?", cleaned):
            return None
        return self._try_parse_number(cleaned)

    def _infer_unit(self, text: str) -> str:
        lowered = str(text).lower()
        if "%" in lowered or "percent" in lowered:
            return "percent"
        if "death" in lowered:
            return "deaths"
        if "test" in lowered:
            return "tests"
        return "count"

    def _classify_document_type(self, local_path: Path, category: str, claim_text: str) -> str:
        filename = local_path.name.lower()
        combined = f"{filename}\n{category}\n{claim_text}".lower()

        if category.startswith("Care Cascade"):
            return "care_cascade_report"
        if any(hint in combined for hint in self.POLICY_FILENAME_HINTS):
            return "guideline_policy"
        if any(hint in combined for hint in self.SITUATIONAL_FILENAME_HINTS):
            return "situational_report"
        if any(hint in combined for hint in self.SURVEILLANCE_FILENAME_HINTS):
            return "surveillance_report"
        return "other"

    def _is_reviewable_surveillance_claim(self, claim: dict) -> bool:
        claim_text = claim.get("claim_text") or ""
        if self._looks_like_artifact_claim(claim_text):
            return False
        if self._is_non_chart_surveillance_context(claim):
            return False

        numeric_tokens = re.findall(r"(?:<)?\d[\d,]*(?:\.\d+)?%?", claim_text)
        if len(numeric_tokens) < 2:
            return False

        metric_type = claim.get("metric_type") or ""
        if metric_type == "general":
            lowered = claim_text.lower()
            if not any(marker in lowered for marker in ("cases", "diagnosed", "deaths", "tests", "art", "viral load", "%")):
                return False

        return True

    def _is_non_chart_surveillance_context(self, claim: dict) -> bool:
        text = self._claim_evidence_text(claim).lower()
        patterns = (
            "annual number of deaths from 1984-2010 ranges from",
            "annual number of deaths from 1984 to 2010 ranges from",
            "from 1984 to 2006, the predominant mot was male-female sex",
            "from 2007, the trend shifted to sexual contact among msm as the predominant mot",
            "the increase started in 2010 and transmission through sharing of needles",
            "integrated these high-level targets in the 7th aids medium term plan",
            "classification of diagnosed cases with advanced clinical manifestations based on immunologic and clinical criteria has been newly implemented",
            "no data on sex and mot for",
        )
        return any(pattern in text for pattern in patterns)

    def _is_policy_claim(self, claim: dict) -> bool:
        if claim.get("metric_type") == "policy":
            return True
        lowered = (claim.get("claim_text") or "").lower()
        return any(marker in lowered for marker in ("guideline", "policy", "administrative order", "memorandum", "shall ", "must "))

    def _looks_like_artifact_claim(self, text: str) -> bool:
        stripped = (text or "").strip()
        if not stripped:
            return True
        return any(pattern.search(stripped) for pattern in self.ARTIFACT_PATTERNS)

    def _is_structured_table_header_artifact(self, claim: dict) -> bool:
        claim_text = (claim.get("claim_text") or "").strip().lower()
        row_label = (claim.get("row_label") or "").strip().lower()
        raw_value = (claim.get("raw_value") or "").strip().lower()
        if claim_text.startswith("region number of %: region has values"):
            return True
        if row_label == "region" and raw_value.startswith("2024 | june 2024"):
            return True
        return False

    def _looks_like_reference_number(self, text: str, numeric: float) -> bool:
        lowered = (text or "").lower()
        if float(int(numeric)) != float(numeric):
            return False

        integer = int(numeric)
        if 1900 <= integer <= 2030 and any(marker in lowered for marker in ("january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december", "year", "quarter")):
            return True
        if any(marker in lowered for marker in (f"ra {integer}", f"ao {integer}", f"dm {integer}", f"dc {integer}", f"administrative order no. {integer}")):
            return True
        return False

    def _write_csv(self, path: Path, rows: list[dict]):
        if not rows:
            path.write_text("", encoding="utf-8")
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    def _write_jsonl(self, path: Path, rows: list[dict]):
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _write_json(self, path: Path, payload: dict):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
