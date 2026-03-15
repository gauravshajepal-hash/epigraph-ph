import csv
import json
import logging
import re
from collections import Counter, defaultdict
from pathlib import Path

import yaml

try:
    import numpy as np
    from scipy.stats import spearmanr
    from sklearn.linear_model import TheilSenRegressor
except ImportError:  # pragma: no cover - optional analytics stack
    np = None
    spearmanr = None
    TheilSenRegressor = None

logger = logging.getLogger(__name__)


class InsightsManager:
    """Builds lightweight trend and insight feeds from normalized exports."""

    GOAL_SERIES = (
        {
            "series_id": "first_95",
            "label": "1st 95: Diagnosis coverage",
            "metric_candidates": ("diagnosed_plhiv_pct",),
            "count_metric_candidates": ("diagnosed_plhiv_count",),
            "target_value": 95.0,
            "target_unit": "percent",
            "deadline_year": 2025,
            "note": "",
        },
        {
            "series_id": "second_95",
            "label": "2nd 95: Treatment coverage",
            "metric_candidates": ("plhiv_on_art_pct",),
            "count_metric_candidates": ("plhiv_on_art_count",),
            "target_value": 95.0,
            "target_unit": "percent",
            "deadline_year": 2025,
            "note": "",
        },
        {
            "series_id": "third_95",
            "label": "3rd 95: Viral suppression",
            "metric_candidates": ("suppression_among_on_art_pct", "viral_load_suppressed_pct"),
            "count_metric_candidates": ("viral_load_suppressed_count",),
            "target_value": 95.0,
            "target_unit": "percent",
            "deadline_year": 2025,
            "note": "Using suppression among tested when suppression among PLHIV on ART is not yet structured.",
        },
    )
    VALID_REGION_CODES = {
        "NCR", "CAR", "CARAGA", "BARMM", "ARMM", "NIR",
        "1", "2", "3", "4A", "4B", "5", "6", "7", "8", "9", "10", "11", "12",
    }
    CURATED_BURDEN_SPECS = (
        {
            "panel_id": "cumulative_reported_cases",
            "label": "Cumulative reported HIV cases",
            "metric_candidates": ("reported_cases_count",),
            "unit": "count",
            "region": "Philippines",
            "subgroup": "",
            "preferred_scopes": ("cumulative",),
            "preferred_granularities": ("month", "quarter", "year"),
            "min_points": 5,
            "note": "Historical cumulative burden extracted from epidemic trend sections and surveillance bulletins.",
        },
        {
            "panel_id": "plhiv_on_art",
            "label": "PLHIV on ART",
            "metric_candidates": ("plhiv_on_art_count", "on_art"),
            "unit": "count",
            "region": "Philippines",
            "subgroup": "",
            "preferred_scopes": ("snapshot",),
            "preferred_granularities": ("quarter", "month", "year"),
            "min_points": 4,
            "note": "Treatment program response from monthly and quarterly HIV/STI reports.",
        },
        {
            "panel_id": "reported_deaths",
            "label": "Reported deaths",
            "metric_candidates": ("reported_deaths_count",),
            "unit": "count",
            "region": "Philippines",
            "subgroup": "",
            "preferred_scopes": ("snapshot", "cumulative"),
            "preferred_granularities": ("month", "quarter", "year"),
            "min_points": 4,
            "note": "Deaths among reported HIV cases extracted from surveillance bulletins.",
        },
        {
            "panel_id": "pregnant_women_reported",
            "label": "Pregnant women reported at diagnosis",
            "metric_candidates": ("pregnant_women_reported_count",),
            "unit": "count",
            "region": "Philippines",
            "subgroup": "",
            "preferred_scopes": ("snapshot", "cumulative"),
            "preferred_granularities": ("quarter", "month", "year"),
            "min_points": 3,
            "note": "Reported pregnant women with HIV across monthly and quarterly surveillance outputs.",
        },
    )
    CURATED_TRANSMISSION_SPECS = (
        {
            "panel_id": "sexual_contact_share",
            "label": "Sexual contact share of reported cases",
            "metric_candidates": ("sexual_contact_cases_pct",),
            "unit": "percent",
            "region": "Philippines",
            "subgroup": "",
            "preferred_scopes": ("cumulative", "snapshot"),
            "preferred_granularities": ("month", "quarter", "year"),
            "min_points": 5,
            "note": "Share of diagnosed cases attributed to sexual contact in historical and recent reports.",
        },
        {
            "panel_id": "mother_to_child_transmission",
            "label": "Mother-to-child transmission",
            "metric_candidates": ("mother_to_child_transmission_count",),
            "unit": "count",
            "region": "Philippines",
            "subgroup": "",
            "preferred_scopes": ("cumulative", "snapshot"),
            "preferred_granularities": ("month", "quarter", "year"),
            "min_points": 4,
            "note": "Historical and recent counts of mother-to-child transmission extracted from surveillance text.",
        },
        {
            "panel_id": "needle_transmission",
            "label": "Needle transmission",
            "metric_candidates": ("needle_transmission_count",),
            "unit": "count",
            "region": "Philippines",
            "subgroup": "",
            "preferred_scopes": ("snapshot", "cumulative"),
            "preferred_granularities": ("month", "quarter", "year"),
            "min_points": 3,
            "note": "Transmission through infected needle sharing where the bulletin reports explicit counts.",
        },
        {
            "panel_id": "msm_share",
            "label": "MSM share among cumulative diagnoses",
            "metric_candidates": ("reported_cases_pct",),
            "unit": "percent",
            "region": "Philippines",
            "subgroup": "msm",
            "preferred_scopes": ("cumulative",),
            "preferred_granularities": ("month", "quarter", "year"),
            "min_points": 3,
            "note": "Cumulative diagnosed-case share among MSM from epidemic trend sections.",
        },
    )

    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        paths_cfg = cfg.get("paths", {})
        self.normalized_dir = Path(paths_cfg.get("normalized", "data/normalized"))
        self.normalized_dir.mkdir(parents=True, exist_ok=True)

    def build_exports(self) -> dict:
        claims = self._read_csv(self.normalized_dir / "claims.csv")
        observations = self._read_csv(self.normalized_dir / "observations.csv")
        review_queue = self._read_csv(self.normalized_dir / "review_queue.csv")
        review_queue_enriched = self._read_csv(self.normalized_dir / "review_queue_enriched.csv")
        all_trends = self._trend_candidates(observations, limit=None)
        national_cascade = self._national_cascade_series(all_trends)
        goal_forecasts = self._goal_forecasts(all_trends)
        national_goal_board = self._national_goal_board(national_cascade, goal_forecasts)
        burden_views = self._curated_series_views(all_trends, self.CURATED_BURDEN_SPECS)
        transmission_views = self._curated_series_views(all_trends, self.CURATED_TRANSMISSION_SPECS)
        epi_analytics = self._epi_analytics(observations, national_cascade)
        review_summary = self._review_queue_summary(review_queue, review_queue_enriched)
        overview_highlights = self._overview_highlights(
            national_goal_board,
            burden_views,
            transmission_views,
            epi_analytics,
            review_summary,
        )

        insights = {
            "overview": {
                "claims_count": len(claims),
                "observations_count": len(observations),
                "review_queue_count": len(review_queue),
                "disease_counts": Counter(row["primary_disease"] for row in claims),
            },
            "yearly_activity": self._yearly_activity(claims),
            "trend_candidates": all_trends[:20],
            "national_cascade": national_cascade,
            "goal_forecasts": goal_forecasts,
            "national_goal_board": national_goal_board,
            "burden_views": burden_views,
            "transmission_views": transmission_views,
            "epi_analytics": epi_analytics,
            "surprising_insights": overview_highlights,
            "documents_with_most_claims": self._top_documents(claims),
            "review_queue": review_summary,
        }

        dashboard_feed = {
            "kpis": {
                "claims": len(claims),
                "observations": len(observations),
                "review_queue": len(review_queue),
                "tracked_diseases": len({row["primary_disease"] for row in claims if row["primary_disease"]}),
            },
            "charts": {
                "yearly_activity": insights["yearly_activity"],
                "trend_candidates": insights["trend_candidates"],
                "national_cascade": insights["national_cascade"],
                "goal_forecasts": insights["goal_forecasts"],
                "national_goal_board": national_goal_board,
                "burden_views": burden_views,
                "transmission_views": transmission_views,
                "regional_gaps": epi_analytics["regional_gaps"],
                "regional_scorecard": epi_analytics["regional_scorecard"],
                "relationship_scatter": epi_analytics["relationship_scatter"],
                "structuring_frontier": review_summary,
            },
            "highlights": overview_highlights,
            "analytics": {
                "relationship_highlights": epi_analytics["relationship_highlights"],
            },
        }

        self._write_json(self.normalized_dir / "insights.json", insights)
        self._write_json(self.normalized_dir / "dashboard_feed.json", dashboard_feed)
        logger.info(
            "Insight exports written: %s claims, %s observations.",
            len(claims),
            len(observations),
        )
        return dashboard_feed

    def _yearly_activity(self, claims: list[dict]) -> list[dict]:
        counts = Counter()
        for row in claims:
            year = row.get("year")
            if year:
                counts[str(year)] += 1
        return [
            {"year": year, "claim_count": count}
            for year, count in sorted(counts.items())
        ]

    def _trend_candidates(self, observations: list[dict], limit: int | None = 20) -> list[dict]:
        grouped: dict[tuple[str, str, str, str, str, str, str], list[dict]] = defaultdict(list)
        for row in observations:
            period = row.get("period_label") or row.get("year")
            if not period:
                continue
            key = (
                row["primary_disease"],
                row["metric_type"],
                row["unit"],
                row.get("region") or "Unknown",
                row.get("subgroup") or "",
                row.get("period_granularity") or "unknown",
                row.get("period_scope") or "snapshot",
            )
            grouped[key].append(row)

        candidates = []
        for (disease, metric, unit, region, subgroup, granularity, scope), rows in grouped.items():
            if len(rows) < 2:
                continue
            ordered = sorted(rows, key=self._observation_sort_key)
            distinct_periods = {
                row.get("period_label") or row.get("year")
                for row in ordered
                if row.get("period_label") or row.get("year")
            }
            if len(distinct_periods) < 2:
                continue
            series = self._collapse_period_points(ordered)
            if len(series) < 2:
                continue
            candidates.append(
                {
                    "primary_disease": disease,
                    "metric_type": metric,
                    "unit": unit,
                    "region": region,
                    "subgroup": subgroup,
                    "period_granularity": granularity,
                    "period_scope": scope,
                    "points": series,
                }
            )
        candidates.sort(key=self._trend_sort_key)
        if limit is None:
            return candidates
        return candidates[:limit]

    def _trend_sort_key(self, trend: dict) -> tuple[int, int, int, str, str]:
        points = trend.get("points") or []
        latest_period = points[-1]["period"] if points else ""
        return (
            0 if trend.get("region") == "Philippines" else 1,
            0 if trend.get("unit") == "percent" else 1,
            -len(points),
            trend.get("metric_type") or "",
            latest_period,
        )

    def _collapse_period_points(self, rows: list[dict]) -> list[dict]:
        collapsed: dict[str, dict] = {}
        for row in rows:
            if row.get("value") in ("", None):
                continue
            period = row.get("period_label") or row.get("year")
            if not period:
                continue
            bucket = collapsed.setdefault(
                period,
                {
                    "period": period,
                    "region": row.get("region") or "Unknown",
                    "subgroup": row.get("subgroup") or "",
                    "values": [],
                },
            )
            bucket["values"].append(float(row["value"]))

        series = []
        for period, payload in collapsed.items():
            values = payload.pop("values")
            payload["value"] = round(sum(values) / len(values), 4)
            if len(values) > 1:
                payload["sample_count"] = len(values)
            series.append(payload)

        return sorted(series, key=lambda item: self._period_label_sort_key(item["period"]))

    def _observation_sort_key(self, row: dict) -> tuple[int, int, int, str]:
        year = self._safe_int(row.get("year"))
        month = self._safe_int(row.get("month"))
        quarter = self._quarter_to_number(row.get("quarter"))
        granularity = row.get("period_granularity") or ""
        if granularity == "month":
            return year, month, 0, row.get("period_label") or ""
        if granularity == "quarter":
            return year, quarter, 0, row.get("period_label") or ""
        return year, 0, 0, row.get("period_label") or ""

    def _period_label_sort_key(self, period_label: str) -> tuple[int, int, int, str]:
        text = str(period_label)
        match = re.match(r"^(\d{4})-(\d{2})$", text)
        if match:
            return int(match.group(1)), int(match.group(2)), 0, text
        match = re.match(r"^(\d{4})\s+Q([1-4])$", text, re.IGNORECASE)
        if match:
            return int(match.group(1)), int(match.group(2)), 0, text
        match = re.match(r"^(\d{4})$", text)
        if match:
            return int(match.group(1)), 0, 0, text
        return 0, 0, 0, text

    def _quarter_to_number(self, quarter: str | None) -> int:
        if not quarter:
            return 0
        try:
            return int(str(quarter).strip().upper().replace("Q", ""))
        except ValueError:
            return 0

    def _safe_int(self, value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _surprising_insights(self, claims: list[dict], observations: list[dict]) -> list[dict]:
        insights = []

        if claims:
            disease_counts = Counter(row["primary_disease"] for row in claims if row["primary_disease"])
            disease, count = disease_counts.most_common(1)[0]
            insights.append(
                {
                    "title": "Most represented disease",
                    "detail": f"{disease} appears in {count} normalized claims.",
                }
            )

        numeric_observations = []
        for row in observations:
            try:
                numeric_observations.append((float(row["value"]), row))
            except (TypeError, ValueError):
                continue

        if numeric_observations:
            highest_value, highest_row = max(numeric_observations, key=lambda item: item[0])
            insights.append(
                {
                    "title": "Largest extracted numeric observation",
                    "detail": (
                        f"{highest_row['primary_disease']} {highest_row['metric_type']} reached "
                        f"{highest_value:g} {highest_row['unit']} in {highest_row.get('period_label') or 'an undated record'}."
                    ),
                }
            )

        yearly_counts = self._yearly_activity(claims)
        if len(yearly_counts) >= 2:
            ordered = sorted(yearly_counts, key=lambda row: row["year"])
            latest = ordered[-1]
            previous = ordered[-2]
            delta = latest["claim_count"] - previous["claim_count"]
            if delta != 0:
                direction = "more" if delta > 0 else "fewer"
                insights.append(
                    {
                        "title": "Year-over-year extraction shift",
                        "detail": (
                            f"{latest['year']} has {abs(delta)} {direction} normalized claims than {previous['year']}."
                        ),
                    }
                )

        return insights

    def _goal_forecasts(self, trends: list[dict]) -> list[dict]:
        forecasts = []
        for spec in self.GOAL_SERIES:
            trend = self._select_goal_trend(trends, spec)
            if not trend:
                continue

            model = self._forecast_trend_to_target(
                trend.get("points") or [],
                trend.get("period_granularity") or "unknown",
                float(spec["target_value"]),
                int(spec["deadline_year"]),
            )
            if not model:
                continue

            actual_metric = trend.get("metric_type") or ""
            is_proxy = actual_metric != spec["metric_candidates"][0]
            forecasts.append(
                {
                    "series_id": spec["series_id"],
                    "label": spec["label"],
                    "target_value": spec["target_value"],
                    "target_unit": spec["target_unit"],
                    "deadline_year": spec["deadline_year"],
                    "actual_metric_type": actual_metric,
                    "proxy": is_proxy,
                    "note": spec["note"] if is_proxy else "",
                    "primary_disease": trend.get("primary_disease") or "",
                    "region": trend.get("region") or "",
                    "subgroup": trend.get("subgroup") or "",
                    "period_granularity": trend.get("period_granularity") or "",
                    "period_scope": trend.get("period_scope") or "",
                    "points": trend.get("points") or [],
                    **model,
                }
            )
        return forecasts

    def _national_cascade_series(self, trends: list[dict]) -> list[dict]:
        series_rows = []
        for spec in self.GOAL_SERIES:
            pct_trend = self._select_goal_trend(trends, spec)
            count_trend = self._select_series_by_metric(
                trends,
                spec.get("count_metric_candidates", ()),
                unit="count",
            )

            if not pct_trend and not count_trend:
                continue

            effective_trend = pct_trend or count_trend
            percent_points = pct_trend.get("points") if pct_trend else []
            count_points = count_trend.get("points") if count_trend else []
            coverage_points = percent_points or count_points
            coverage_start = coverage_points[0]["period"] if coverage_points else ""
            coverage_end = coverage_points[-1]["period"] if coverage_points else ""
            actual_metric = pct_trend.get("metric_type") if pct_trend else ""
            proxy = bool(pct_trend and actual_metric != spec["metric_candidates"][0])

            series_rows.append(
                {
                    "series_id": spec["series_id"],
                    "label": spec["label"],
                    "target_value": spec["target_value"],
                    "target_unit": spec["target_unit"],
                    "deadline_year": spec["deadline_year"],
                    "actual_metric_type": actual_metric,
                    "proxy": proxy,
                    "note": spec["note"] if proxy else "",
                    "primary_disease": effective_trend.get("primary_disease") or "",
                    "region": effective_trend.get("region") or "",
                    "subgroup": effective_trend.get("subgroup") or "",
                    "period_granularity": effective_trend.get("period_granularity") or "",
                    "period_scope": effective_trend.get("period_scope") or "",
                    "coverage_start": coverage_start,
                    "coverage_end": coverage_end,
                    "coverage_point_count": len(coverage_points),
                    "percent_points": percent_points,
                    "count_points": count_points,
                }
            )
        return series_rows

    def _national_goal_board(self, national_cascade: list[dict], goal_forecasts: list[dict]) -> list[dict]:
        forecast_map = {row.get("series_id"): row for row in goal_forecasts}
        board = []
        for row in national_cascade:
            merged = dict(row)
            forecast = forecast_map.get(row.get("series_id"), {})
            merged.update(
                {
                    "latest_period": forecast.get("latest_period") or (row.get("percent_points") or row.get("count_points") or [{}])[-1].get("period", ""),
                    "latest_value": forecast.get("latest_value", ""),
                    "gap_to_target": forecast.get("gap_to_target", ""),
                    "trajectory": forecast.get("trajectory", ""),
                    "deadline_status": forecast.get("deadline_status", ""),
                    "projected_target_period": forecast.get("projected_target_period", ""),
                    "years_to_target": forecast.get("years_to_target", ""),
                    "r_squared": forecast.get("r_squared", ""),
                    "fit_quality": forecast.get("fit_quality", ""),
                    "model_type": forecast.get("model_type", ""),
                }
            )
            board.append(merged)
        return board

    def _curated_series_views(self, trends: list[dict], specs: tuple[dict, ...]) -> list[dict]:
        panels = []
        for spec in specs:
            trend = self._select_curated_series(trends, spec)
            if not trend:
                continue
            points = trend.get("points") or []
            latest = points[-1] if points else {}
            previous = points[-2] if len(points) >= 2 else {}
            latest_value = float(latest.get("value") or 0) if latest else 0.0
            previous_value = float(previous.get("value") or 0) if previous else None
            panels.append(
                {
                    "panel_id": spec["panel_id"],
                    "label": spec["label"],
                    "note": spec.get("note") or "",
                    "metric_type": trend.get("metric_type") or "",
                    "primary_disease": trend.get("primary_disease") or "",
                    "region": trend.get("region") or "",
                    "subgroup": trend.get("subgroup") or "",
                    "unit": trend.get("unit") or "",
                    "period_granularity": trend.get("period_granularity") or "",
                    "period_scope": trend.get("period_scope") or "",
                    "coverage_start": points[0]["period"] if points else "",
                    "coverage_end": latest.get("period") or "",
                    "point_count": len(points),
                    "latest_value": round(latest_value, 2),
                    "latest_period": latest.get("period") or "",
                    "delta": round(latest_value - previous_value, 2) if previous_value is not None else "",
                    "points": points,
                }
            )
        return panels

    def _select_curated_series(self, trends: list[dict], spec: dict) -> dict | None:
        preferred_scopes = {value: index for index, value in enumerate(spec.get("preferred_scopes", ()))} 
        preferred_granularities = {
            value: index for index, value in enumerate(spec.get("preferred_granularities", ()))
        }
        candidates = []
        for trend in trends:
            if trend.get("metric_type") not in spec["metric_candidates"]:
                continue
            if trend.get("unit") != spec["unit"]:
                continue
            if spec.get("region") is not None and trend.get("region") != spec.get("region"):
                continue
            if spec.get("subgroup") is not None and (trend.get("subgroup") or "") != spec.get("subgroup"):
                continue
            if len(trend.get("points") or []) < int(spec.get("min_points", 2)):
                continue
            candidates.append(trend)

        if not candidates:
            return None

        candidates.sort(
            key=lambda trend: (
                preferred_scopes.get(trend.get("period_scope") or "", 99),
                preferred_granularities.get(trend.get("period_granularity") or "", 99),
                -len(trend.get("points") or []),
                self._period_label_sort_key((trend.get("points") or [{}])[-1].get("period", "")),
            )
        )
        return candidates[0]

    def _epi_analytics(self, observations: list[dict], national_cascade: list[dict]) -> dict:
        regional_gaps = self._regional_gap_snapshots(observations)
        regional_scorecard = self._regional_scorecard(regional_gaps)
        relationship_scatter = self._relationship_scatter(observations)
        relationship_highlights = self._relationship_highlights(regional_gaps, relationship_scatter, national_cascade)
        return {
            "regional_gaps": regional_gaps,
            "regional_scorecard": regional_scorecard,
            "relationship_scatter": relationship_scatter,
            "relationship_highlights": relationship_highlights,
        }

    def _is_valid_region(self, region: str) -> bool:
        code = (region or "").strip().upper()
        return bool(code) and code in self.VALID_REGION_CODES

    def _regional_gap_snapshots(self, observations: list[dict]) -> list[dict]:
        specs = (
            {
                "snapshot_id": "regional_diagnosis_gap",
                "label": "Diagnosis coverage by region",
                "metric_candidates": ("diagnosis_coverage",),
                "target_value": 95.0,
                "unit": "percent",
                "note": "",
            },
            {
                "snapshot_id": "regional_treatment_gap",
                "label": "Treatment coverage by region",
                "metric_candidates": ("art_coverage",),
                "target_value": 95.0,
                "unit": "percent",
                "note": "",
            },
            {
                "snapshot_id": "regional_suppression_gap",
                "label": "Viral suppression by region",
                "metric_candidates": ("suppression_among_on_art", "vl_suppression_among_tested"),
                "target_value": 95.0,
                "unit": "percent",
                "note": "Using suppression among tested where suppression among PLHIV on ART is unavailable.",
            },
        )

        snapshots = []
        for spec in specs:
            payload = self._select_latest_regional_snapshot(observations, spec)
            if payload:
                snapshots.append(payload)
        return snapshots

    def _select_latest_regional_snapshot(self, observations: list[dict], spec: dict) -> dict | None:
        chosen_metric = ""
        chosen_rows: list[dict] = []
        for metric in spec["metric_candidates"]:
            rows = [
                row for row in observations
                if row.get("metric_type") == metric
                and row.get("unit") == spec["unit"]
                and self._is_valid_region(row.get("region") or "")
            ]
            if not rows:
                continue
            latest_period = max(
                (row.get("period_label") or row.get("year") or "" for row in rows),
                key=self._period_label_sort_key,
            )
            period_rows = [
                row for row in rows
                if (row.get("period_label") or row.get("year") or "") == latest_period
            ]
            if period_rows:
                chosen_metric = metric
                chosen_rows = period_rows
                break

        if not chosen_rows:
            return None

        grouped: dict[str, list[float]] = defaultdict(list)
        for row in chosen_rows:
            try:
                grouped[row["region"]].append(float(row["value"]))
            except (TypeError, ValueError):
                continue

        regions = []
        for region, values in grouped.items():
            if not values:
                continue
            value = round(sum(values) / len(values), 2)
            regions.append(
                {
                    "region": region,
                    "value": value,
                    "gap_to_target": round(spec["target_value"] - value, 2),
                }
            )

        regions.sort(key=lambda row: (-row["value"], row["region"]))
        if not regions:
            return None

        return {
            "snapshot_id": spec["snapshot_id"],
            "label": spec["label"],
            "metric_type": chosen_metric,
            "proxy": chosen_metric != spec["metric_candidates"][0],
            "note": spec["note"] if chosen_metric != spec["metric_candidates"][0] else "",
            "period_label": chosen_rows[0].get("period_label") or chosen_rows[0].get("year") or "",
            "target_value": spec["target_value"],
            "unit": spec["unit"],
            "spread": round(regions[0]["value"] - regions[-1]["value"], 2),
            "median_value": round(self._median([row["value"] for row in regions]), 2),
            "leader_region": regions[0]["region"],
            "laggard_region": regions[-1]["region"],
            "regions": regions,
        }

    def _regional_scorecard(self, regional_gaps: list[dict]) -> dict:
        metric_map = {
            "regional_diagnosis_gap": "diagnosis_coverage",
            "regional_treatment_gap": "treatment_coverage",
            "regional_suppression_gap": "viral_suppression",
        }
        by_region: dict[str, dict] = {}
        periods: dict[str, str] = {}

        for snapshot in regional_gaps:
            metric_key = metric_map.get(snapshot.get("snapshot_id") or "")
            if not metric_key:
                continue
            periods[metric_key] = snapshot.get("period_label") or ""
            for row in snapshot.get("regions") or []:
                region = row.get("region") or ""
                if not region:
                    continue
                bucket = by_region.setdefault(
                    region,
                    {
                        "region": region,
                        "metric_count": 0,
                        "values": {},
                        "gaps": {},
                    },
                )
                bucket["values"][metric_key] = row.get("value")
                bucket["gaps"][metric_key] = row.get("gap_to_target")

        rows = []
        for region, payload in by_region.items():
            gaps = [float(value) for value in payload["gaps"].values() if value not in ("", None)]
            if not gaps:
                continue
            rows.append(
                {
                    "region": region,
                    "metric_count": len(payload["values"]),
                    "mean_gap_to_target": round(sum(gaps) / len(gaps), 2),
                    "values": payload["values"],
                    "gaps": payload["gaps"],
                }
            )

        rows.sort(key=lambda row: (row["mean_gap_to_target"], -row["metric_count"], row["region"]))
        if not rows:
            return {"periods": periods, "rows": [], "top_performing": [], "largest_gaps": []}

        largest_gap_rows = sorted(
            rows,
            key=lambda row: (-row["mean_gap_to_target"], row["region"]),
        )
        return {
            "periods": periods,
            "rows": rows,
            "top_performing": rows[:5],
            "largest_gaps": largest_gap_rows[:5],
        }

    def _relationship_scatter(self, observations: list[dict]) -> list[dict]:
        specs = (
            {
                "chart_id": "diagnosis_vs_treatment",
                "label": "Diagnosis vs treatment coverage",
                "x_metric": "diagnosis_coverage",
                "y_metric_candidates": ("art_coverage",),
                "x_label": "Diagnosis coverage",
                "y_label": "Treatment coverage",
            },
            {
                "chart_id": "treatment_vs_suppression",
                "label": "Treatment vs viral suppression",
                "x_metric": "art_coverage",
                "y_metric_candidates": ("suppression_among_on_art", "vl_suppression_among_tested"),
                "x_label": "Treatment coverage",
                "y_label": "Viral suppression",
            },
        )

        charts = []
        for spec in specs:
            payload = self._build_relationship_scatter(observations, spec)
            if payload:
                charts.append(payload)
        return charts

    def _build_relationship_scatter(self, observations: list[dict], spec: dict) -> dict | None:
        x_rows = [
            row for row in observations
            if row.get("metric_type") == spec["x_metric"]
            and row.get("unit") == "percent"
            and self._is_valid_region(row.get("region") or "")
        ]
        if not x_rows:
            return None

        y_metric = ""
        y_rows: list[dict] = []
        for candidate in spec["y_metric_candidates"]:
            rows = [
                row for row in observations
                if row.get("metric_type") == candidate
                and row.get("unit") == "percent"
                and self._is_valid_region(row.get("region") or "")
            ]
            if rows:
                y_metric = candidate
                y_rows = rows
                break
        if not y_rows:
            return None

        shared_periods = {
            row.get("period_label") or row.get("year") or ""
            for row in x_rows
        } & {
            row.get("period_label") or row.get("year") or ""
            for row in y_rows
        }
        if not shared_periods:
            return None

        latest_period = max(shared_periods, key=self._period_label_sort_key)
        x_map = self._regional_value_map(x_rows, latest_period)
        y_map = self._regional_value_map(y_rows, latest_period)
        shared_regions = sorted(set(x_map) & set(y_map))
        if len(shared_regions) < 3:
            return None

        points = [
            {
                "region": region,
                "x": x_map[region],
                "y": y_map[region],
            }
            for region in shared_regions
        ]
        points.sort(key=lambda row: row["x"])

        correlation = ""
        if spearmanr is not None:
            result = spearmanr([row["x"] for row in points], [row["y"] for row in points])
            correlation = round(float(result.statistic), 3)

        median_x = self._median([row["x"] for row in points])
        median_y = self._median([row["y"] for row in points])
        for row in points:
            row["quadrant"] = self._quadrant_label(row["x"], row["y"], median_x, median_y)

        fit_line = []
        fit_r_squared = ""
        model_type = ""
        above_fit = None
        below_fit = None
        if len(points) >= 3:
            x_values = [row["x"] for row in points]
            y_values = [row["y"] for row in points]
            if np is not None and TheilSenRegressor is not None and len(points) >= 4:
                model = TheilSenRegressor(random_state=42)
                x_array = np.array(x_values, dtype=float).reshape(-1, 1)
                y_array = np.array(y_values, dtype=float)
                model.fit(x_array, y_array)
                slope = float(model.coef_[0])
                intercept = float(model.intercept_)
                predicted = [float(model.predict(np.array([[value]], dtype=float))[0]) for value in x_values]
                model_type = "theil_sen"
            else:
                mean_x = sum(x_values) / len(x_values)
                mean_y = sum(y_values) / len(y_values)
                variance_x = sum((value - mean_x) ** 2 for value in x_values)
                slope = 0.0 if variance_x == 0 else sum(
                    (x_value - mean_x) * (y_value - mean_y)
                    for x_value, y_value in zip(x_values, y_values)
                ) / variance_x
                intercept = mean_y - (slope * mean_x)
                predicted = [intercept + (slope * value) for value in x_values]
                model_type = "ols_linear"

            ss_res = sum((actual - pred) ** 2 for actual, pred in zip(y_values, predicted))
            mean_y = sum(y_values) / len(y_values)
            ss_tot = sum((actual - mean_y) ** 2 for actual in y_values)
            fit_r_squared = "" if ss_tot == 0 else round(max(0.0, 1 - (ss_res / ss_tot)), 3)
            min_x = min(x_values)
            max_x = max(x_values)
            fit_line = [
                {"x": round(min_x, 2), "y": round(intercept + (slope * min_x), 2)},
                {"x": round(max_x, 2), "y": round(intercept + (slope * max_x), 2)},
            ]

            residuals = [
                {
                    "region": row["region"],
                    "residual": round(row["y"] - predicted[index], 2),
                }
                for index, row in enumerate(points)
            ]
            residuals.sort(key=lambda row: row["residual"])
            below_fit = residuals[0]
            above_fit = residuals[-1]

        return {
            "chart_id": spec["chart_id"],
            "label": spec["label"],
            "period_label": latest_period,
            "x_metric": spec["x_metric"],
            "y_metric": y_metric,
            "x_label": spec["x_label"],
            "y_label": spec["y_label"],
            "proxy": y_metric != spec["y_metric_candidates"][0],
            "points": points,
            "spearman_r": correlation,
            "median_x": round(median_x, 2),
            "median_y": round(median_y, 2),
            "fit_line": fit_line,
            "fit_r_squared": fit_r_squared,
            "fit_model_type": model_type,
            "above_fit": above_fit,
            "below_fit": below_fit,
        }

    def _regional_value_map(self, rows: list[dict], period_label: str) -> dict[str, float]:
        grouped: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            current_period = row.get("period_label") or row.get("year") or ""
            if current_period != period_label:
                continue
            region = row.get("region") or ""
            if not self._is_valid_region(region):
                continue
            try:
                grouped[region].append(float(row["value"]))
            except (TypeError, ValueError):
                continue
        return {
            region: round(sum(values) / len(values), 2)
            for region, values in grouped.items()
            if values
        }

    def _relationship_highlights(
        self,
        regional_gaps: list[dict],
        relationship_scatter: list[dict],
        national_cascade: list[dict],
    ) -> list[dict]:
        highlights = []

        for snapshot in regional_gaps:
            regions = snapshot.get("regions") or []
            if len(regions) < 2:
                continue
            leader = regions[0]
            laggard = regions[-1]
            highlights.append(
                {
                    "title": snapshot["label"],
                    "detail": (
                        f"{leader['region']} leads at {leader['value']:.1f}%, while "
                        f"{laggard['region']} trails at {laggard['value']:.1f}% in {snapshot['period_label']}."
                    ),
                }
            )
            if len(highlights) >= 2:
                break

        for chart in relationship_scatter:
            correlation = chart.get("spearman_r")
            if correlation in ("", None):
                continue
            strength = "strong" if abs(correlation) >= 0.7 else "moderate" if abs(correlation) >= 0.4 else "weak"
            direction = "positive" if correlation >= 0 else "negative"
            highlights.append(
                {
                    "title": chart["label"],
                    "detail": (
                        f"{chart['period_label']} shows a {strength} {direction} regional association "
                        f"(Spearman r={correlation:.2f})."
                    ),
                }
            )
            above_fit = chart.get("above_fit")
            below_fit = chart.get("below_fit")
            if above_fit and below_fit:
                highlights.append(
                    {
                        "title": f"{chart['label']} outliers",
                        "detail": (
                            f"{above_fit['region']} sits above the fitted pattern "
                            f"({above_fit['residual']:+.1f}), while {below_fit['region']} sits below it "
                            f"({below_fit['residual']:+.1f}) in {chart['period_label']}."
                        ),
                    }
                )
            break

        for series in national_cascade:
            percent_points = series.get("percent_points") or []
            if len(percent_points) < 2:
                continue
            start = percent_points[0]
            end = percent_points[-1]
            delta = round(float(end["value"]) - float(start["value"]), 2)
            if abs(delta) < 1:
                continue
            highlights.append(
                {
                    "title": series["label"],
                    "detail": (
                        f"Moved from {start['value']:.1f}% in {start['period']} to "
                        f"{end['value']:.1f}% in {end['period']} ({delta:+.1f} points)."
                    ),
                }
            )
            break

        return highlights[:4]

    def _median(self, values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(float(value) for value in values)
        middle = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[middle]
        return (ordered[middle - 1] + ordered[middle]) / 2

    def _quadrant_label(self, x_value: float, y_value: float, median_x: float, median_y: float) -> str:
        if x_value >= median_x and y_value >= median_y:
            return "high-high"
        if x_value >= median_x and y_value < median_y:
            return "high-low"
        if x_value < median_x and y_value >= median_y:
            return "low-high"
        return "low-low"

    def _select_goal_trend(self, trends: list[dict], spec: dict) -> dict | None:
        return self._select_series_by_metric(
            trends,
            spec["metric_candidates"],
            unit=spec["target_unit"],
        )

    def _select_series_by_metric(
        self,
        trends: list[dict],
        metric_candidates: tuple[str, ...],
        *,
        unit: str,
    ) -> dict | None:
        for metric in metric_candidates:
            candidates = []
            for trend in trends:
                if trend.get("metric_type") != metric:
                    continue
                if trend.get("unit") != unit:
                    continue
                if trend.get("region") != "Philippines":
                    continue
                if trend.get("subgroup"):
                    continue
                if len(trend.get("points") or []) < 1:
                    continue
                candidates.append(trend)
            if candidates:
                candidates.sort(key=self._trend_sort_key)
                return candidates[0]
        return None

    def _forecast_trend_to_target(
        self,
        points: list[dict],
        period_granularity: str,
        target_value: float,
        deadline_year: int,
    ) -> dict | None:
        clean_points = []
        for point in points:
            try:
                value = float(point["value"])
            except (KeyError, TypeError, ValueError):
                continue
            x = self._period_point_to_decimal_year(point["period"], period_granularity)
            clean_points.append((x, value, point["period"]))

        if len(clean_points) < 2:
            return None

        latest_x, latest_value, latest_period = clean_points[-1]
        gap = target_value - latest_value
        if len(clean_points) < 3:
            return {
                "model_type": "ols_linear",
                "point_count": len(clean_points),
                "latest_period": latest_period,
                "latest_value": round(latest_value, 2),
                "gap_to_target": round(gap, 2),
                "trajectory": "insufficient_data",
                "deadline_status": "unknown",
                "projected_target_period": "",
                "projected_target_year": "",
                "years_to_target": "",
                "slope_per_year": "",
                "r_squared": "",
                "fit_quality": "low",
            }

        xs = [row[0] for row in clean_points]
        ys = [row[1] for row in clean_points]
        mean_y = sum(ys) / len(ys)
        model_type = "ols_linear"
        if np is not None and TheilSenRegressor is not None and len(clean_points) >= 4:
            model = TheilSenRegressor(random_state=42)
            x_array = np.array(xs, dtype=float).reshape(-1, 1)
            y_array = np.array(ys, dtype=float)
            model.fit(x_array, y_array)
            slope = float(model.coef_[0])
            intercept = float(model.intercept_)
            predicted = [float(model.predict(np.array([[x]], dtype=float))[0]) for x in xs]
            model_type = "theil_sen"
        else:
            mean_x = sum(xs) / len(xs)
            variance_x = sum((x - mean_x) ** 2 for x in xs)
            if variance_x == 0:
                return None

            slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / variance_x
            intercept = mean_y - slope * mean_x
            predicted = [intercept + slope * x for x in xs]
        ss_tot = sum((y - mean_y) ** 2 for y in ys)
        ss_res = sum((y - y_hat) ** 2 for y, y_hat in zip(ys, predicted))
        r_squared = 1.0 if ss_tot == 0 else max(0.0, min(1.0, 1 - (ss_res / ss_tot)))

        projected_target_year = ""
        projected_target_period = ""
        years_to_target = ""
        if latest_value < target_value and slope > 0:
            target_x = (target_value - intercept) / slope
            if target_x > latest_x:
                projected_years = target_x - latest_x
                if projected_years <= 15:
                    projected_target_year = round(target_x, 2)
                    projected_target_period = self._decimal_year_to_label(target_x, period_granularity)
                    years_to_target = round(projected_years, 2)

        if latest_value >= target_value:
            trajectory = "reached"
        elif r_squared < 0.2 and len(clean_points) >= 4:
            trajectory = "volatile"
        elif slope > 0.25:
            trajectory = "improving"
        elif slope < -0.25:
            trajectory = "declining"
        else:
            trajectory = "flat"

        deadline_x = self._deadline_decimal_year(deadline_year, period_granularity)
        if latest_value >= target_value:
            deadline_status = "met"
        elif latest_x >= deadline_x:
            deadline_status = "missed"
        elif projected_target_year and float(projected_target_year) <= deadline_x:
            deadline_status = "on_track"
        else:
            deadline_status = "unknown"

        if r_squared >= 0.8 and len(clean_points) >= 5:
            fit_quality = "high"
        elif r_squared >= 0.45 and len(clean_points) >= 4:
            fit_quality = "medium"
        else:
            fit_quality = "low"

        return {
            "model_type": model_type,
            "point_count": len(clean_points),
            "latest_period": latest_period,
            "latest_value": round(latest_value, 2),
            "gap_to_target": round(gap, 2),
            "trajectory": trajectory,
            "deadline_status": deadline_status,
            "projected_target_period": projected_target_period,
            "projected_target_year": projected_target_year,
            "years_to_target": years_to_target,
            "slope_per_year": round(slope, 3),
            "r_squared": round(r_squared, 3),
            "fit_quality": fit_quality,
        }

    def _period_point_to_decimal_year(self, period_label: str, period_granularity: str) -> float:
        text = str(period_label)
        match = re.match(r"^(\d{4})\s+Q([1-4])$", text, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            quarter = int(match.group(2))
            return year + ((quarter - 1) / 4.0)
        match = re.match(r"^(\d{4})-(\d{2})$", text)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            return year + ((month - 1) / 12.0)
        match = re.match(r"^(\d{4})$", text)
        if match:
            return float(match.group(1))
        return float(self._period_label_sort_key(text)[0])

    def _decimal_year_to_label(self, value: float, period_granularity: str) -> str:
        year = int(value)
        fractional = max(0.0, value - year)
        if period_granularity == "quarter":
            quarter = min(4, max(1, int(round(fractional * 4)) + 1))
            return f"{year} Q{quarter}"
        if period_granularity == "month":
            month = min(12, max(1, int(round(fractional * 12)) + 1))
            return f"{year}-{month:02d}"
        return str(year)

    def _deadline_decimal_year(self, deadline_year: int, period_granularity: str) -> float:
        if period_granularity == "quarter":
            return deadline_year + 0.75
        if period_granularity == "month":
            return deadline_year + (11 / 12)
        return float(deadline_year)

    def _review_queue_summary(self, review_queue: list[dict], review_queue_enriched: list[dict]) -> dict:
        by_reason = Counter()
        by_priority = Counter()
        by_document = Counter()
        for row in review_queue:
            by_reason[row.get("review_reason") or "unspecified"] += 1
            by_priority[row.get("priority") or "unspecified"] += 1
            by_document[row.get("filename") or ""] += 1

        by_family = Counter()
        family_action = {}
        for row in review_queue_enriched:
            family = row.get("template_family") or "unclassified"
            by_family[family] += 1
            if family not in family_action and row.get("recommended_strategy"):
                family_action[family] = row.get("recommended_strategy")

        chart_blocker_families = {
            "ocr_scrambled_table",
            "quick_facts_table",
            "mode_of_transmission_table",
            "age_distribution_table",
            "regional_distribution_table",
        }
        chart_blocker_count = sum(by_family.get(family, 0) for family in chart_blocker_families)
        actionable_narrative_count = sum(
            count
            for family, count in by_family.items()
            if family not in chart_blocker_families and family != "non_chart_context"
        )

        return {
            "count": len(review_queue),
            "by_reason": by_reason,
            "by_priority": by_priority,
            "by_family": by_family,
            "chart_blocker_count": chart_blocker_count,
            "actionable_narrative_count": actionable_narrative_count,
            "top_families": [
                {
                    "family": family,
                    "count": count,
                    "share": round((count / max(len(review_queue_enriched), 1)) * 100, 1),
                    "recommended_strategy": family_action.get(family, ""),
                }
                for family, count in by_family.most_common(8)
            ],
            "top_documents": [
                {"filename": filename, "review_count": count}
                for filename, count in by_document.most_common(10)
                if filename
            ],
        }

    def _overview_highlights(
        self,
        national_goal_board: list[dict],
        burden_views: list[dict],
        transmission_views: list[dict],
        epi_analytics: dict,
        review_summary: dict,
    ) -> list[dict]:
        highlights = []

        if national_goal_board:
            cascade_bits = []
            for row in national_goal_board:
                latest_value = row.get("latest_value")
                if latest_value in ("", None):
                    continue
                cascade_bits.append(f"{row.get('series_id', '').replace('_', ' ')} {float(latest_value):.1f}%")
            if cascade_bits:
                highlights.append(
                    {
                        "title": "95-95-95 status",
                        "detail": f"Latest national cascade readout: {', '.join(cascade_bits)}.",
                    }
                )

        if burden_views:
            longest = max(burden_views, key=lambda row: len(row.get("points") or []))
            highlights.append(
                {
                    "title": "Longest historical series",
                    "detail": (
                        f"{longest.get('label')} currently spans {longest.get('coverage_start') or 'unknown'} "
                        f"to {longest.get('coverage_end') or 'unknown'} across {longest.get('point_count') or 0} extracted points."
                    ),
                }
            )

        if transmission_views:
            strongest = max(transmission_views, key=lambda row: len(row.get("points") or []))
            highlights.append(
                {
                    "title": "Transmission trend coverage",
                    "detail": (
                        f"{strongest.get('label')} has the deepest structured history in the transmission layer "
                        f"with {strongest.get('point_count') or 0} points."
                    ),
                }
            )

        regional_gaps = epi_analytics.get("regional_gaps") or []
        if regional_gaps:
            snapshot = regional_gaps[0]
            highlights.append(
                {
                    "title": snapshot.get("label") or "Regional gap",
                    "detail": (
                        f"{snapshot.get('leader_region') or 'Unknown'} leads at {snapshot.get('regions', [{}])[0].get('value', 'n/a')}%, "
                        f"while {snapshot.get('laggard_region') or 'Unknown'} trails in {snapshot.get('period_label') or 'the latest period'}."
                    ),
                }
            )

        top_families = review_summary.get("top_families") or []
        if top_families:
            family = top_families[0]
            highlights.append(
                {
                    "title": "Largest remaining structuring family",
                    "detail": (
                        f"{family.get('family', 'unclassified').replace('_', ' ')} accounts for "
                        f"{family.get('count', 0)} backlog items."
                    ),
                }
            )

        return highlights[:6]

    def _top_documents(self, claims: list[dict]) -> list[dict]:
        counts = Counter(row["filename"] for row in claims if row.get("filename"))
        return [
            {"filename": filename, "claim_count": count}
            for filename, count in counts.most_common(10)
        ]

    def _read_csv(self, path: Path) -> list[dict]:
        if not path.exists() or path.stat().st_size == 0:
            return []
        with open(path, "r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def _write_json(self, path: Path, payload: dict):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=self._json_default)

    def _json_default(self, value):
        if isinstance(value, Counter):
            return dict(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
