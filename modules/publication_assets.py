from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, MaxNLocator, MultipleLocator


MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

CASCADE_COLORS = {
    "diagnosis": "#5975d9",
    "treatment": "#1a8b73",
    "suppression": "#db6b2c",
}

SERIES_COLORS = {
    "cases": "#0f7c66",
    "sexual": "#175f8c",
    "pregnant": "#0f7c66",
    "tgw": "#b35323",
    "ofw": "#1a8b73",
    "youth": "#3565af",
    "positive": "#1a8b73",
    "negative": "#c4561b",
    "alive": "#0f7c66",
    "ltfu": "#d86a2b",
    "not_on_treatment": "#c89a25",
}

REGION_ALIASES = {
    "1": "Region 1",
    "2": "Region 2",
    "3": "Region 3",
    "4A": "Region 4A",
    "4B": "Region 4B",
    "5": "Region 5",
    "6": "Region 6",
    "7": "Region 7",
    "8": "Region 8",
    "9": "Region 9",
    "10": "Region 10",
    "11": "Region 11",
    "12": "Region 12",
    "NCR": "NCR",
    "BARMM": "BARMM",
    "CAR": "CAR",
    "CARAGA": "CARAGA",
    "NIR": "NIR",
}


def _parse_numeric(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if not text or text in {"-", ".", "-."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _format_region(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return REGION_ALIASES.get(text, text)


def _period_sort_value(label: str) -> int:
    text = str(label or "").strip()
    if not text:
        return 0
    match = re.match(r"^(\d{4}) Q([1-4])$", text)
    if match:
        return int(match.group(1)) * 100 + int(match.group(2)) * 3
    match = re.match(r"^(\d{4})-(\d{2})$", text)
    if match:
        return int(match.group(1)) * 100 + int(match.group(2))
    match = re.match(r"^(\d{4})-(\d{4})-(\d{2})$", text)
    if match:
        return int(match.group(2)) * 100 + int(match.group(3))
    match = re.match(r"^(\d{4})-(\d{4}) Q([1-4])$", text)
    if match:
        return int(match.group(2)) * 100 + int(match.group(3)) * 3
    return 0


def _infer_period_from_filename(filename: str) -> tuple[str, int, int]:
    stem = Path(filename).stem.lower()
    year_match = re.search(r"(19|20)\d{2}", stem)
    year = int(year_match.group(0)) if year_match else 0

    quarter_match = re.search(r"(?:^|_|\b)q([1-4])(?:_|$|\b)", stem)
    if quarter_match and year:
        quarter = int(quarter_match.group(1))
        return f"{year} Q{quarter}", year, quarter * 3

    if any(token in stem for token in ("january_-_march", "january_to_march", "jan_-_mar", "jan_to_mar")) and year:
        return f"{year} Q1", year, 3
    if any(token in stem for token in ("april_-_june", "april_to_june", "apr_-_jun", "apr_to_jun", "april_-_jun")) and year:
        return f"{year} Q2", year, 6
    if any(token in stem for token in ("july_-_september", "july_to_september", "jul_sep", "jul_-_sep")) and year:
        return f"{year} Q3", year, 9
    if any(token in stem for token in ("october_-_december", "october_to_december", "oct_dec", "oct_-_dec", "october_-_dec")) and year:
        return f"{year} Q4", year, 12
    if "july-october" in stem and year:
        return f"{year}-10", year, 10

    found_months = []
    for token, month in MONTHS.items():
        if token in stem:
            found_months.append(month)
    if found_months and year:
        month = max(found_months)
        return f"{year}-{month:02d}", year, month

    if year:
        return f"{year}-12", year, 12
    return "", 0, 0


def _annualize_latest(points: list[dict], value_key: str = "value") -> list[dict]:
    by_year: dict[int, dict] = {}
    for point in points:
        year = int(point["year"])
        current = by_year.get(year)
        if current is None or point["sort_value"] > current["sort_value"]:
            by_year[year] = point
    return [by_year[year] for year in sorted(by_year)]


def _annualize_sum(points: list[dict], value_key: str = "value") -> list[dict]:
    by_year: dict[int, dict] = {}
    for point in points:
        year = int(point["year"])
        if year not in by_year:
            by_year[year] = {"year": year, "label": str(year), "sort_value": year * 100 + 12, value_key: 0.0}
        by_year[year][value_key] += float(point[value_key])
    return [by_year[year] for year in sorted(by_year)]


def _quarter_sort_key(label: str) -> float:
    match = re.match(r"^(\d{4}) Q([1-4])$", label)
    if match:
        return int(match.group(1)) + (int(match.group(2)) - 1) / 4
    return math.nan


def _year_ticks(start: int = 2010, end: int = 2025) -> list[int]:
    return [year for year in range(start, end + 1) if year in {start, 2015, 2020, end}]


def _complete_annual_series(points: list[dict], start: int = 2010, end: int = 2025) -> tuple[list[int], np.ndarray, list[int], list[float]]:
    by_year = {}
    for point in points:
        year = int(point.get("year") or 0)
        if year:
            by_year[year] = float(point["value"])
    years = list(range(start, end + 1))
    values = np.array([by_year.get(year, math.nan) for year in years], dtype=float)
    observed_years = [year for year in years if year in by_year]
    observed_values = [by_year[year] for year in observed_years]
    return years, values, observed_years, observed_values


def _nice_count_bounds(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 1.0
    lower = min(values)
    upper = max(values)
    span = max(upper - lower, upper * 0.08, 1.0)
    return max(0.0, lower - span * 0.18), upper + span * 0.12


def _nice_percent_bounds(values: list[float], step: int = 2) -> tuple[float, float, list[int]]:
    if not values:
        return 0.0, 100.0, list(range(0, 101, 20))
    lower = max(0.0, min(values) - 1.0)
    upper = min(100.0, max(values) + 1.0)
    floor = math.floor(lower / step) * step
    ceiling = math.ceil(upper / step) * step
    if ceiling - floor < step * 4:
        ceiling = min(100.0, floor + step * 4)
    ticks = list(range(int(floor), int(ceiling) + 1, step))
    return floor, ceiling, ticks


def _collapse_annual_plateaus(points: list[dict]) -> list[dict]:
    if not points:
        return []
    ordered = sorted(points, key=lambda row: int(row.get("year") or 0))
    collapsed = [ordered[0]]
    for row in ordered[1:]:
        previous = collapsed[-1]
        if abs(float(row["value"]) - float(previous["value"])) < 1e-6:
            previous["coverage_end"] = int(row["year"])
            continue
        collapsed.append(row)
    return collapsed


def _find_nearby_numbers(lines: list[str], start_index: int, window: int = 10) -> list[float]:
    values: list[float] = []
    for probe in lines[start_index:start_index + window]:
        normalized = probe.replace("\u00a0", " ").replace("\u2009", " ")
        for token in re.findall(r"\d[\d,]*(?:\.\d+)?", normalized):
            value = _parse_numeric(token)
            if value is not None:
                values.append(value)
    return values


def _humanize_residual_label(label: str, value: float) -> str:
    region, _, stage = str(label or "").partition(" | ")
    if "Suppression after treatment" in stage:
        phrase = "suppression above expected" if value > 0 else "suppression below expected"
    elif "Treatment after diagnosis" in stage:
        phrase = "treatment above expected" if value > 0 else "treatment below expected"
    else:
        phrase = "above expected" if value > 0 else "below expected"
    return f"{region}\n{phrase}"


@dataclass
class PublicationFigure:
    title: str
    note: str
    svg: str
    svg_path: str
    png_path: str


class PublicationAssetBuilder:
    def __init__(self, base_dir: str | Path | None = None):
        self.base_dir = Path(base_dir or Path(__file__).resolve().parents[1])
        self.normalized_dir = self.base_dir / "data" / "normalized"
        self.audit_dir = self.normalized_dir / "audit"
        self.processed_dir = self.base_dir / "data" / "processed_md"
        self.figure_dir = self.normalized_dir / "publication_figures"
        self.asset_path = self.normalized_dir / "publication_assets.json"

    def build(self) -> dict:
        self.figure_dir.mkdir(parents=True, exist_ok=True)
        dashboard = self._read_json(self.normalized_dir / "dashboard_feed.json")
        observations = self._read_observations()

        series = {
            "national_cascade": self._build_national_cascade(dashboard),
            "regional_ladder": self._build_regional_ladder(dashboard),
            "anomalies": self._build_anomalies(dashboard, observations),
            "historical": self._build_historical_series(observations),
            "key_populations": self._build_key_population_series(observations),
        }

        figures = {
            "national_cascade": self._render_national_cascade(series["national_cascade"]),
            "regional_ladder": self._render_regional_ladder(series["regional_ladder"]),
            "anomaly_board": self._render_anomaly_board(series["anomalies"]),
            "historical_board": self._render_historical_board(series["historical"]),
            "key_populations_board": self._render_key_population_board(series["key_populations"]),
        }

        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "series": series,
            "figures": {
                key: {
                    "title": figure.title,
                    "note": figure.note,
                    "svg": figure.svg,
                    "svg_path": figure.svg_path,
                    "png_path": figure.png_path,
                }
                for key, figure in figures.items()
            },
        }
        self.asset_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    def _read_json(self, path: Path) -> dict:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _read_observations(self) -> list[dict]:
        path = self.normalized_dir / "observations.csv"
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def _read_unaids_annual_series(self) -> dict:
        path = self.audit_dir / "unaids_philippines_all_ages.csv"
        rows = {
            "plhiv": [],
            "new_infections": [],
            "aids_deaths": [],
            "art_coverage_plhiv": [],
            "first_95": [],
            "second_95": [],
        }
        if not path.exists():
            return rows
        indicator_map = {
            "People living with HIV": "plhiv",
            "New HIV infections": "new_infections",
            "AIDS-related deaths": "aids_deaths",
            "ART coverage among all PLHIV": "art_coverage_plhiv",
            "1st 95 official": "first_95",
            "2nd 95 official": "second_95",
        }
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                key = indicator_map.get(row.get("indicator", ""))
                if not key:
                    continue
                year = int(row.get("year") or 0)
                value = _parse_numeric(row.get("value"))
                if not year or value is None or year < 2010:
                    continue
                rows[key].append({
                    "year": year,
                    "label": str(year),
                    "sort_value": year * 100 + 12,
                    "value": float(value),
                })
        return rows

    def _build_national_cascade(self, dashboard: dict) -> dict:
        rows = {row["series_id"]: row for row in dashboard.get("charts", {}).get("national_goal_board", [])}
        official = self._read_unaids_annual_series()
        patched = {
            "first_95": [
                ("2023 Q2", 59.0), ("2023 Q3", 61.0), ("2023 Q4", 63.0),
                ("2024 Q1", 57.0), ("2024 Q2", 59.0), ("2024 Q3", 61.0),
                ("2025 Q1", 55.0), ("2025 Q2", 57.0), ("2025 Q3", 59.0), ("2025 Q4", 61.0),
            ],
            "second_95": [
                ("2023 Q2", 64.0), ("2023 Q3", 64.0), ("2023 Q4", 63.0),
                ("2024 Q1", 65.0), ("2024 Q2", 66.0), ("2024 Q3", 67.0),
                ("2024 Q4", 67.0), ("2025 Q1", 66.0), ("2025 Q2", 66.0),
                ("2025 Q3", 67.0), ("2025 Q4", 66.0),
            ],
            "third_95": [
                ("2023 Q2", 32.0), ("2023 Q3", 40.0), ("2023 Q4", 36.0),
                ("2024 Q1", 39.0), ("2024 Q2", 39.0), ("2024 Q3", 39.0),
                ("2024 Q4", 40.0), ("2025 Q1", 40.0), ("2025 Q2", 47.0),
                ("2025 Q3", 57.0), ("2025 Q4", 57.0),
            ],
        }
        count_points = {
            "first_95": [("2025 Q1", 139610.0), ("2025 Q2", 144192.0), ("2025 Q3", 149375.0), ("2025 Q4", 153207.0)],
            "second_95": [
                ("2023 Q2", 70916.0), ("2023 Q3", 74258.0), ("2023 Q4", 75300.0),
                ("2024 Q1", 79643.0), ("2024 Q2", 84086.0), ("2024 Q3", 88544.0),
                ("2024 Q4", 90854.0), ("2025 Q1", 92712.0), ("2025 Q2", 95556.0),
                ("2025 Q3", 98198.0), ("2025 Q4", 100671.0),
            ],
            "third_95": [
                ("2023 Q2", 22690.0), ("2023 Q3", 29703.0), ("2023 Q4", 27138.0),
                ("2024 Q1", 31382.0), ("2024 Q2", 32794.0), ("2024 Q3", 34322.0),
                ("2024 Q4", 36723.0), ("2025 Q1", 36630.0), ("2025 Q2", 44714.0),
                ("2025 Q3", 55130.0), ("2025 Q4", 57184.0),
            ],
        }
        ordered = []
        titles = {
            "first_95": "1st 95: Diagnosis coverage",
            "second_95": "2nd 95: Treatment coverage",
            "third_95": "3rd 95: Viral suppression",
        }
        official_map = {
            "first_95": [row for row in official.get("first_95", []) if int(row.get("year") or 0) >= 2015],
            "second_95": [row for row in official.get("second_95", []) if int(row.get("year") or 0) >= 2015],
            "third_95": [],
        }
        official_labels = {
            "first_95": "Grey annual points show official UNAIDS context.",
            "second_95": "Grey annual points show official UNAIDS context.",
            "third_95": "Highlighted quarterly line comes from official SHIP/WHO reporting; UNAIDS does not publish an annual third-95 series for the Philippines.",
        }
        for series_id in ("first_95", "second_95", "third_95"):
            point_rows = [{"period": p, "value": v} for p, v in patched[series_id]]
            ordered.append({
                "series_id": series_id,
                "label": titles[series_id],
                "target": 95.0,
                "points": point_rows,
                "count_points": [{"period": p, "value": v} for p, v in count_points[series_id]],
                "latest_value": point_rows[-1]["value"],
                "latest_period": point_rows[-1]["period"],
                "gap_to_target": round(95.0 - point_rows[-1]["value"], 1),
                "coverage_start": point_rows[0]["period"],
                "coverage_end": point_rows[-1]["period"],
                "actual_metric_type": rows.get(series_id, {}).get("actual_metric_type", ""),
                "official_annual": official_map.get(series_id, []),
                "official_context_label": official_labels[series_id],
            })
        return {"rows": ordered}

    def _build_regional_ladder(self, dashboard: dict) -> dict:
        scorecard = dashboard.get("charts", {}).get("regional_scorecard", {})
        rows = []
        for row in scorecard.get("rows", []):
            if int(row.get("metric_count", 0) or 0) < 2:
                continue
            rows.append({
                "region": _format_region(row.get("region", "")),
                "diagnosis": float(row.get("values", {}).get("diagnosis_coverage", math.nan)),
                "treatment": float(row.get("values", {}).get("treatment_coverage", math.nan)),
                "suppression": float(row.get("values", {}).get("viral_suppression", math.nan)),
                "mean_gap": float(row.get("mean_gap_to_target", math.nan)),
            })
        rows.sort(key=lambda row: row["mean_gap"])
        return {"period_label": scorecard.get("periods", {}).get("diagnosis_coverage", "2025 Q4"), "rows": rows}

    def _build_anomalies(self, dashboard: dict, observations: list[dict]) -> dict:
        charts = dashboard.get("charts", {}).get("relationship_scatter", [])
        residual_rows = []
        for chart in charts[:2]:
            fit_line = chart.get("fit_line", [])
            if len(fit_line) < 2:
                continue
            x0, y0 = fit_line[0]["x"], fit_line[0]["y"]
            x1, y1 = fit_line[-1]["x"], fit_line[-1]["y"]
            slope = 0 if x1 == x0 else (y1 - y0) / (x1 - x0)
            pair_label = "Treatment after diagnosis" if "diagnosis" in chart.get("chart_id", "") else "Suppression after treatment"
            for point in chart.get("points", []):
                fit_y = y0 + slope * (point["x"] - x0)
                residual_rows.append({
                    "label": f"{_format_region(point['region'])} | {pair_label}",
                    "value": round(float(point["y"]) - float(fit_y), 2),
                })
        residual_rows = sorted(residual_rows, key=lambda row: abs(row["value"]), reverse=True)[:8]
        residual_rows = sorted(residual_rows, key=lambda row: row["value"])

        period_map = defaultdict(list)
        for row in observations:
            region = row.get("region", "")
            if region in {"", "Philippines"}:
                continue
            if row.get("metric_type") not in {"alive_on_art_count", "lost_to_follow_up_count", "not_on_treatment_count"}:
                continue
            period_label = row.get("period_label", "")
            sort_value = _period_sort_value(period_label)
            if sort_value:
                period_map[period_label].append(row)
        latest_period = max(period_map.keys(), key=_period_sort_value) if period_map else ""
        leakage = defaultdict(lambda: {"region": "", "alive": 0.0, "ltfu": 0.0, "not_on_treatment": 0.0})
        for row in period_map.get(latest_period, []):
            region = _format_region(row.get("region", ""))
            bucket = leakage[region]
            bucket["region"] = region
            value = float(row.get("value") or 0)
            if row["metric_type"] == "alive_on_art_count":
                bucket["alive"] = value
            elif row["metric_type"] == "lost_to_follow_up_count":
                bucket["ltfu"] = value
            elif row["metric_type"] == "not_on_treatment_count":
                bucket["not_on_treatment"] = value
        leakage_rows = sorted(leakage.values(), key=lambda row: row["ltfu"] + row["not_on_treatment"], reverse=True)[:10]
        return {"period_label": latest_period, "residual_rows": residual_rows, "leakage_rows": leakage_rows}

    def _build_historical_series(self, observations: list[dict]) -> dict:
        extracted = self._extract_markdown_series()
        official = self._read_unaids_annual_series()
        cases = _annualize_latest(extracted["cases"])
        sexual_obs = []
        for row in observations:
            if row.get("region") != "Philippines":
                continue
            if row.get("metric_type") != "sexual_contact_cases_pct":
                continue
            value = _parse_numeric(row.get("value"))
            year = int(row.get("year") or 0)
            if value is None or not year or value < 70 or value > 100:
                continue
            sexual_obs.append({
                "year": year,
                "label": row.get("period_label") or str(year),
                "sort_value": _period_sort_value(row.get("period_label", "")) or (year * 100 + 12),
                "value": float(value),
            })
        sexual_by_year = {}
        for row in _annualize_latest(sexual_obs):
            sexual_by_year[int(row["year"])] = row
        for row in _annualize_latest(extracted["sexual_share"]):
            year = int(row["year"])
            current = sexual_by_year.get(year)
            if current is None:
                sexual_by_year[year] = row
                continue
            current_value = float(current["value"])
            if abs(current_value - float(row["value"])) > 2.5 or row["sort_value"] >= current["sort_value"]:
                sexual_by_year[year] = row
        sexual = [sexual_by_year[year] for year in sorted(sexual_by_year)]
        return {
            "cases": cases,
            "sexual_share": sexual,
            "plhiv": official["plhiv"],
            "new_infections": official["new_infections"],
            "aids_deaths": official["aids_deaths"],
            "art_coverage_plhiv": official["art_coverage_plhiv"],
        }

    def _build_key_population_series(self, observations: list[dict]) -> dict:
        pregnant_rows = []
        youth_rows = []
        for row in observations:
            if row.get("region") != "Philippines":
                continue
            value = _parse_numeric(row.get("value"))
            if value is None:
                continue
            period_label = row.get("period_label", "")
            sort_value = _period_sort_value(period_label)
            if not sort_value:
                continue
            year = int(row.get("year") or 0)
            subgroup = row.get("subgroup", "")
            metric = row.get("metric_type", "")
            if metric == "pregnant_women_reported_count" and subgroup == "pregnant_women" and row.get("period_scope") == "cumulative":
                pregnant_rows.append({"year": year, "label": period_label, "sort_value": sort_value, "value": value})
            elif metric == "reported_cases_pct" and subgroup == "age_15_24":
                youth_rows.append({"year": year, "label": period_label, "sort_value": sort_value, "value": value})

        ofw_rows = []
        for row in observations:
            if row.get("region") != "Philippines":
                continue
            if row.get("metric_type") != "reported_cases_count" or row.get("subgroup") != "migrant_workers":
                continue
            value = _parse_numeric(row.get("value"))
            year = int(row.get("year") or 0)
            if value is None or not year:
                continue
            ofw_rows.append({
                "year": year,
                "label": row.get("period_label") or str(year),
                "sort_value": _period_sort_value(row.get("period_label", "")) or (year * 100 + 12),
                "value": float(value),
            })

        extracted = self._extract_markdown_series()

        pregnant_by_year = {}
        for row in _annualize_latest(pregnant_rows):
            pregnant_by_year[int(row["year"])] = row
        for row in _annualize_latest(extracted["pregnant_cumulative"]):
            year = int(row["year"])
            current = pregnant_by_year.get(year)
            if current is None:
                previous = pregnant_by_year.get(year - 1)
                if previous is None:
                    if 100 <= row["value"] <= 2000:
                        pregnant_by_year[year] = row
                else:
                    if previous["value"] * 0.8 <= row["value"] <= previous["value"] * 1.35:
                        pregnant_by_year[year] = row
                continue
            if current["value"] * 0.8 <= row["value"] <= current["value"] * 1.25 and row["sort_value"] > current["sort_value"]:
                pregnant_by_year[year] = row

        ofw_by_year = {}
        for row in _annualize_latest(ofw_rows):
            ofw_by_year[int(row["year"])] = row
        for row in _annualize_latest(extracted["ofw"]):
            ofw_by_year[int(row["year"])] = row

        youth_by_year = {}
        for row in _annualize_latest(youth_rows):
            youth_by_year[int(row["year"])] = row
        for row in _annualize_latest(extracted["youth_share"]):
            year = int(row["year"])
            current = youth_by_year.get(year)
            if current is None:
                youth_by_year[year] = row
                continue
            current_value = float(current["value"])
            extracted_value = float(row["value"])
            if extracted_value >= 20 and (current_value < 20 or abs(current_value - extracted_value) >= 4 or row["sort_value"] >= current["sort_value"]):
                youth_by_year[year] = row

        tgw_by_year = {}
        for row in _annualize_latest(extracted["tgw_cumulative"]):
            tgw_by_year[int(row["year"])] = row

        return {
            "pregnant_cumulative": [pregnant_by_year[year] for year in sorted(pregnant_by_year)],
            "tgw_cumulative": [tgw_by_year[year] for year in sorted(tgw_by_year)],
            "ofw_cumulative": [ofw_by_year[year] for year in sorted(ofw_by_year)],
            "youth_share": [youth_by_year[year] for year in sorted(youth_by_year)],
        }

    def _extract_markdown_series(self) -> dict:
        results = {
            "cases": [],
            "sexual_share": [],
            "ofw": [],
            "youth_share": [],
            "pregnant_cumulative": [],
            "tgw_cumulative": [],
        }
        for path in sorted(self.processed_dir.glob("*.md")):
            if path.name.startswith("_") or path.name.endswith(".markitdown.md"):
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            period_label, year, month = _infer_period_from_filename(path.name)
            if not year:
                continue
            sort_value = year * 100 + month
            cases_value = self._extract_total_cases(text)
            if cases_value:
                results["cases"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(cases_value), "filename": path.name})
            sexual_value = self._extract_sexual_share(text)
            if sexual_value is not None:
                results["sexual_share"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(sexual_value), "filename": path.name})
            ofw_value = self._extract_ofw_count(text)
            if ofw_value:
                results["ofw"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(ofw_value), "filename": path.name})
            youth_value = self._extract_youth_share(text)
            if youth_value is not None:
                results["youth_share"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(youth_value), "filename": path.name})
            pregnant_value = self._extract_pregnant_cumulative(text)
            if pregnant_value:
                results["pregnant_cumulative"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(pregnant_value), "filename": path.name})
            tgw_value = self._extract_tgw_cumulative(text)
            if tgw_value:
                results["tgw_cumulative"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(tgw_value), "filename": path.name})

        deduped = {}
        for key, rows in results.items():
            by_period = {}
            for row in rows:
                current = by_period.get(row["label"])
                if current is None or row["value"] > current["value"]:
                    by_period[row["label"]] = row
            deduped[key] = [by_period[label] for label in sorted(by_period, key=_period_sort_value)]
        return deduped

    def _extract_total_cases(self, text: str) -> float | None:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        number_pattern = re.compile(r"\d[\d,\s]{2,}")
        patterns = [
            re.compile(r"Cumulatively,\s*([\d,\s]+)\s+con\S*\s+HIV cases have been reported", re.I),
            re.compile(r"Since then, there have been\s+([\d,\s]+)\s+confirmed HIV cases reported to the HARP", re.I),
            re.compile(r"From January 1984 to [A-Za-z]+\s+\d{4}, there (?:has been|were)\s+([\d,\s]+).*?cases reported", re.I | re.S),
            re.compile(r"Majority of the total reported cases\s*\(([\d,\s]+)\s*,\s*[\d.]+%\)\s*were", re.I),
        ]
        for rx in patterns:
            match = rx.search(text)
            if match:
                value = _parse_numeric(match.group(1))
                if value and value > 1000:
                    return value
        for index, line in enumerate(lines):
            if "total reported cases" not in line.lower():
                continue
            candidates = []
            for probe in lines[index:index + 10]:
                for number in number_pattern.findall(probe):
                    value = _parse_numeric(number)
                    if value and value > 5000:
                        candidates.append(value)
            if candidates:
                return max(candidates)
        return None

    def _extract_sexual_share(self, text: str) -> float | None:
        patterns = [
            re.compile(r"of the [\d,\s]+ HIV positive cases.*?([\d,\s]+)\s*\(([\d.]+)%\)\s*were infected through sexual contact", re.I | re.S),
            re.compile(r"\(([\d.]+)%\)\s+acquired HIV through sexual contact", re.I),
            re.compile(r"Sexual contact\s*\(([\d.]+)%\)\s+was the leading mode of transmission", re.I),
            re.compile(r"Sexual contact\s*\(([\d.]+)%\)\s+was the predominant mode of transmission", re.I),
            re.compile(r"From January 1984(?:\s*[–-]\s*| to )?[A-Za-z]+\s+\d{4}.*?sexual contact.*?\(([\d.]+)%\)\s+was the predominant mode of transmission", re.I | re.S),
        ]
        for rx in patterns:
            match = rx.search(text)
            if match:
                value_group = match.lastindex if (match.lastindex or 0) >= 2 else 1
                value = _parse_numeric(match.group(value_group))
                count = _parse_numeric(match.group(1)) if (match.lastindex or 0) >= 2 else None
                if value is not None and 70 <= value <= 100 and (count is None or count >= 1000):
                    return value
        total_cases = self._extract_total_cases(text)
        if total_cases:
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            for index, line in enumerate(lines):
                normalized = re.sub(r"\s+", " ", line).strip().lower()
                if normalized not in {"sexual contact", "sexual"}:
                    continue
                values = _find_nearby_numbers(lines, index + 1, window=8)
                if len(values) < 6:
                    continue
                last_pair = values[-2:]
                cumulative = sum(last_pair)
                if total_cases * 0.8 <= cumulative <= total_cases:
                    return round((cumulative / total_cases) * 100.0, 1)
                large_values = [value for value in values if 1000 <= value <= total_cases]
                if len(large_values) >= 2:
                    candidate = sum(large_values[-2:])
                    if total_cases * 0.8 <= candidate <= total_cases:
                        return round((candidate / total_cases) * 100.0, 1)
            count_patterns = [
                re.compile(r"Of the [\d,\s]+ HIV positive cases.*?([\d,\s]+)\s*\(([\d.]+)%\)\s*were infected through sexual contact", re.I | re.S),
                re.compile(r"From January 1984(?:\s*[–-]\s*| to )?[A-Za-z]+\s+\d{4}, there (?:has been|were)\s*[\d,\s]+.*?([\d,\s]+)\s*\(([\d.]+)%\)\s*were infected through sexual contact", re.I | re.S),
            ]
            for rx in count_patterns:
                for match in rx.finditer(text):
                    count = _parse_numeric(match.group(1))
                    if count is None or count < 1000 or count > total_cases:
                        continue
                    share = (count / total_cases) * 100.0
                    if 70 <= share <= 100:
                        return round(share, 1)
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            for index, line in enumerate(lines):
                normalized = re.sub(r"\s+", " ", line).strip().lower()
                if normalized not in {"sexual contact", "sexual"}:
                    continue
                values = _find_nearby_numbers(lines, index + 1, window=10)
                if len(values) < 6:
                    continue
                last_pair = values[-2:]
                cumulative = sum(last_pair)
                if total_cases * 0.8 <= cumulative <= total_cases:
                    return round((cumulative / total_cases) * 100.0, 1)
                large_values = [value for value in values if 1000 <= value <= total_cases]
                if len(large_values) >= 2:
                    candidate = sum(large_values[-2:])
                    if total_cases * 0.8 <= candidate <= total_cases:
                        return round((candidate / total_cases) * 100.0, 1)
        return None

    def _extract_ofw_count(self, text: str) -> float | None:
        patterns = [
            re.compile(r"Since 1984,?\s*(?:a total of\s*)?([\d,\s]+)\s*\(([\d.]+)%\)\s*(?:migrant workers among the diagnosed cases|of diagnosed cases have been migrant workers)", re.I),
            re.compile(r"Since 1984,?\s*a total of\s*([\d,\s]+)\s*\(([\d.]+)%\)\s*migrant workers among the diagnosed", re.I),
            re.compile(r"There were\s*([\d,\s]+)\s*HIV positive OFWs since 1984", re.I),
            re.compile(r"From January 1984(?:\s*[–-]\s*| to )?[A-Za-z]+\s+\d{4},\s*[\d.]+%\s*\(([\d,\s]+)\)\s*of\s*the total cases were OFWs", re.I | re.S),
            re.compile(r"From January 1984(?:\s*[–-]\s*| to )?[A-Za-z]+\s+\d{4},\s*([\d,\s]+)\s*\(([\d.]+)%\)\s*of the total\s*cases were OFWs", re.I | re.S),
            re.compile(r"From January 1984(?:\s*[–-]\s*| to )?[A-Za-z]+\s+\d{4}, out of the [\d,\s]+ cases,\s*([\d,\s]+)\s*\(([\d.]+)%\)\s*were HIV[- ]positive OFWs", re.I | re.S),
            re.compile(r"From January 1984(?:\s*[â€“-]\s*| to )?[A-Za-z]+\s+\d{4},\s*(?:\w+\s+percent|[\d.]+%)\s*\(([\d,\s]+)\)\s*of\s*the total cases were OFWs", re.I | re.S),
            re.compile(r"([\d,\s]+)\s*\(([\d.]+)%\)\s*were OFWs", re.I),
        ]
        for rx in patterns:
            match = rx.search(text)
            if match:
                value = _parse_numeric(match.group(1))
                if value and value > 100:
                    return value
        return None

    def _extract_youth_share(self, text: str) -> float | None:
        patterns = [
            re.compile(r"From January 1984 to [A-Za-z]+\s+\d{4},\s*([\d,\s]+)\s*\(([\d.]+)%\)\s*of the reported cases were 15-24 years old", re.I),
            re.compile(r"From January 1984(?:\s*[–-]\s*| to )?[A-Za-z]+\s+\d{4},\s*([\d,\s]+)\s*\(([\d.]+)%\)\s*of the reported cases were 15-24 years old", re.I),
            re.compile(r"([\d,\s]+)\s*\(([\d.]+)%\)\s*were youth aged\s*15-24", re.I),
            re.compile(r"([\d,\s]+)\s*\(([\d.]+)%\)\s*were among the youth aged 15-24 years old", re.I),
            re.compile(r"([\d,\s]+)\s*\(([\d.]+)%\)\s*were youth\s*\(15-24 years old", re.I),
            re.compile(r"([\d,\s]+)\s*\(([\d.]+)%\)\s*were 15-24 years old", re.I),
        ]
        for rx in patterns:
            match = rx.search(text)
            if match:
                count = _parse_numeric(match.group(1))
                value = _parse_numeric(match.group(2))
                if count and count > 5000 and value is not None and 10 <= value <= 60:
                    return value
        total_cases = self._extract_total_cases(text)
        if total_cases:
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            for index, line in enumerate(lines):
                normalized = re.sub(r"\s+", " ", line).strip().lower()
                if normalized not in {"youth 15-24yo", "youth 15-24 y/o", "15-24 y/o"}:
                    continue
                values = _find_nearby_numbers(lines, index + 1, window=6)
                candidates = [value for value in values if 1000 <= value <= total_cases]
                if candidates:
                    return round((max(candidates) / total_cases) * 100.0, 1)
        return None

    def _extract_pregnant_cumulative(self, text: str) -> float | None:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        patterns = [
            re.compile(r"Since then, a total of\s*([\d,\s]+)\s*diagnosed women have been reported as pregnant at the time of diagnosis", re.I),
            re.compile(r"a total of\s*([\d,\s]+)\s*diagnosed women (?:were|was) reported pregnant at the time of diagnosis", re.I),
            re.compile(r"\b([\d,\s]+)\s*women reported pregnant at the time of diagnosis\b", re.I),
        ]
        for rx in patterns:
            match = rx.search(text)
            if match:
                value = _parse_numeric(match.group(1))
                if value and value > 100:
                    return value
        for index, line in enumerate(lines):
            lower = line.lower()
            if lower.startswith("reported pregnant"):
                candidates = []
                for probe in lines[index:index + 6]:
                    for number in re.findall(r"\d[\d,\s]{1,}", probe):
                        value = _parse_numeric(number)
                        if value and 100 <= value <= 5000 and int(value) not in range(1984, 2031):
                            candidates.append(value)
                if candidates:
                    return max(candidates)
        caption_candidates = []
        for index in range(len(lines)):
            window = " ".join(lines[index:index + 4])
            lower = window.lower()
            if "pregnant" not in lower:
                continue
            for number in re.findall(r"\(N?\s*=\s*([\d,\s]+)\)", window, re.I):
                value = _parse_numeric(number)
                if value and 100 <= value <= 5000:
                    caption_candidates.append(value)
        if caption_candidates:
            return max(caption_candidates)
        return None

    def _extract_tgw_cumulative(self, text: str) -> float | None:
        patterns = [
            re.compile(r"Of the\s*([\d,\s]+)\s*TGW diagnosed(?: with HIV)? from January 2018", re.I),
            re.compile(r"A total of\s*([\d,\s]+)\s*TGW were diagnosed from January 2018", re.I),
            re.compile(r"January 2018 to [A-Za-z]+\s+\d{4},\s*(?:three|\d+(?:\.\d+)?)\s*percent\s*\(?([\d,\s]+)\)?\s*of\s*[\d,\s]+[a-z]?\s*diagnosed.*?transgender women", re.I | re.S),
        ]
        for rx in patterns:
            match = rx.search(text)
            if match:
                value = _parse_numeric(match.group(1))
                if value and value > 500:
                    return value
        return None

    def _base_style(self):
        plt.rcParams.update({
            "font.family": "DejaVu Sans",
            "font.size": 12,
            "axes.edgecolor": "#d6d6cf",
            "axes.linewidth": 0.8,
            "axes.facecolor": "#fffdf8",
            "figure.facecolor": "#fffaf0",
            "savefig.facecolor": "#fffaf0",
            "savefig.transparent": False,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.titleweight": "bold",
        })

    def _finalize_axis(self, ax):
        ax.grid(axis="y", color="#d6ddd7", linestyle=(0, (3, 3)), linewidth=1)
        ax.spines["left"].set_color("#cfd6d0")
        ax.spines["bottom"].set_color("#aab6b0")
        ax.tick_params(axis="both", colors="#50615d", labelsize=11)
        ax.set_facecolor("#fffdf8")

    def _save_figure(self, figure: plt.Figure, basename: str, title: str, note: str) -> PublicationFigure:
        svg_path = self.figure_dir / f"{basename}.svg"
        png_path = self.figure_dir / f"{basename}.png"
        figure.savefig(svg_path, format="svg", bbox_inches="tight")
        figure.savefig(png_path, format="png", dpi=180, bbox_inches="tight")
        plt.close(figure)
        svg = svg_path.read_text(encoding="utf-8")
        return PublicationFigure(
            title=title,
            note=note,
            svg=svg,
            svg_path=str(svg_path.relative_to(self.base_dir)).replace("\\", "/"),
            png_path=str(png_path.relative_to(self.base_dir)).replace("\\", "/"),
        )

    def _render_national_cascade(self, data: dict) -> PublicationFigure:
        self._base_style()
        fig = plt.figure(figsize=(14.6, 5.4))
        gs = GridSpec(1, 3, figure=fig, wspace=0.18)
        short_titles = {"first_95": "1st 95: Diagnosed", "second_95": "2nd 95: On ART", "third_95": "3rd 95: Suppressed"}
        legend_handles = [
            Line2D([0], [0], color="#9eb5c7", linewidth=2.0, linestyle=(0, (2, 2)), marker="o", markerfacecolor="#fffdf8", markeredgecolor="#9eb5c7", markeredgewidth=1.2, markersize=6, label="Official published context"),
            Line2D([0], [0], color="#0f7c66", linewidth=3.0, marker="o", markerfacecolor="#db6b2c", markeredgecolor="#fffaf0", markeredgewidth=1.2, markersize=6, label="Quarterly surveillance"),
            Line2D([0], [0], color="#c4561b", linewidth=1.4, linestyle=(0, (4, 3)), label="95 target"),
        ]
        for idx, row in enumerate(data["rows"]):
            ax = fig.add_subplot(gs[0, idx])
            xs = [_quarter_sort_key(point["period"]) for point in row["points"]]
            ys = [point["value"] for point in row["points"]]
            annual_points = row.get("official_annual", [])
            annual_xs = [point["year"] + 0.75 for point in annual_points]
            annual_ys = [point["value"] for point in annual_points]
            if annual_xs:
                ax.plot(annual_xs, annual_ys, color="#9eb5c7", linewidth=2.0, linestyle=(0, (2, 2)), zorder=1)
                ax.scatter(annual_xs, annual_ys, facecolors="#fffdf8", edgecolors="#9eb5c7", linewidth=1.2, s=28, zorder=2)
            ax.plot(xs, ys, color="#0f7c66", linewidth=3.0, zorder=3)
            ax.scatter(xs, ys, color="#db6b2c", edgecolors="#fffaf0", linewidth=1.4, s=50, zorder=4)
            ax.axhline(95, color="#c4561b", linewidth=1.4, linestyle=(0, (4, 3)), zorder=1)
            ax.text(xs[-1], 95.7, "95 target", color="#9b3c16", fontsize=10, ha="right", va="bottom")
            ax.set_xlim(2015.0, _quarter_sort_key("2025 Q4") + 0.08)
            ax.set_ylim(25, 100)
            ax.set_xticks([2015, 2020, _quarter_sort_key("2023 Q2"), _quarter_sort_key("2025 Q4")])
            ax.set_xticklabels(["2015", "2020", "2023 Q2", "2025 Q4"])
            ax.yaxis.set_major_locator(MultipleLocator(10))
            ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(value)}%"))
            self._finalize_axis(ax)
            ax.set_title(short_titles[row["series_id"]], fontsize=14, fontfamily="DejaVu Serif", loc="left", pad=10)
            ax.text(
                0.02,
                0.92,
                f"{row['latest_period']}: {row['latest_value']:.0f}% | gap {row['gap_to_target']:.0f}",
                transform=ax.transAxes,
                fontsize=10.2,
                color="#4c5f5a",
                fontweight="bold",
                bbox={"facecolor": "#fffaf0", "edgecolor": "none", "boxstyle": "round,pad=0.18", "alpha": 0.82},
            )
            context_text = row.get("official_context_label", "")
            ax.text(0.02, 0.04, context_text, transform=ax.transAxes, fontsize=9.4, color="#5e6a66")
            ax.set_ylabel("Coverage")
        fig.legend(handles=legend_handles, loc="upper center", bbox_to_anchor=(0.5, 1.03), ncol=3, frameon=False, fontsize=11)
        fig.subplots_adjust(top=0.84, bottom=0.14, left=0.06, right=0.98)
        note = "Grey annual points show official UNAIDS context where published. The highlighted line shows official quarterly HIV surveillance reporting. The third 95 uses suppression among PLHIV on ART and has no annual UNAIDS comparator series for the Philippines."
        return self._save_figure(fig, "national_cascade_board", "National 95-95-95 cascade", note)

    def _render_regional_ladder(self, data: dict) -> PublicationFigure:
        self._base_style()
        rows = data["rows"]
        fig, ax = plt.subplots(figsize=(15.2, 9.0))
        y_positions = np.arange(len(rows))[::-1]
        markers = {"diagnosis": "o", "treatment": "s", "suppression": "D"}
        y_offsets = {"diagnosis": 0.12, "treatment": 0.0, "suppression": -0.12}
        for y, row in zip(y_positions, rows):
            points = [row["diagnosis"], row["treatment"], row["suppression"]]
            ax.hlines(y, min(points), max(points), color="#c1c8c3", linewidth=2.4, zorder=1)
            ax.scatter(row["diagnosis"], y + y_offsets["diagnosis"], color=CASCADE_COLORS["diagnosis"], marker=markers["diagnosis"], s=130, edgecolors="#fffaf0", linewidth=1.8, zorder=5)
            ax.scatter(row["treatment"], y + y_offsets["treatment"], color=CASCADE_COLORS["treatment"], marker=markers["treatment"], s=115, edgecolors="#fffaf0", linewidth=1.6, zorder=6)
            ax.scatter(row["suppression"], y + y_offsets["suppression"], color=CASCADE_COLORS["suppression"], marker=markers["suppression"], s=120, edgecolors="#fffaf0", linewidth=1.6, zorder=7)
        ax.axvline(95, color="#c4561b", linewidth=1.6, linestyle=(0, (4, 3)), zorder=1)
        ax.annotate("95% target", xy=(95, 1.0), xycoords=("data", "axes fraction"), xytext=(-10, 8), textcoords="offset points", ha="right", va="bottom", color="#9b3c16", fontsize=11)
        ax.set_xlim(30, 96)
        ax.set_ylim(-0.8, len(rows) - 0.2)
        ax.set_yticks(y_positions)
        ax.set_yticklabels([row["region"] for row in rows], fontsize=12)
        ax.tick_params(axis="y", length=0)
        ax.set_xticks([30, 40, 50, 60, 70, 80, 90])
        ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(value)}%"))
        self._finalize_axis(ax)
        ax.set_xlabel("Coverage at latest comparable quarter", fontsize=12, labelpad=12)
        legend_handles = [
            Line2D([0], [0], marker=markers["diagnosis"], color="none", markerfacecolor=CASCADE_COLORS["diagnosis"], markeredgecolor="#fffaf0", markeredgewidth=1.4, markersize=10, label="Diagnosis"),
            Line2D([0], [0], marker=markers["treatment"], color="none", markerfacecolor=CASCADE_COLORS["treatment"], markeredgecolor="#fffaf0", markeredgewidth=1.4, markersize=10, label="Treatment"),
            Line2D([0], [0], marker=markers["suppression"], color="none", markerfacecolor=CASCADE_COLORS["suppression"], markeredgecolor="#fffaf0", markeredgewidth=1.4, markersize=10, label="Viral suppression"),
        ]
        ax.legend(handles=legend_handles, loc="upper left", frameon=False, ncol=3, bbox_to_anchor=(0.0, 1.02), fontsize=11)
        leader = rows[0]["region"]
        laggard = rows[-1]["region"]
        note = f"Regions are ordered from smallest to largest average gap to the 95% target. {leader} is closest overall; {laggard} is furthest away."
        fig.subplots_adjust(left=0.23, right=0.98, top=0.90, bottom=0.12)
        return self._save_figure(fig, "regional_cascade_ladder", "Regional cascade ladder", note)

    def _render_anomaly_board(self, data: dict) -> PublicationFigure:
        self._base_style()
        fig = plt.figure(figsize=(15.4, 7.2))
        gs = GridSpec(1, 2, figure=fig, width_ratios=[1.2, 1.0], wspace=0.28)
        ax_left = fig.add_subplot(gs[0, 0])
        ax_right = fig.add_subplot(gs[0, 1])

        residual_rows = data["residual_rows"][-6:] if len(data["residual_rows"]) > 6 else data["residual_rows"]
        labels = [_humanize_residual_label(row["label"], row["value"]) for row in residual_rows]
        values = [row["value"] for row in residual_rows]
        colors = [SERIES_COLORS["positive"] if value > 0 else SERIES_COLORS["negative"] for value in values]
        ypos = np.arange(len(labels))
        ax_left.barh(ypos, values, color=colors, edgecolor="white", linewidth=1.0)
        ax_left.axvline(0, color="#8ca29d", linewidth=1.0)
        ax_left.set_yticks(ypos)
        ax_left.set_yticklabels(labels, fontsize=10.5)
        for y, value in zip(ypos, values):
            ax_left.text(value + (0.4 if value >= 0 else -0.4), y, f"{value:+.1f}", va="center", ha="left" if value >= 0 else "right", fontsize=10, color="#4c5f5a")
        self._finalize_axis(ax_left)
        ax_left.set_xlabel("Percentage points above or below expected")
        ax_left.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(value):+d}"))

        leak_rows = data["leakage_rows"][:8]
        regions = [row["region"] for row in leak_rows][::-1]
        alive = [row["alive"] for row in leak_rows][::-1]
        ltfu = [row["ltfu"] for row in leak_rows][::-1]
        not_on = [row["not_on_treatment"] for row in leak_rows][::-1]
        ypos = np.arange(len(regions))
        ax_right.barh(ypos, alive, color=SERIES_COLORS["alive"], label="Alive on ART")
        ax_right.barh(ypos, ltfu, left=alive, color=SERIES_COLORS["ltfu"], label="Lost to follow-up")
        ax_right.barh(ypos, not_on, left=np.array(alive) + np.array(ltfu), color=SERIES_COLORS["not_on_treatment"], label="Not on treatment")
        ax_right.set_yticks(ypos)
        ax_right.set_yticklabels(regions, fontsize=11)
        ax_right.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(value/1000)}K" if value >= 1000 else f"{int(value)}"))
        self._finalize_axis(ax_right)
        ax_right.set_xlabel(f"Latest structured treatment-outcome snapshot ({data['period_label']})")
        ax_right.legend(loc="upper left", frameon=False, fontsize=11)
        fig.subplots_adjust(left=0.24, right=0.98, top=0.92, bottom=0.13)
        note = f"Residuals show which regions are above or below the fitted cascade pattern. Leakage uses the {data['period_label']} treatment-outcome snapshot."
        return self._save_figure(fig, "anomaly_board", "Regional outliers and treatment leakage", note)

    def _shade_unavailable(self, ax, start_year: int, end_year: int, observed_start: int | None, observed_end: int | None):
        if observed_start is None or observed_end is None:
            ax.axvspan(start_year - 0.5, end_year + 0.5, color="#edf0ec", alpha=0.65, zorder=0)
            return
        if observed_start > start_year:
            ax.axvspan(start_year - 0.5, observed_start - 0.5, color="#edf0ec", alpha=0.65, zorder=0)
        if observed_end < end_year:
            ax.axvspan(observed_end + 0.5, end_year + 0.5, color="#edf0ec", alpha=0.65, zorder=0)

    def _annual_series_bounds(self, points: list[dict]) -> tuple[int | None, int | None]:
        years = [int(point["year"]) for point in points if point.get("year")]
        if not years:
            return None, None
        return min(years), max(years)

    def _render_historical_board(self, data: dict) -> PublicationFigure:
        self._base_style()
        fig = plt.figure(figsize=(15.6, 9.0))
        gs = GridSpec(2, 2, figure=fig, hspace=0.44, wspace=0.22)
        axes = [fig.add_subplot(gs[row, col]) for row in range(2) for col in range(2)]
        panel_specs = [
            ("Cumulative reported HIV cases", data["cases"], SERIES_COLORS["cases"], "count", "Direct surveillance end-of-year cumulative count, 2010 to 2025."),
            ("People living with HIV", data["plhiv"], "#3565af", "count", "Official UNAIDS annual estimate, 2010 to 2024."),
            ("New HIV infections", data["new_infections"], "#b35323", "count", "Official UNAIDS annual estimate, 2010 to 2024."),
            ("AIDS-related deaths", data["aids_deaths"], "#8a3f2a", "count", "Official UNAIDS annual estimate, 2010 to 2024."),
        ]

        for ax, (title, points, color, unit, subtitle) in zip(axes, panel_specs):
            years, series, observed_years, observed_values = _complete_annual_series(points)
            observed_start, observed_end = self._annual_series_bounds(points)
            self._shade_unavailable(ax, 2010, 2025, observed_start, observed_end)
            if observed_years:
                if unit == "count":
                    ymin, ymax = _nice_count_bounds(observed_values)
                    baseline = ymin
                    ax.set_ylim(ymin, ymax)
                    ax.yaxis.set_major_locator(MaxNLocator(5))
                    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(round(value / 1000.0))}K" if value >= 1000 else f"{int(round(value))}"))
                else:
                    ymin, ymax, yticks = _nice_percent_bounds(observed_values, step=2)
                    baseline = ymin
                    ax.set_ylim(ymin, ymax)
                    ax.set_yticks(yticks)
                    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(round(value))}%"))
                ax.plot(years, series, color=color, linewidth=3.0, zorder=3)
                ax.fill_between(years, series, baseline, where=np.isfinite(series), color="#dbece7", alpha=0.6, zorder=1)
                ax.scatter(observed_years, observed_values, color="#db6b2c", edgecolors="#fffaf0", linewidth=1.4, s=48, zorder=4)
            ax.set_title(title, fontsize=16, fontfamily="DejaVu Serif", loc="left", pad=14)
            ax.text(0.0, 1.03, subtitle, transform=ax.transAxes, fontsize=9.6, color="#5e6a66")
            if observed_years:
                ax.text(1.0, 1.03, f"{observed_start}-{observed_end}", transform=ax.transAxes, ha="right", fontsize=9.6, color="#0c6150", fontweight="bold")
            ax.set_xlim(2009.5, 2025.5)
            ax.set_xticks(_year_ticks(2010, 2025))
            self._finalize_axis(ax)

        fig.subplots_adjust(top=0.93, bottom=0.10, left=0.07, right=0.98)
        note = "Historical board combines direct surveillance counts with official UNAIDS annual estimates. Shaded years indicate no observed or published value for that panel."
        return self._save_figure(fig, "historical_board", "Historical burden and exposure shift", note)

    def _render_key_population_board(self, data: dict) -> PublicationFigure:
        self._base_style()
        fig = plt.figure(figsize=(15.2, 10.2))
        gs = GridSpec(2, 2, figure=fig, hspace=0.52, wspace=0.20)

        panel_specs = [
            ("Pregnant women diagnosed (cumulative)", data["pregnant_cumulative"], SERIES_COLORS["pregnant"], "count", "Annual latest cumulative value from quarterly surveillance."),
            ("TGW diagnosed (cumulative)", data["tgw_cumulative"], SERIES_COLORS["tgw"], "count", "Annual latest cumulative count from the surveillance series."),
            ("OFW cumulative burden", data["ofw_cumulative"], SERIES_COLORS["ofw"], "count", "Annual latest cumulative count from direct source extraction."),
            ("Youth share of reported cases", data["youth_share"], SERIES_COLORS["youth"], "percent", "Annual latest share among people aged 15-24."),
        ]

        axes = [fig.add_subplot(gs[row, col]) for row in range(2) for col in range(2)]
        for ax, (title, points, color, unit, subtitle) in zip(axes, panel_specs):
            panel_points = _collapse_annual_plateaus(points) if title == "TGW diagnosed (cumulative)" else points
            years, series, observed_years, observed_values = _complete_annual_series(panel_points)
            observed_start, observed_end = self._annual_series_bounds(panel_points)
            self._shade_unavailable(ax, 2010, 2025, observed_start, observed_end)
            if observed_years:
                baseline = 0.0
                ax.plot(years, series, color=color, linewidth=3.0, zorder=3)
                if unit == "percent":
                    ymin, ymax, yticks = _nice_percent_bounds(observed_values, step=2)
                    baseline = ymin
                    ax.set_ylim(ymin, ymax)
                    ax.set_yticks(yticks)
                    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(round(value))}%"))
                else:
                    ymin, ymax = _nice_count_bounds(observed_values)
                    baseline = ymin
                    ax.set_ylim(ymin, ymax)
                    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(round(value / 1000.0))}K" if value >= 1000 else f"{int(round(value))}"))
                ax.fill_between(years, series, baseline, where=np.isfinite(series), color="#dbece7", alpha=0.6, zorder=1)
                ax.scatter(observed_years, observed_values, color="#db6b2c", edgecolors="#fffaf0", linewidth=1.4, s=48, zorder=4)
            ax.set_title(title, fontsize=15, fontfamily="DejaVu Serif", loc="left", pad=12)
            ax.text(0.0, 1.03, subtitle, transform=ax.transAxes, fontsize=9.6, color="#5e6a66")
            if observed_years:
                ax.text(1.0, 1.03, f"{observed_start}-{observed_end}", transform=ax.transAxes, ha="right", fontsize=9.6, color="#0c6150", fontweight="bold")
            ax.set_xlim(2009.5, 2025.5)
            ax.set_xticks(_year_ticks(2010, 2025))
            ax.yaxis.set_major_locator(MaxNLocator(5))
            self._finalize_axis(ax)

        fig.subplots_adjust(top=0.93, bottom=0.09, left=0.07, right=0.98)
        note = "All four panels share the same 2010 to 2025 x-axis. Shaded periods indicate years with no observed values for that specific series."
        return self._save_figure(fig, "key_populations_board", "Key population sentinel panels", note)
