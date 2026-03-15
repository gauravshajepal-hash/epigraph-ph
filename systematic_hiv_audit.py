from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parent
NORMALIZED_DIR = ROOT / "data" / "normalized"
AUDIT_DIR = NORMALIZED_DIR / "audit"
OBSERVATIONS_CSV = NORMALIZED_DIR / "observations.csv"
CLAIMS_CSV = NORMALIZED_DIR / "claims.csv"
UNAIDS_CSV = ROOT / "tmp" / "unaids_audit" / "unzipped" / "Estimates_2025_en.csv"
PROCESSED_MD_DIR = ROOT / "data" / "processed_md"

VALID_REGIONS = {
    "NCR", "CAR", "CARAGA", "BARMM", "ARMM", "NIR",
    "1", "2", "3", "4A", "4B", "5", "6", "7", "8", "9", "10", "11", "12",
}

UNAIDS_DATASET_URL = "https://aidsinfo.unaids.org/dataset"
WHO_2025_ARTICLE_URL = "https://www.who.int/philippines/news/detail/11-06-2025-unaids--who-support-doh-s-call-for-urgent-action-as-the-philippines-faces-the-fastest-growing-hiv-surge-in-the-asia-pacific-region"
WHO_COUNTRY_PROFILE_URL = "https://cdn.who.int/media/docs/default-source/wpro---documents/countries/philippines/philippines-country-profile-hiv-2016.pdf"

WORLD_BANK_INDICATORS = {
    "SH.DYN.AIDS.ZS": "Prevalence of HIV, total (% of population ages 15-49)",
    "SH.HIV.INCD.ZS": "Incidence of HIV, ages 15-49 (per 1,000 uninfected population ages 15-49)",
    "SH.HIV.INCD": "Adults (ages 15+) and children (ages 0-14) newly infected with HIV",
    "SH.HIV.ARTC.ZS": "Antiretroviral therapy coverage (% of people living with HIV)",
    "SH.HIV.PMTC.ZS": "Use of any antiretroviral therapy among pregnant women with HIV",
}

PRIMARY_SHIP_FILENAMES = [
    "2024_Q4_HIV_AIDS_Surveillance_of_the_Philippines_2.pdf",
    "HIV_STI_2025_January_-_March.pdf",
    "HIV_STI_2025_Q2.pdf",
    "HIV_STI_2025_Jul_Sep.pdf",
    "HIV_STI_2025_Q4.pdf",
]

TREATMENT_TABLE_HEADER_STOP = (
    "Table ",
    "TREATMENT",
    "Note:",
)

TREATMENT_TABLE_SKIP_LINES = {
    "Region of",
    "Treatment",
    "Facility",
    "Viral Load Status among PLHIV on ART per region",
    "Alive on ART",
    "Tested for VL",
    "VL",
    "Suppressed",
    "%",
    "Tested",
    "for VL",
}

TREATMENT_TABLE_BREAK_PATTERNS = (
    re.compile(r"^## \[Page "),
    re.compile(r"^\d+\.\s"),
)


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_float(value: str | None) -> float | None:
    if value in (None, "", "..."):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def fetch_world_bank_series() -> list[dict]:
    rows: list[dict] = []
    for indicator_id, label in WORLD_BANK_INDICATORS.items():
        url = f"https://api.worldbank.org/v2/country/PHL/indicator/{indicator_id}?format=json&per_page=200"
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        payload = response.json()
        data_rows = payload[1]
        for row in data_rows:
            if row.get("value") in (None, ""):
                continue
            rows.append(
                {
                    "indicator_id": indicator_id,
                    "indicator": label,
                    "year": int(row["date"]),
                    "value": row.get("value"),
                    "unit": row.get("unit") or "",
                    "obs_status": row.get("obs_status") or "",
                    "decimal": row.get("decimal"),
                    "source_note": "World Bank API",
                    "source_url": url,
                }
            )
    rows.sort(key=lambda row: (row["indicator_id"], row["year"]))
    return rows


def load_quarter_national_headline(observations: list[dict]) -> dict[str, dict]:
    metrics = {
        "diagnosed_plhiv_count",
        "diagnosed_plhiv_pct",
        "plhiv_on_art_count",
        "plhiv_on_art_pct",
        "viral_load_tested_count",
        "viral_load_tested_pct",
        "viral_load_suppressed_count",
        "viral_load_suppressed_pct",
        "suppression_among_on_art_pct",
    }
    bucket: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in observations:
        if row.get("document_type") != "surveillance_report":
            continue
        if row.get("region") != "Philippines":
            continue
        if row.get("metric_type") not in metrics:
            continue
        period = row.get("period_label") or ""
        if not period:
            continue
        bucket[(period, row["metric_type"])].append(row)

    chosen: dict[str, dict] = defaultdict(dict)
    for (period, metric), rows in bucket.items():
        rows.sort(
            key=lambda row: (
                0 if str(row.get("page_index") or "") == "0" else 1,
                -(parse_float(row.get("value")) or 0.0),
            )
        )
        selected = rows[0]
        chosen[period][metric] = {
            "value": parse_float(selected.get("value")),
            "filename": selected.get("filename") or "",
            "page_index": selected.get("page_index") or "",
        }
    return chosen


def load_quarter_regional_sums(observations: list[dict]) -> dict[str, dict]:
    metrics = {"estimated_plhiv", "diagnosed_plhiv", "on_art", "vl_tested", "vl_suppressed"}
    grouped: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    row_counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in observations:
        if row.get("document_type") != "care_cascade_report":
            continue
        if row.get("region") not in VALID_REGIONS:
            continue
        if row.get("subgroup"):
            continue
        if row.get("metric_type") not in metrics:
            continue
        period = row.get("period_label") or ""
        if not period:
            continue
        filename = row.get("filename") or ""
        key = (period, filename)
        value = parse_float(row.get("value"))
        if value is None:
            continue
        grouped[key][row["metric_type"]] += value
        row_counts[key] += 1

    selected: dict[str, dict] = {}
    by_period: dict[str, list[tuple[str, dict[str, float], int]]] = defaultdict(list)
    for (period, filename), sums in grouped.items():
        by_period[period].append((filename, sums, row_counts[(period, filename)]))

    for period, candidates in by_period.items():
        candidates.sort(
            key=lambda item: (
                -item[2],
                -(item[1].get("estimated_plhiv") or 0.0),
                item[0],
            )
        )
        filename, sums, count = candidates[0]
        selected[period] = {
            "filename": filename,
            "row_count": count,
            **{metric: round(sums.get(metric, 0.0), 1) for metric in metrics},
        }
    return selected


def normalize_region_label(label: str) -> str | None:
    token = (label or "").strip()
    if not token:
        return None
    upper = token.upper().replace(" ", "")
    aliases = {
        "CARAGA": "CARAGA",
        "CAR": "CAR",
        "NCR": "NCR",
        "NIR": "NIR",
        "ARMM": "ARMM",
        "BARMM": "BARMM",
        "R4A": "4A",
    }
    if upper in aliases:
        return aliases[upper]
    if upper in VALID_REGIONS:
        return upper
    return None


def parse_treatment_table_for_file(filename: str) -> dict[str, float] | None:
    if not filename or not filename.lower().endswith(".pdf"):
        return None
    markdown_path = PROCESSED_MD_DIR / filename.replace(".pdf", ".md")
    if not markdown_path.exists():
        return None
    lines = markdown_path.read_text(encoding="utf-8").splitlines()
    try:
        start = next(
            index for index, line in enumerate(lines)
            if "Viral Load Status among PLHIV on ART per region" in line
        )
    except StopIteration:
        return None

    cells: list[str] = []
    for raw_line in lines[start + 1:]:
        line = raw_line.strip()
        if not line:
            continue
        if any(pattern.match(line) for pattern in TREATMENT_TABLE_BREAK_PATTERNS):
            break
        if any(line.startswith(prefix) for prefix in TREATMENT_TABLE_HEADER_STOP):
            break
        for part in raw_line.split("|"):
            cell = part.strip()
            if not cell:
                continue
            cells.append(cell)

    rows = []
    index = 0
    while index < len(cells):
        cell = cells[index]
        if cell in TREATMENT_TABLE_SKIP_LINES or cell.startswith("(n="):
            index += 1
            continue

        region = normalize_region_label(cell)
        if not region:
            index += 1
            continue

        values: list[float] = []
        probe = index + 1
        while probe < len(cells) and len(values) < 5:
            token = cells[probe]
            if token in TREATMENT_TABLE_SKIP_LINES or token.startswith("(n="):
                probe += 1
                continue
            if any(pattern.match(token) for pattern in TREATMENT_TABLE_BREAK_PATTERNS):
                break
            if any(token.startswith(prefix) for prefix in TREATMENT_TABLE_HEADER_STOP):
                break
            number = parse_float(token.replace("%", ""))
            if number is None:
                probe += 1
                continue
            values.append(number)
            probe += 1

        if len(values) == 5:
            rows.append(
                {
                    "region": region,
                    "on_art": values[0],
                    "vl_tested": values[1],
                    "vl_tested_pct": values[2],
                    "vl_suppressed": values[3],
                    "vl_suppression_among_tested": values[4],
                }
            )
            index = probe
            continue
        index += 1

    if not rows:
        return None

    return {
        "row_count": len(rows),
        "on_art": round(sum(row["on_art"] for row in rows), 1),
        "vl_tested": round(sum(row["vl_tested"] for row in rows), 1),
        "vl_suppressed": round(sum(row["vl_suppressed"] for row in rows), 1),
    }


def pct(num: float | None, den: float | None) -> float | None:
    if num in (None, 0) or den in (None, 0):
        return None
    return round((num / den) * 100, 1)


def build_internal_consistency(observations: list[dict]) -> tuple[list[dict], list[dict]]:
    national = load_quarter_national_headline(observations)
    regional = load_quarter_regional_sums(observations)
    periods = sorted(set(national) | set(regional))

    checks: list[dict] = []
    suspect_rows: list[dict] = []

    for period in periods:
        nat = national.get(period, {})
        reg = regional.get(period, {})
        treatment_table = parse_treatment_table_for_file(
            nat.get("diagnosed_plhiv_count", {}).get("filename")
            or nat.get("plhiv_on_art_count", {}).get("filename")
            or ""
        )
        reg_first = pct(reg.get("diagnosed_plhiv"), reg.get("estimated_plhiv"))
        reg_second = pct(reg.get("on_art"), reg.get("diagnosed_plhiv"))
        reg_third = pct(reg.get("vl_suppressed"), reg.get("on_art"))
        reg_proxy = pct(reg.get("vl_suppressed"), reg.get("vl_tested"))
        tf_second = pct(treatment_table.get("on_art"), nat.get("diagnosed_plhiv_count", {}).get("value")) if treatment_table else None
        tf_third = pct(treatment_table.get("vl_suppressed"), treatment_table.get("on_art")) if treatment_table else None
        tf_proxy = pct(treatment_table.get("vl_suppressed"), treatment_table.get("vl_tested")) if treatment_table else None

        row = {
            "period": period,
            "national_filename": nat.get("diagnosed_plhiv_count", {}).get("filename") or nat.get("plhiv_on_art_count", {}).get("filename") or "",
            "regional_filename": reg.get("filename") or "",
            "treatment_table_filename": nat.get("diagnosed_plhiv_count", {}).get("filename") or nat.get("plhiv_on_art_count", {}).get("filename") or "",
            "national_diagnosed_count": nat.get("diagnosed_plhiv_count", {}).get("value"),
            "regional_diagnosed_sum": reg.get("diagnosed_plhiv"),
            "national_on_art_count": nat.get("plhiv_on_art_count", {}).get("value"),
            "regional_on_art_sum": reg.get("on_art"),
            "treatment_table_on_art_sum": treatment_table.get("on_art") if treatment_table else None,
            "national_vl_tested_count": nat.get("viral_load_tested_count", {}).get("value"),
            "regional_vl_tested_sum": reg.get("vl_tested"),
            "treatment_table_vl_tested_sum": treatment_table.get("vl_tested") if treatment_table else None,
            "national_vl_suppressed_count": nat.get("viral_load_suppressed_count", {}).get("value"),
            "regional_vl_suppressed_sum": reg.get("vl_suppressed"),
            "treatment_table_vl_suppressed_sum": treatment_table.get("vl_suppressed") if treatment_table else None,
            "national_first_95": nat.get("diagnosed_plhiv_pct", {}).get("value"),
            "regional_first_95": reg_first,
            "national_second_95": nat.get("plhiv_on_art_pct", {}).get("value"),
            "regional_second_95": reg_second,
            "treatment_table_second_95": tf_second,
            "national_third_95": nat.get("suppression_among_on_art_pct", {}).get("value"),
            "regional_third_95": reg_third,
            "treatment_table_third_95": tf_third,
            "national_proxy_suppressed_among_tested": nat.get("viral_load_suppressed_pct", {}).get("value"),
            "regional_proxy_suppressed_among_tested": reg_proxy,
            "treatment_table_proxy_suppressed_among_tested": tf_proxy,
        }
        checks.append(row)

        for metric, left_key, right_key, threshold in (
            ("on ART count", "national_on_art_count", "treatment_table_on_art_sum", 0.005),
            ("VL tested count", "national_vl_tested_count", "treatment_table_vl_tested_sum", 0.005),
            ("VL suppressed count", "national_vl_suppressed_count", "treatment_table_vl_suppressed_sum", 0.005),
        ):
            left = row.get(left_key)
            right = row.get(right_key)
            if left and right:
                rel = abs(left - right) / left
                if rel > threshold:
                    suspect_rows.append(
                        {
                            "period": period,
                            "issue_type": "national_vs_treatment_table_count_mismatch",
                            "metric": metric,
                            "national_value": left,
                            "regional_value": right,
                            "relative_difference": round(rel, 4),
                            "national_filename": row["national_filename"],
                            "regional_filename": row["treatment_table_filename"],
                        }
                    )

        for metric, left_key, right_key, threshold_pp in (
            ("1st 95", "national_first_95", "regional_first_95", 1.0),
            ("2nd 95", "national_second_95", "treatment_table_second_95", 1.0),
            ("3rd 95", "national_third_95", "treatment_table_third_95", 1.0),
            ("suppressed among tested", "national_proxy_suppressed_among_tested", "treatment_table_proxy_suppressed_among_tested", 1.0),
        ):
            left = row.get(left_key)
            right = row.get(right_key)
            if left is not None and right is not None and abs(left - right) > threshold_pp:
                suspect_rows.append(
                    {
                        "period": period,
                        "issue_type": "national_percent_mismatch",
                        "metric": metric,
                        "national_value": left,
                        "regional_value": right,
                        "difference_pp": round(left - right, 1),
                        "national_filename": row["national_filename"],
                        "regional_filename": (
                            row["regional_filename"] if metric in {"1st 95", "3rd 95"} else row["treatment_table_filename"]
                        ),
                    }
                )

    # Direct bad-row screening for headline metrics.
    for row in observations:
        if row.get("document_type") != "surveillance_report":
            continue
        if row.get("region") != "Philippines":
            continue
        if row.get("subgroup"):
            continue
        metric = row.get("metric_type") or ""
        if metric not in {"plhiv_on_art_pct", "viral_load_suppressed_pct", "viral_load_tested_pct", "suppression_among_on_art_pct"}:
            continue
        value = parse_float(row.get("value"))
        if value is None:
            continue
        period = row.get("period_label") or ""
        page_index = str(row.get("page_index") or "")
        if metric.endswith("_pct") and (value < 5.0 or value > 100.0):
            suspect_rows.append(
                {
                    "period": period,
                    "issue_type": "implausible_headline_percent",
                    "metric": metric,
                    "national_value": value,
                    "regional_value": "",
                    "difference_pp": "",
                    "national_filename": row.get("filename") or "",
                    "regional_filename": "",
                    "page_index": page_index,
                }
            )
        if page_index != "0":
            suspect_rows.append(
                {
                    "period": period,
                    "issue_type": "nonheadline_national_row_competing_with_headline",
                    "metric": metric,
                    "national_value": value,
                    "regional_value": "",
                    "difference_pp": "",
                    "national_filename": row.get("filename") or "",
                    "regional_filename": "",
                    "page_index": page_index,
                }
            )

    return checks, suspect_rows


def load_primary_ship_sources(claims: list[dict]) -> list[dict]:
    seen = set()
    rows = []
    for row in claims:
        filename = row.get("filename") or ""
        source_url = row.get("source_url") or ""
        if filename not in PRIMARY_SHIP_FILENAMES or not source_url:
            continue
        key = (filename, source_url)
        if key in seen:
            continue
        seen.add(key)
        rows.append({"filename": filename, "source_url": source_url})
    rows.sort(key=lambda row: row["filename"])
    return rows


def write_inventory_markdown(path: Path, ship_sources: list[dict]) -> None:
    lines = [
        "# Internet source inventory for Philippines HIV audit",
        "",
        "## Annual official datasets",
        "",
        f"- [UNAIDS Data API / dataset]({UNAIDS_DATASET_URL})",
        "- [World Bank prevalence of HIV, total (% of population ages 15-49)](https://data.worldbank.org/indicator/SH.DYN.AIDS.ZS?locations=PH)",
        "- [World Bank incidence of HIV, ages 15-49](https://data.worldbank.org/indicator/SH.HIV.INCD.ZS?locations=PH)",
        "- [World Bank new HIV infections](https://data.worldbank.org/indicator/SH.HIV.INCD?locations=PH)",
        "- [World Bank ART coverage](https://data.worldbank.org/indicator/SH.HIV.ARTC.ZS?locations=PH)",
        "",
        "## WHO official references",
        "",
        f"- [WHO Philippines / UNAIDS joint news release, 11 June 2025]({WHO_2025_ARTICLE_URL})",
        f"- [WHO Philippines HIV country profile PDF, 2016]({WHO_COUNTRY_PROFILE_URL})",
        "",
        "## Official SHIP / Philippines quarterly surveillance reports used for local quarter-end cross-checks",
        "",
    ]
    for row in ship_sources:
        lines.append(f"- [{row['filename']}]({row['source_url']})")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    observations = read_csv(OBSERVATIONS_CSV)
    claims = read_csv(CLAIMS_CSV)

    world_bank_rows = fetch_world_bank_series()
    checks, suspect_rows = build_internal_consistency(observations)
    ship_sources = load_primary_ship_sources(claims)

    write_csv(AUDIT_DIR / "world_bank_philippines_hiv.csv", world_bank_rows)
    write_csv(AUDIT_DIR / "local_narrative_vs_regional_sums.csv", checks)
    write_csv(AUDIT_DIR / "suspect_local_metrics.csv", suspect_rows)
    write_inventory_markdown(AUDIT_DIR / "internet_source_inventory.md", ship_sources)

    summary = {
        "world_bank_series_rows": len(world_bank_rows),
        "consistency_rows": len(checks),
        "suspect_local_rows": len(suspect_rows),
        "ship_sources": ship_sources,
    }
    (AUDIT_DIR / "systematic_audit_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote systematic audit outputs to {AUDIT_DIR}")


if __name__ == "__main__":
    main()
