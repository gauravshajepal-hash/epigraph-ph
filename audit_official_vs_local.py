from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
NORMALIZED_DIR = ROOT / "data" / "normalized"
OBSERVATIONS_CSV = NORMALIZED_DIR / "observations.csv"
UNAIDS_CSV = ROOT / "tmp" / "unaids_audit" / "unzipped" / "Estimates_2025_en.csv"
OUTPUT_DIR = NORMALIZED_DIR / "audit"

UNAIDS_DATASET_URL = "https://aidsinfo.unaids.org/dataset"
SHIP_Q4_2025_URL = "https://www.ship.ph/wp-content/uploads/2026/02/2025_Q4-HIV-AIDS-Surveillance-Report-of-the-Philippines-2.pdf"

VALID_REGIONS = {
    "NCR", "CAR", "CARAGA", "BARMM", "ARMM", "NIR",
    "1", "2", "3", "4A", "4B", "5", "6", "7", "8", "9", "10", "11", "12",
}

HEADLINE_METRICS = {
    "estimated_plhiv",
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

UNAIDS_INDICATORS = {
    "PLWH": "People living with HIV",
    "NEW_INFECTIONS": "New HIV infections",
    "AIDS_DEATHS": "AIDS-related deaths",
    "PLHIV_KNOWLEDGE_OF_STATUS": "1st 95 official",
    "PERCENT_KNOW_STATUS_ON_ART": "2nd 95 official",
    "PERCENT_PLWH": "ART coverage among all PLHIV",
    "PERCENT_ON_ART_VL_SUPPRESSED": "3rd 95 official",
    "VIRAL_LOAD_SUPPRESSION": "Suppressed viral load among all PLHIV",
}


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def parse_float(value: str | None) -> float | None:
    if value in (None, "", "..."):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    parsed = parse_float(value)
    if parsed is None:
        return None
    return int(round(parsed))


def load_unaids_philippines() -> dict[str, list[dict]]:
    rows = read_csv(UNAIDS_CSV)
    out: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        if row.get("Area") != "Philippines":
            continue
        if row.get("Subgroup") != "All ages estimate":
            continue
        gid = row.get("Indicator_GId") or ""
        if gid not in UNAIDS_INDICATORS:
            continue
        out[gid].append(
            {
                "year": int(row["Time Period"]),
                "indicator": UNAIDS_INDICATORS[gid],
                "indicator_gid": gid,
                "value": parse_float(row.get("Data value")),
                "formatted": (row.get("Formatted") or "").strip(),
                "unit": row.get("Unit") or "",
                "source": row.get("Source") or "",
                "footnote": row.get("Footnote") or "",
            }
        )
    for gid in out:
        out[gid].sort(key=lambda row: row["year"])
    return out


def build_local_regional_cascade(observations: list[dict]) -> list[dict]:
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in observations:
        if row.get("document_type") != "care_cascade_report":
            continue
        if row.get("region") not in VALID_REGIONS:
            continue
        period = row.get("period_label") or ""
        metric = row.get("metric_type") or ""
        if not period or metric not in {"estimated_plhiv", "diagnosed_plhiv", "on_art", "vl_tested", "vl_suppressed"}:
            continue
        value = parse_float(row.get("value"))
        if value is None:
            continue
        grouped[period][metric] += value

    series = []
    for period, bucket in grouped.items():
        estimated = bucket.get("estimated_plhiv")
        diagnosed = bucket.get("diagnosed_plhiv")
        on_art = bucket.get("on_art")
        vl_tested = bucket.get("vl_tested")
        vl_suppressed = bucket.get("vl_suppressed")

        def pct(num: float | None, den: float | None) -> float | None:
            if not num or not den:
                return None
            return round((num / den) * 100, 1)

        series.append(
            {
                "period": period,
                "year": int(period[:4]) if period[:4].isdigit() else None,
                "estimated_plhiv": round(estimated, 1) if estimated else None,
                "diagnosed_plhiv": round(diagnosed, 1) if diagnosed else None,
                "on_art": round(on_art, 1) if on_art else None,
                "vl_tested": round(vl_tested, 1) if vl_tested else None,
                "vl_suppressed": round(vl_suppressed, 1) if vl_suppressed else None,
                "first_95_local": pct(diagnosed, estimated),
                "second_95_local": pct(on_art, diagnosed),
                "art_coverage_local": pct(on_art, estimated),
                "third_95_local": pct(vl_suppressed, on_art),
                "suppression_among_tested_local": pct(vl_suppressed, vl_tested),
            }
        )
    series.sort(key=lambda row: row["period"])
    return series


def build_local_national_headline_series(observations: list[dict]) -> list[dict]:
    bucket: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in observations:
        if row.get("document_type") != "surveillance_report":
            continue
        if row.get("region") != "Philippines":
            continue
        if str(row.get("page_index") or "") != "0":
            continue
        metric = row.get("metric_type") or ""
        period = row.get("period_label") or ""
        if not period or metric not in HEADLINE_METRICS:
            continue
        bucket[(period, metric)].append(row)

    selected: dict[str, dict[str, float | str | None]] = defaultdict(dict)
    for (period, metric), rows in bucket.items():
        rows.sort(
            key=lambda row: (
                -(parse_float(row.get("value")) or 0.0),
                row.get("filename") or "",
            )
        )
        row = rows[0]
        selected[period][metric] = parse_float(row.get("value"))
        selected[period][f"{metric}_filename"] = row.get("filename") or ""

    series = []
    for period, row in selected.items():
        estimated = row.get("estimated_plhiv")
        diagnosed = row.get("diagnosed_plhiv_count")
        on_art = row.get("plhiv_on_art_count")
        vl_tested = row.get("viral_load_tested_count")
        vl_suppressed = row.get("viral_load_suppressed_count")

        def pct(num: float | None, den: float | None) -> float | None:
            if not num or not den:
                return None
            return round((num / den) * 100, 1)

        series.append(
            {
                "period": period,
                "year": int(period[:4]) if period[:4].isdigit() else None,
                "estimated_plhiv": estimated,
                "diagnosed_plhiv": diagnosed,
                "on_art": on_art,
                "vl_tested": vl_tested,
                "vl_suppressed": vl_suppressed,
                "first_95_local": row.get("diagnosed_plhiv_pct") or pct(diagnosed, estimated),
                "second_95_local": row.get("plhiv_on_art_pct") or pct(on_art, diagnosed),
                "art_coverage_local": pct(on_art, estimated),
                "third_95_local": row.get("suppression_among_on_art_pct") or pct(vl_suppressed, on_art),
                "suppression_among_tested_local": row.get("viral_load_suppressed_pct") or pct(vl_suppressed, vl_tested),
            }
        )
    series.sort(key=lambda row: row["period"])
    return series


def latest_period_for_year(local_series: list[dict], year: int) -> dict | None:
    rows = [row for row in local_series if row.get("year") == year]
    if not rows:
        return None
    return sorted(rows, key=lambda row: row["period"])[-1]


def build_comparison(unaids: dict[str, list[dict]], local_series: list[dict]) -> list[dict]:
    official_by_year = defaultdict(dict)
    for gid, rows in unaids.items():
        for row in rows:
            official_by_year[row["year"]][gid] = row

    comparisons = []
    for year, official in sorted(official_by_year.items()):
        local = latest_period_for_year(local_series, year)
        if not local:
            continue

        def add(metric_label: str, official_gid: str, local_key: str):
            official_row = official.get(official_gid)
            official_value = official_row.get("value") if official_row else None
            local_value = local.get(local_key)
            comparisons.append(
                {
                    "year": year,
                    "local_period": local["period"],
                    "metric": metric_label,
                    "official_indicator_gid": official_gid,
                    "official_value": official_value,
                    "local_value": local_value,
                    "difference_local_minus_official": (
                        round(local_value - official_value, 1)
                        if official_value is not None and local_value is not None
                        else None
                    ),
                }
            )

        add("1st 95", "PLHIV_KNOWLEDGE_OF_STATUS", "first_95_local")
        add("2nd 95", "PERCENT_KNOW_STATUS_ON_ART", "second_95_local")
        add("ART coverage among all PLHIV", "PERCENT_PLWH", "art_coverage_local")
        add("3rd 95", "PERCENT_ON_ART_VL_SUPPRESSED", "third_95_local")
        add("Suppressed among all PLHIV", "VIRAL_LOAD_SUPPRESSION", "third_95_local")
    return comparisons


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(
    path: Path,
    unaids: dict[str, list[dict]],
    local_series: list[dict],
    local_headline_series: list[dict],
    comparisons: list[dict],
) -> None:
    q4_2024 = latest_period_for_year(local_series, 2024)
    q4_2025 = latest_period_for_year(local_headline_series, 2025)

    def fmt(value: float | int | None, spec: str) -> str:
        if value in (None, ""):
            return ""
        return format(value, spec)

    def fmt_pct(value: float | int | None) -> str:
        if value in (None, ""):
            return ""
        return f"{value:.1f}%"

    lines = [
        "# Philippines HIV audit: official UNAIDS vs local normalized series",
        "",
        f"- Official dataset: {UNAIDS_DATASET_URL}",
        f"- Official 2025 Q4 surveillance source used for the local cascade: {SHIP_Q4_2025_URL}",
        "",
        "## Headline finding",
        "",
        "- The local dashboard was previously wrong on the third 95. It was using suppression among those tested for viral load, not suppression among PLHIV on ART.",
        f"- Correct local third-95 value from the national headline/treatment-facility series is {q4_2025['third_95_local']:.1f}% at {q4_2025['period']}." if q4_2025 and q4_2025.get("third_95_local") is not None else "- Correct local third-95 value could not be derived.",
        f"- The proxy series it had been showing was {q4_2025['suppression_among_tested_local']:.1f}% at {q4_2025['period']}." if q4_2025 and q4_2025.get("suppression_among_tested_local") is not None else "",
        "",
        "## 2024 official vs local aligned metrics",
        "",
        "| Metric | Official 2024 | Local latest 2024 period | Local value | Difference |",
        "|---|---:|---|---:|---:|",
    ]

    for row in comparisons:
        if row["year"] != 2024:
            continue
        official = "" if row["official_value"] is None else f"{row['official_value']:.1f}"
        local = "" if row["local_value"] is None else f"{row['local_value']:.1f}"
        diff = "" if row["difference_local_minus_official"] is None else f"{row['difference_local_minus_official']:+.1f}"
        lines.append(f"| {row['metric']} | {official} | {row['local_period']} | {local} | {diff} |")

    lines.extend(
        [
            "",
            "## Official UNAIDS annual series available for the Philippines",
            "",
            "| Indicator | Coverage | Latest value |",
            "|---|---|---:|",
        ]
    )
    for gid, label in UNAIDS_INDICATORS.items():
        rows = unaids.get(gid) or []
        if not rows:
            continue
        first_year = rows[0]["year"]
        last_year = rows[-1]["year"]
        latest = rows[-1]["formatted"] or "..."
        lines.append(f"| {label} | {first_year}-{last_year} | {latest} |")

    lines.extend(
        [
            "",
            "## Local annualized quarter-end cascade snapshots",
            "",
            "| Period | Estimated PLHIV | Diagnosed | On ART | VL tested | VL suppressed | 1st 95 | 2nd 95 | 3rd 95 | Suppressed among tested |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in local_headline_series:
        if row["period"] not in {"2024 Q4", "2025 Q4"}:
            continue
        lines.append(
            f"| {row['period']} | {fmt(row.get('estimated_plhiv'), '.0f')} | {fmt(row.get('diagnosed_plhiv'), '.0f')} | "
            f"{fmt(row.get('on_art'), '.0f')} | {fmt(row.get('vl_tested'), '.0f')} | {fmt(row.get('vl_suppressed'), '.0f')} | "
            f"{fmt_pct(row.get('first_95_local'))} | {fmt_pct(row.get('second_95_local'))} | "
            f"{fmt_pct(row.get('third_95_local'))} | {fmt_pct(row.get('suppression_among_tested_local'))} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The residence-based local quarter-end care-cascade sums are close to the official UNAIDS 2024 annual first-95, second-95, and ART-coverage values.",
            "- The main error found in the local dashboard was denominator selection for the third 95.",
            "- The dashboard third-95 should use the national headline/treatment-facility basis, not the residence-based regional annex basis.",
            "- The official UNAIDS 2025 dataset has blank Philippines values for `PERCENT_ON_ART_VL_SUPPRESSED` and `VIRAL_LOAD_SUPPRESSION`, so it cannot directly validate the third-95 series there.",
            "- For 2025, the authoritative source currently available in this workspace is the official SHIP quarterly surveillance report. That source supports ~57% for suppression among PLHIV on ART and ~97% for suppression among those tested.",
        ]
    )

    path.write_text("\n".join(line for line in lines if line is not None), encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    observations = read_csv(OBSERVATIONS_CSV)
    unaids = load_unaids_philippines()
    local_series = build_local_regional_cascade(observations)
    local_headline_series = build_local_national_headline_series(observations)
    comparisons = build_comparison(unaids, local_series)

    write_csv(OUTPUT_DIR / "unaids_philippines_all_ages.csv", [
        {
            "year": row["year"],
            "indicator": row["indicator"],
            "indicator_gid": row["indicator_gid"],
            "value": row["value"],
            "formatted": row["formatted"],
            "unit": row["unit"],
            "source": row["source"],
            "footnote": row["footnote"],
        }
        for rows in unaids.values()
        for row in rows
    ])
    write_csv(OUTPUT_DIR / "local_regional_cascade_sums.csv", local_series)
    write_csv(OUTPUT_DIR / "local_national_headline_series.csv", local_headline_series)
    write_csv(OUTPUT_DIR / "official_vs_local_comparison.csv", comparisons)

    summary = {
        "official_dataset": UNAIDS_DATASET_URL,
        "local_q4_2025_source": SHIP_Q4_2025_URL,
        "local_q4_2024_residence_basis": latest_period_for_year(local_series, 2024),
        "local_q4_2025_headline_basis": latest_period_for_year(local_headline_series, 2025),
        "comparison_rows": len(comparisons),
    }
    (OUTPUT_DIR / "audit_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_markdown(OUTPUT_DIR / "audit_report.md", unaids, local_series, local_headline_series, comparisons)
    print(f"Wrote audit outputs to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
