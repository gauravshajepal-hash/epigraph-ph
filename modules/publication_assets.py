from __future__ import annotations

import csv
import json
import math
import os
import re
import shutil
import subprocess
import textwrap
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
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
VALID_REGION_NAMES = set(REGION_ALIASES.values())
PUBLICATION_START_YEAR = 2015
PUBLICATION_END_YEAR = 2025


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


def _year_ticks(start: int = PUBLICATION_START_YEAR, end: int = PUBLICATION_END_YEAR) -> list[int]:
    return [year for year in range(start, end + 1) if year in {start, 2015, 2020, end}]


def _complete_annual_series(points: list[dict], start: int = PUBLICATION_START_YEAR, end: int = PUBLICATION_END_YEAR) -> tuple[list[int], np.ndarray, list[int], list[float]]:
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


def _inverse_gamma_sample(rng: np.random.Generator, shape: float, rate: float) -> float:
    safe_shape = max(float(shape), 1e-6)
    safe_rate = max(float(rate), 1e-9)
    gamma_draw = float(rng.gamma(shape=safe_shape, scale=1.0 / safe_rate))
    return 1.0 / max(gamma_draw, 1e-12)


def _bayesian_regression_mcmc(
    y: np.ndarray,
    X: np.ndarray,
    *,
    draws: int,
    burnin: int,
    thin: int,
    rng: np.random.Generator,
    prior_sd: float = 0.35,
    a0: float = 2.5,
    b0: float = 0.02,
) -> dict:
    y = np.asarray(y, dtype=float)
    X = np.asarray(X, dtype=float)
    if y.ndim != 1 or X.ndim != 2 or X.shape[0] != y.shape[0]:
        raise ValueError("Bayesian regression requires 1D y and 2D X with matching rows.")
    n_obs, n_coef = X.shape
    prior_var = float(prior_sd) ** 2
    prior_precision = np.eye(n_coef, dtype=float) / prior_var
    xtx = X.T @ X
    xty = X.T @ y

    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    resid = y - X @ beta
    sigma2 = max(float(np.var(resid, ddof=max(n_coef, 1))), 0.0025)

    total_iterations = burnin + draws * thin
    beta_draws: list[np.ndarray] = []
    sigma_draws: list[float] = []
    for iteration in range(total_iterations):
        precision = xtx / sigma2 + prior_precision
        covariance = np.linalg.inv(precision + np.eye(n_coef) * 1e-9)
        mean = covariance @ (xty / sigma2)
        beta = rng.multivariate_normal(mean, covariance)
        resid = y - X @ beta
        sigma2 = _inverse_gamma_sample(
            rng,
            a0 + n_obs / 2.0,
            b0 + 0.5 * float(resid.T @ resid),
        )
        if iteration >= burnin and (iteration - burnin) % thin == 0:
            beta_draws.append(beta.copy())
            sigma_draws.append(float(sigma2))

    beta_samples = np.asarray(beta_draws, dtype=float)
    sigma_samples = np.asarray(sigma_draws, dtype=float)
    beta_mean = np.mean(beta_samples, axis=0)
    fitted = X @ beta_mean
    residuals = y - fitted
    return {
        "beta_draws": beta_samples,
        "sigma_draws": sigma_samples,
        "beta_mean": beta_mean,
        "sigma_mean": float(np.mean(sigma_samples)),
        "fitted": fitted,
        "residuals": residuals,
    }


def _hierarchical_step_mcmc(
    step_observations: dict[str, float],
    *,
    draws: int,
    burnin: int,
    thin: int,
    rng: np.random.Generator,
    mu_prior_sd: float = 4.0,
    a_sigma: float = 2.5,
    b_sigma: float = 6.0,
    a_tau: float = 2.5,
    b_tau: float = 6.0,
) -> dict:
    regions = list(step_observations)
    if not regions:
        zeros = np.zeros(draws, dtype=float)
        return {
            "regions": regions,
            "mu_draws": zeros,
            "sigma2_draws": np.full(draws, 9.0, dtype=float),
            "tau2_draws": np.full(draws, 4.0, dtype=float),
            "theta_draws": {},
            "mu_mean": 0.0,
            "sigma_mean": 3.0,
            "tau_mean": 2.0,
        }

    y = np.array([float(step_observations[region]) for region in regions], dtype=float)
    n_regions = len(regions)
    theta = y.copy()
    mu = float(np.mean(y))
    sigma2 = max(float(np.var(y, ddof=0)), 4.0)
    tau2 = max(float(np.var(y, ddof=0) / 2.0), 2.0)
    total_iterations = burnin + draws * thin

    mu_draws: list[float] = []
    sigma_draws: list[float] = []
    tau_draws: list[float] = []
    theta_draws: dict[str, list[float]] = {region: [] for region in regions}

    for iteration in range(total_iterations):
        theta_variance = 1.0 / ((1.0 / sigma2) + (1.0 / tau2))
        theta_sd = math.sqrt(theta_variance)
        for index, region in enumerate(regions):
            theta_mean = theta_variance * ((y[index] / sigma2) + (mu / tau2))
            theta[index] = float(rng.normal(theta_mean, theta_sd))

        mu_variance = 1.0 / ((n_regions / tau2) + (1.0 / (mu_prior_sd ** 2)))
        mu_mean = mu_variance * (float(np.sum(theta)) / tau2)
        mu = float(rng.normal(mu_mean, math.sqrt(mu_variance)))

        sigma2 = _inverse_gamma_sample(
            rng,
            a_sigma + n_regions / 2.0,
            b_sigma + 0.5 * float(np.sum((y - theta) ** 2)),
        )
        tau2 = _inverse_gamma_sample(
            rng,
            a_tau + n_regions / 2.0,
            b_tau + 0.5 * float(np.sum((theta - mu) ** 2)),
        )

        if iteration >= burnin and (iteration - burnin) % thin == 0:
            mu_draws.append(mu)
            sigma_draws.append(float(sigma2))
            tau_draws.append(float(tau2))
            for index, region in enumerate(regions):
                theta_draws[region].append(float(theta[index]))

    mu_array = np.asarray(mu_draws, dtype=float)
    sigma_array = np.asarray(sigma_draws, dtype=float)
    tau_array = np.asarray(tau_draws, dtype=float)
    return {
        "regions": regions,
        "mu_draws": mu_array,
        "sigma2_draws": sigma_array,
        "tau2_draws": tau_array,
        "theta_draws": {region: np.asarray(values, dtype=float) for region, values in theta_draws.items()},
        "mu_mean": float(np.mean(mu_array)) if mu_array.size else 0.0,
        "sigma_mean": math.sqrt(float(np.mean(sigma_array))) if sigma_array.size else 3.0,
        "tau_mean": math.sqrt(float(np.mean(tau_array))) if tau_array.size else 2.0,
    }


def _shrink_correlation(residual_matrix: np.ndarray, shrinkage: float = 0.35) -> np.ndarray:
    residual_matrix = np.asarray(residual_matrix, dtype=float)
    if residual_matrix.ndim != 2 or residual_matrix.shape[1] == 0:
        return np.eye(1, dtype=float)
    if residual_matrix.shape[0] < 2:
        return np.eye(residual_matrix.shape[1], dtype=float)
    corr = np.corrcoef(residual_matrix, rowvar=False)
    corr = np.nan_to_num(corr, nan=0.0)
    np.fill_diagonal(corr, 1.0)
    corr = shrinkage * np.eye(corr.shape[0], dtype=float) + (1.0 - shrinkage) * corr
    corr = (corr + corr.T) / 2.0
    eigenvalues, eigenvectors = np.linalg.eigh(corr)
    eigenvalues[eigenvalues < 1e-6] = 1e-6
    corr = eigenvectors @ np.diag(eigenvalues) @ eigenvectors.T
    scale = np.sqrt(np.diag(corr))
    corr = corr / np.outer(scale, scale)
    np.fill_diagonal(corr, 1.0)
    return corr


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
    figure_pdf_path: str


class PublicationAssetBuilder:
    def __init__(self, base_dir: str | Path | None = None):
        self.base_dir = Path(base_dir or Path(__file__).resolve().parents[1])
        self.normalized_dir = self.base_dir / "data" / "normalized"
        self.audit_dir = self.normalized_dir / "audit"
        self.processed_dir = self.base_dir / "data" / "processed_md"
        self.figure_dir = self.normalized_dir / "publication_figures"
        self.report_dir = self.figure_dir / "reports"
        self.output_pdf_dir = self.base_dir / "output" / "pdf"
        self.asset_path = self.normalized_dir / "publication_assets.json"
        self.r_script_path = self.base_dir / "scripts" / "render_publication_figures.R"

    def build(self) -> dict:
        self.figure_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.output_pdf_dir.mkdir(parents=True, exist_ok=True)
        dashboard = self._read_json(self.normalized_dir / "dashboard_feed.json")
        observations = self._read_observations()
        filename_source_map = self._build_filename_source_map(observations)

        series = {
            "national_cascade": self._build_national_cascade(dashboard),
            "regional_ladder": self._build_regional_ladder(dashboard),
            "anomalies": self._build_anomalies(dashboard, observations),
            "historical": self._build_historical_series(observations, filename_source_map),
            "key_populations": self._build_key_population_series(observations, filename_source_map),
            "regional_yearly": self._build_regional_yearly_series(observations),
        }
        series["anomaly_yearly"] = self._build_yearly_anomaly_series(
            series["regional_yearly"],
            observations,
            filename_source_map,
        )
        series["experimental_regional"] = self._build_experimental_regional_series(
            series["national_cascade"],
            series["regional_yearly"],
            series["anomaly_yearly"],
        )
        methodology = self._build_methodology(series)
        references = self._build_references(series)
        r_figures = self._render_r_publication_figures(series)

        figures = {
            "national_cascade": r_figures.get("national_cascade") or self._render_national_cascade(series["national_cascade"]),
            "regional_ladder": r_figures.get("regional_ladder") or self._render_regional_ladder(series["regional_ladder"]),
            "anomaly_board": r_figures.get("anomaly_board") or self._render_anomaly_board(series["anomalies"]),
            "historical_board": r_figures.get("historical_board") or self._render_historical_board(series["historical"]),
            "key_populations_board": r_figures.get("key_populations_board") or self._render_key_population_board(series["key_populations"]),
        }

        figure_payload = {}
        for key, figure in figures.items():
            report_pdf_path = self._write_pdf_report(
                key,
                figure,
                methodology.get("by_figure", {}).get(key, {}),
                references,
            )
            figure_payload[key] = {
                "title": figure.title,
                "note": figure.note,
                "svg": figure.svg,
                "svg_path": figure.svg_path,
                "png_path": figure.png_path,
                "figure_pdf_path": figure.figure_pdf_path,
                "pdf_path": report_pdf_path,
            }

        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "series": series,
            "figures": figure_payload,
            "methodology": methodology,
            "references": references,
        }
        self.asset_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    def _r_libs_user(self) -> Path:
        return Path.home() / "Documents" / "R" / "win-library" / "4.5"

    def _find_rscript(self) -> Path | None:
        candidates = []
        env_path = os.environ.get("RSCRIPT_PATH")
        if env_path:
            path = Path(env_path)
            if path.exists():
                return path
        for root in (Path("C:/Program Files/R"), Path("C:/Program Files (x86)/R")):
            if not root.exists():
                continue
            candidates.extend(root.glob("R-*/bin/x64/Rscript.exe"))
            candidates.extend(root.glob("R-*/bin/Rscript.exe"))
        if not candidates:
            return None
        candidates = sorted(candidates, key=lambda item: item.as_posix())
        return candidates[-1]

    def _figure_from_existing(self, basename: str, title: str, note: str) -> PublicationFigure | None:
        svg_path = self.figure_dir / f"{basename}.svg"
        png_path = self.figure_dir / f"{basename}.png"
        pdf_path = self.figure_dir / f"{basename}.pdf"
        if not (svg_path.exists() and png_path.exists() and pdf_path.exists()):
            return None
        return PublicationFigure(
            title=title,
            note=note,
            svg=svg_path.read_text(encoding="utf-8"),
            svg_path=str(svg_path.relative_to(self.base_dir)).replace("\\", "/"),
            png_path=str(png_path.relative_to(self.base_dir)).replace("\\", "/"),
            figure_pdf_path=str(pdf_path.relative_to(self.base_dir)).replace("\\", "/"),
        )

    def _render_r_publication_figures(self, series: dict) -> dict[str, PublicationFigure]:
        rscript = self._find_rscript()
        if rscript is None or not self.r_script_path.exists():
            return {}

        payload_path = self.normalized_dir / "publication_r_input.json"
        payload = {"series": series}
        payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        env = os.environ.copy()
        env["R_LIBS_USER"] = str(self._r_libs_user())
        figures: dict[str, PublicationFigure] = {}
        result = subprocess.run(
            [str(rscript), str(self.r_script_path), str(payload_path), str(self.figure_dir)],
            check=True,
            cwd=str(self.base_dir),
            env=env,
            capture_output=True,
            text=True,
        )
        print("R output:", result.stdout)
        print("R error:", result.stderr)

        configs = {
            "national_cascade": ("national_cascade_board", "National 95-95-95 board", "The board combines a compact target-position strip, an observed 2018-2025 year-end trajectory from the official DOH/HARP table, and the observed 2025 stage counts."),
            "regional_ladder": ("regional_stage_matrix", "Regional stage matrix", "The publication figure is an ordered yearly regional stage matrix. It shows the latest observed regional diagnosis, treatment, and suppression values directly without a second gap companion chart."),
            "anomaly_board": ("anomaly_board", "Performance versus treatment burden", "The publication figure uses a single performance-versus-burden quadrant so the residual and leakage stories stay in one compact view."),
            "historical_board": ("historical_board", "Historical burden indicators", "Historical panels are rendered from observed annual values only. No interpolation is applied across missing years."),
            "key_populations_board": ("key_populations_board", "Key population sentinel panels", "Key-population series use a shared 2015-2025 presentation window while keeping gaps visible where no defensible observed value exists."),
        }
        for key, (basename, title, note) in configs.items():
            figure = self._figure_from_existing(basename, title, note)
            if figure is not None:
                figures[key] = figure
        return figures

    def _read_json(self, path: Path) -> dict:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _read_observations(self) -> list[dict]:
        path = self.normalized_dir / "observations.csv"
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def _build_filename_source_map(self, observations: list[dict]) -> dict[str, str]:
        source_map: dict[str, str] = {}
        for row in observations:
            filename = str(row.get("filename") or "").strip()
            source_url = str(row.get("source_url") or "").strip()
            if filename and source_url and filename not in source_map:
                source_map[filename] = source_url
                source_map.setdefault(f"{Path(filename).stem}.md", source_url)
        return source_map

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

    def _read_harp_annual_cascade_series(self) -> dict:
        path = self.normalized_dir / "official_harp_95_2018_2025.csv"
        rows = {
            "estimated_plhiv": [],
            "diagnosed_plhiv": [],
            "first_95": [],
            "plhiv_on_art": [],
            "second_95": [],
            "vl_tested_on_art": [],
            "suppressed_count": [],
            "third_95": [],
            "vl_testing_coverage": [],
            "vl_suppression_among_tested": [],
        }
        if not path.exists():
            return rows
        indicator_map = {
            "estimated_plhiv": "estimated_plhiv",
            "diagnosed_plhiv": "diagnosed_plhiv",
            "first_95": "first_95",
            "plhiv_on_art": "plhiv_on_art",
            "second_95": "second_95",
            "total_vl_tested_on_art": "vl_tested_on_art",
            "virally_suppressed_among_tested": "suppressed_count",
            "third_95": "third_95",
            "vl_testing_coverage": "vl_testing_coverage",
            "vl_suppression_among_tested": "vl_suppression_among_tested",
        }
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                year = int(row.get("year") or 0)
                if not year or year < 2018:
                    continue
                for column, key in indicator_map.items():
                    value = _parse_numeric(row.get(column))
                    if value is None:
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
        official_unaids = self._read_unaids_annual_series()
        official_harp = self._read_harp_annual_cascade_series()
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
        ordered = []
        titles = {
            "first_95": "1st 95: Diagnosis coverage",
            "second_95": "2nd 95: Treatment coverage",
            "third_95": "3rd 95: Viral suppression",
        }
        official_map = {
            "first_95": official_harp.get("first_95", []),
            "second_95": official_harp.get("second_95", []),
            "third_95": official_harp.get("third_95", []),
        }
        annual_count_map = {
            "first_95": official_harp.get("diagnosed_plhiv", []),
            "second_95": official_harp.get("plhiv_on_art", []),
            "third_95": official_harp.get("suppressed_count", []),
        }
        official_labels = {
            "first_95": "Official year-end context uses the DOH/HARP accomplishment table from 2018-2025.",
            "second_95": "Official year-end context uses the DOH/HARP accomplishment table from 2018-2025.",
            "third_95": "Official year-end context uses the DOH/HARP accomplishment table from 2018-2025.",
        }
        for series_id in ("first_95", "second_95", "third_95"):
            point_rows = [{"period": p, "value": v} for p, v in patched[series_id]]
            official_rows = official_map.get(series_id, [])
            latest_value = point_rows[-1]["value"]
            latest_period = point_rows[-1]["period"]
            if official_rows:
                latest_value = float(official_rows[-1]["value"])
                latest_period = f"{official_rows[-1]['year']} year-end"
            ordered.append({
                "series_id": series_id,
                "label": titles[series_id],
                "target": 95.0,
                "points": point_rows,
                "count_points": annual_count_map.get(series_id, []),
                "latest_value": latest_value,
                "latest_period": latest_period,
                "gap_to_target": round(95.0 - latest_value, 1),
                "coverage_start": "2018 year-end",
                "coverage_end": latest_period,
                "actual_metric_type": rows.get(series_id, {}).get("actual_metric_type", ""),
                "official_annual": official_rows,
                "official_context_label": official_labels[series_id],
            })
        return {"rows": ordered, "estimated_points": official_harp.get("estimated_plhiv", [])}

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
        residual_by_region: dict[str, dict] = {}
        for chart in charts[:2]:
            fit_line = chart.get("fit_line", [])
            if len(fit_line) < 2:
                continue
            x0, y0 = fit_line[0]["x"], fit_line[0]["y"]
            x1, y1 = fit_line[-1]["x"], fit_line[-1]["y"]
            slope = 0 if x1 == x0 else (y1 - y0) / (x1 - x0)
            pair_label = "Treatment after diagnosis" if "diagnosis" in chart.get("chart_id", "") else "Suppression after treatment"
            for point in chart.get("points", []):
                region = _format_region(point["region"])
                fit_y = y0 + slope * (point["x"] - x0)
                residual_value = round(float(point["y"]) - float(fit_y), 2)
                residual = {
                    "region": region,
                    "stage": pair_label,
                    "label": f"{region} | {pair_label}",
                    "value": residual_value,
                }
                residual_rows.append(residual)
                current = residual_by_region.get(region)
                if current is None or abs(residual_value) > abs(current["value"]):
                    residual_by_region[region] = residual
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
        all_leakage_rows = sorted(leakage.values(), key=lambda row: row["ltfu"] + row["not_on_treatment"], reverse=True)
        leakage_rows = all_leakage_rows[:10]

        performance_burden_rows = []
        for row in all_leakage_rows:
            region = row["region"]
            residual = residual_by_region.get(region)
            if residual is None:
                continue
            performance_burden_rows.append({
                "region": region,
                "residual": residual["value"],
                "stage": residual["stage"],
                "alive": row["alive"],
                "ltfu": row["ltfu"],
                "not_on_treatment": row["not_on_treatment"],
                "leakage_burden": row["ltfu"] + row["not_on_treatment"],
            })

        return {
            "period_label": latest_period,
            "residual_rows": residual_rows,
            "leakage_rows": leakage_rows,
            "performance_burden_rows": performance_burden_rows,
        }

    def _build_historical_series(self, observations: list[dict], filename_source_map: dict[str, str]) -> dict:
        extracted = self._extract_markdown_series(filename_source_map)
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

    def _build_key_population_series(self, observations: list[dict], filename_source_map: dict[str, str]) -> dict:
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

        extracted = self._extract_markdown_series(filename_source_map)

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

    def _build_regional_yearly_series(self, observations: list[dict]) -> dict:
        cascade_metric_map = {
            "diagnosis_coverage": "diagnosis",
            "art_coverage": "treatment",
            "suppression_among_on_art": "suppression",
        }
        annual_cascade: dict[tuple[int, str, str], dict] = {}
        annual_burden: dict[tuple[int, str], dict] = {}
        annual_national: dict[tuple[int, str], dict] = {}

        for row in observations:
            region = _format_region(row.get("region", ""))
            year = int(row.get("year") or 0)
            if not year:
                continue
            metric_type = str(row.get("metric_type") or "")
            period_label = str(row.get("period_label") or "")
            sort_value = _period_sort_value(period_label) or (year * 100 + 12)
            confidence = float(row.get("confidence") or 0)
            value = _parse_numeric(row.get("value"))
            if value is None:
                continue

            if metric_type in cascade_metric_map:
                metric = cascade_metric_map[metric_type]
                if region in VALID_REGION_NAMES:
                    key = (year, region, metric)
                    current = annual_cascade.get(key)
                    candidate = {
                        "year": year,
                        "region": region,
                        "metric": metric,
                        "value": float(value),
                        "period_label": period_label,
                        "filename": row.get("filename", ""),
                        "source_url": row.get("source_url", ""),
                        "snippet": row.get("snippet", ""),
                        "_sort_value": sort_value,
                        "_confidence": confidence,
                    }
                    if current is None or sort_value > current["_sort_value"] or (sort_value == current["_sort_value"] and confidence > current["_confidence"]):
                        annual_cascade[key] = candidate
                elif region in {"Philippines", "National"}:
                    key = (year, metric)
                    current = annual_national.get(key)
                    candidate = {
                        "year": year,
                        "metric": metric,
                        "value": float(value),
                        "period_label": period_label,
                        "filename": row.get("filename", ""),
                        "source_url": row.get("source_url", ""),
                        "_sort_value": sort_value,
                        "_confidence": confidence,
                    }
                    if current is None or sort_value > current["_sort_value"] or (sort_value == current["_sort_value"] and confidence > current["_confidence"]):
                        annual_national[key] = candidate

            if (
                metric_type == "reported_cases_count"
                and region in VALID_REGION_NAMES
                and not str(row.get("subgroup") or "").strip()
            ):
                key = (year, region)
                current = annual_burden.get(key)
                candidate = {
                    "year": year,
                    "region": region,
                    "value": float(value),
                    "period_label": period_label,
                    "filename": row.get("filename", ""),
                    "source_url": row.get("source_url", ""),
                    "_sort_value": sort_value,
                    "_confidence": confidence,
                }
                if current is None or sort_value > current["_sort_value"] or (sort_value == current["_sort_value"] and confidence > current["_confidence"]):
                    annual_burden[key] = candidate

        years = sorted({year for year, _, _ in annual_cascade})
        rows_by_year: dict[str, list[dict]] = {}
        regions = set()
        for year in years:
            year_rows = []
            year_regions = sorted({region for y, region, _ in annual_cascade if y == year})
            for region in year_regions:
                diagnosis = annual_cascade.get((year, region, "diagnosis"))
                treatment = annual_cascade.get((year, region, "treatment"))
                suppression = annual_cascade.get((year, region, "suppression"))
                if not (diagnosis and treatment and suppression):
                    continue
                regions.add(region)
                point = {
                    "region": region,
                    "year": year,
                    "diagnosis": diagnosis["value"],
                    "treatment": treatment["value"],
                    "suppression": suppression["value"],
                    "diagnosis_period": diagnosis["period_label"],
                    "treatment_period": treatment["period_label"],
                    "suppression_period": suppression["period_label"],
                    "diagnosis_source_url": diagnosis["source_url"],
                    "treatment_source_url": treatment["source_url"],
                    "suppression_source_url": suppression["source_url"],
                    "diagnosis_filename": diagnosis["filename"],
                    "treatment_filename": treatment["filename"],
                    "suppression_filename": suppression["filename"],
                    "mean_gap": round(((95 - diagnosis["value"]) + (95 - treatment["value"]) + (95 - suppression["value"])) / 3.0, 1),
                }
                year_rows.append(point)
            year_rows.sort(key=lambda row: row["mean_gap"])
            if year_rows:
                rows_by_year[str(year)] = year_rows

        region_histories: dict[str, dict] = {}
        for region in sorted(regions):
            cascade_history = []
            burden_history = []
            for year in years:
                diagnosis = annual_cascade.get((year, region, "diagnosis"))
                treatment = annual_cascade.get((year, region, "treatment"))
                suppression = annual_cascade.get((year, region, "suppression"))
                if diagnosis or treatment or suppression:
                    cascade_history.append({
                        "year": year,
                        "diagnosis": diagnosis["value"] if diagnosis else None,
                        "treatment": treatment["value"] if treatment else None,
                        "suppression": suppression["value"] if suppression else None,
                        "diagnosis_period": diagnosis["period_label"] if diagnosis else "",
                        "treatment_period": treatment["period_label"] if treatment else "",
                        "suppression_period": suppression["period_label"] if suppression else "",
                        "diagnosis_source_url": diagnosis["source_url"] if diagnosis else "",
                        "treatment_source_url": treatment["source_url"] if treatment else "",
                        "suppression_source_url": suppression["source_url"] if suppression else "",
                        "diagnosis_filename": diagnosis["filename"] if diagnosis else "",
                        "treatment_filename": treatment["filename"] if treatment else "",
                        "suppression_filename": suppression["filename"] if suppression else "",
                    })
                burden = annual_burden.get((year, region))
                if burden:
                    burden_history.append({
                        "year": year,
                        "value": burden["value"],
                        "period_label": burden["period_label"],
                        "source_url": burden["source_url"],
                        "filename": burden["filename"],
                    })
            region_histories[region] = {
                "cascade": cascade_history,
                "burden": burden_history,
            }

        national_history = {
            "diagnosis": [],
            "treatment": [],
            "suppression": [],
        }
        for metric in ("diagnosis", "treatment", "suppression"):
            for year in years:
                row = annual_national.get((year, metric))
                if not row:
                    continue
                national_history[metric].append({
                    "year": year,
                    "value": row["value"],
                    "period_label": row["period_label"],
                    "source_url": row["source_url"],
                    "filename": row["filename"],
                })

        return {
            "years": [int(year) for year in sorted(rows_by_year.keys(), key=int)],
            "default_year": max((int(year) for year in rows_by_year.keys()), default=None),
            "rows_by_year": rows_by_year,
            "regions": sorted(region_histories),
            "region_histories": region_histories,
            "national_history": national_history,
            "coverage_note": "Yearly regional cascade uses the latest observed comparable quarter inside each year. Structured region-level cascade is currently available for 2024 and 2025 only.",
        }

    def _build_yearly_anomaly_series(
        self,
        regional_yearly: dict,
        observations: list[dict],
        filename_source_map: dict[str, str] | None = None,
    ) -> dict:
        def _fit(points_x: list[float], points_y: list[float]) -> tuple[float, float]:
            if len(points_x) < 2 or len(set(points_x)) < 2:
                return 0.0, float(np.mean(points_y)) if points_y else 0.0
            slope, intercept = np.polyfit(points_x, points_y, 1)
            return float(slope), float(intercept)

        def _residual_rows(year_rows: list[dict]) -> list[dict]:
            diagnosis = [row["diagnosis"] for row in year_rows]
            treatment = [row["treatment"] for row in year_rows]
            suppression = [row["suppression"] for row in year_rows]
            dx_slope, dx_intercept = _fit(diagnosis, treatment)
            tx_slope, tx_intercept = _fit(treatment, suppression)
            rows = []
            for row in year_rows:
                fit_treatment = dx_intercept + dx_slope * row["diagnosis"]
                fit_suppression = tx_intercept + tx_slope * row["treatment"]
                rows.append({
                    "region": row["region"],
                    "stage": "Treatment after diagnosis",
                    "label": f"{row['region']} | Treatment after diagnosis",
                    "value": round(row["treatment"] - fit_treatment, 2),
                    "observed": round(row["treatment"], 1),
                    "expected": round(fit_treatment, 1),
                    "source_url": row["treatment_source_url"],
                    "filename": row["treatment_filename"],
                    "period_label": row["treatment_period"],
                })
                rows.append({
                    "region": row["region"],
                    "stage": "Suppression after treatment",
                    "label": f"{row['region']} | Suppression after treatment",
                    "value": round(row["suppression"] - fit_suppression, 2),
                    "observed": round(row["suppression"], 1),
                    "expected": round(fit_suppression, 1),
                    "source_url": row["suppression_source_url"],
                    "filename": row["suppression_filename"],
                    "period_label": row["suppression_period"],
                })
            return sorted(rows, key=lambda item: item["value"])

        leakage_metric_map = {
            "alive_on_art_count": ("alive", "Alive on ART", False),
            "lost_to_follow_up_count": ("ltfu", "Lost to follow-up", False),
            "not_on_treatment_count": ("not_on_treatment", "Other documented off treatment", False),
        }
        annual_leakage: dict[tuple[int, str, str], dict] = {}
        for row in observations:
            metric_type = str(row.get("metric_type") or "")
            if metric_type not in leakage_metric_map:
                continue
            region = _format_region(row.get("region", ""))
            if region not in VALID_REGION_NAMES:
                continue
            year = int(row.get("year") or 0)
            if not year:
                continue
            period_label = str(row.get("period_label") or "")
            sort_value = _period_sort_value(period_label) or (year * 100 + 12)
            confidence = float(row.get("confidence") or 0)
            value = _parse_numeric(row.get("value"))
            if value is None:
                continue
            metric, label, is_proxy = leakage_metric_map[metric_type]
            key = (year, region, metric)
            current = annual_leakage.get(key)
            candidate = {
                "year": year,
                "region": region,
                "metric": metric,
                "metric_label": label,
                "is_proxy_metric": is_proxy,
                "value": float(value),
                "period_label": period_label,
                "filename": row.get("filename", ""),
                "source_url": row.get("source_url", ""),
                "_sort_value": sort_value,
                "_confidence": confidence,
            }
            if current is None or sort_value > current["_sort_value"] or (sort_value == current["_sort_value"] and confidence > current["_confidence"]):
                annual_leakage[key] = candidate

        extracted_year_end = self._extract_year_end_treatment_outcomes(filename_source_map or {})
        for year, rows in extracted_year_end.items():
            for row in rows:
                for metric in ("alive", "ltfu", "not_on_treatment"):
                    key = (year, row["region"], metric)
                    if key in annual_leakage:
                        continue
                    annual_leakage[key] = {
                        "year": year,
                        "region": row["region"],
                        "metric": metric,
                        "metric_label": "Other documented off treatment" if metric == "not_on_treatment" else ("Alive on ART" if metric == "alive" else "Lost to follow-up"),
                        "is_proxy_metric": metric == "not_on_treatment",
                        "value": float(row[metric]),
                        "period_label": row["period_label"],
                        "filename": row["filename"],
                        "source_url": row["source_url"],
                        "_sort_value": row["sort_value"],
                        "_confidence": 1.0,
                    }

        residuals_by_year = {}
        for year_text, year_rows in regional_yearly.get("rows_by_year", {}).items():
            residuals_by_year[year_text] = _residual_rows(year_rows)

        leakage_by_year: dict[str, list[dict]] = {}
        leakage_years = sorted({year for year, _, _ in annual_leakage})
        for year in leakage_years:
            rows = []
            year_regions = sorted({region for y, region, _ in annual_leakage if y == year})
            for region in year_regions:
                alive = annual_leakage.get((year, region, "alive"))
                ltfu = annual_leakage.get((year, region, "ltfu"))
                not_on = annual_leakage.get((year, region, "not_on_treatment"))
                if not (alive or ltfu or not_on):
                    continue
                rows.append({
                    "region": region,
                    "alive": float(alive["value"]) if alive else 0.0,
                    "ltfu": float(ltfu["value"]) if ltfu else 0.0,
                    "not_on_treatment": float(not_on["value"]) if not_on else 0.0,
                    "other_off_treatment_label": (not_on.get("metric_label") if not_on else "Other documented off treatment"),
                    "is_proxy_off_treatment": bool(not_on and not_on.get("is_proxy_metric")),
                    "period_label": max(
                        [item["period_label"] for item in (alive, ltfu, not_on) if item],
                        key=_period_sort_value,
                        default=str(year),
                    ),
                    "source_url": next((item["source_url"] for item in (alive, ltfu, not_on) if item and item.get("source_url")), ""),
                    "filename": next((item["filename"] for item in (alive, ltfu, not_on) if item and item.get("filename")), ""),
                    "missing_not_on_treatment": not not_on,
                })
            rows.sort(key=lambda row: row["ltfu"] + row["not_on_treatment"], reverse=True)
            leakage_by_year[str(year)] = rows

        available_years = sorted(
            {int(year) for year in residuals_by_year.keys()} | {int(year) for year in leakage_by_year.keys()}
        )
        return {
            "years": available_years,
            "default_year": max(available_years, default=None),
            "residuals_by_year": residuals_by_year,
            "leakage_by_year": leakage_by_year,
            "coverage_note": "Residuals are available where regional cascade coverage exists. Treatment leakage uses the latest official treatment-outcome snapshot inside each year, with 2023 backfilled from the year-end SHIP table.",
        }

    def _build_experimental_regional_series(
        self,
        national_cascade: dict,
        regional_yearly: dict,
        anomaly_yearly: dict,
    ) -> dict:
        def _annualize_national_stage(row: dict) -> dict[int, float]:
            annual = {}
            for point in row.get("official_annual", []):
                year = int(point.get("year") or 0)
                value = _parse_numeric(point.get("value"))
                if year and value is not None:
                    annual[year] = float(value)
            return annual

        def _logit_percent(value: float) -> float:
            proportion = float(np.clip(float(value) / 100.0, 0.001, 0.999))
            return float(math.log(proportion / (1.0 - proportion)))

        def _inv_logit_percent(value: float) -> float:
            return float(100.0 / (1.0 + math.exp(-float(value))))

        stage_id_map = {
            "diagnosis": "first_95",
            "treatment": "second_95",
            "suppression": "third_95",
        }
        official_harp = self._read_harp_annual_cascade_series()
        national_rows = {row.get("series_id"): row for row in national_cascade.get("rows", [])}
        national_observed = {
            stage: _annualize_national_stage(national_rows.get(series_id, {}))
            for stage, series_id in stage_id_map.items()
        }
        if not any(national_observed.values()):
            national_observed = {
                "diagnosis": {int(point["year"]): float(point["value"]) for point in official_harp.get("first_95", [])},
                "treatment": {int(point["year"]): float(point["value"]) for point in official_harp.get("second_95", [])},
                "suppression": {int(point["year"]): float(point["value"]) for point in official_harp.get("third_95", [])},
            }
        national_counts_observed = {
            "estimated_plhiv": {int(point["year"]): float(point["value"]) for point in official_harp.get("estimated_plhiv", [])},
            "diagnosed_plhiv": {int(point["year"]): float(point["value"]) for point in official_harp.get("diagnosed_plhiv", [])},
            "plhiv_on_art": {int(point["year"]): float(point["value"]) for point in official_harp.get("plhiv_on_art", [])},
            "suppressed_count": {int(point["year"]): float(point["value"]) for point in official_harp.get("suppressed_count", [])},
        }

        common_national_years = sorted(
            set(national_observed["diagnosis"].keys())
            & set(national_observed["treatment"].keys())
            & set(national_observed["suppression"].keys())
            & set(national_counts_observed["estimated_plhiv"].keys())
        )
        latest_hard_year = max(common_national_years, default=2025)
        forecast_end_year = max(latest_hard_year + 10, 2035)
        forecast_years = list(range(latest_hard_year + 1, forecast_end_year + 1))

        stage_matrix = np.array(
            [
                [_logit_percent(national_observed["diagnosis"][year]), _logit_percent(national_observed["treatment"][year]), _logit_percent(national_observed["suppression"][year])]
                for year in common_national_years
            ],
            dtype=float,
        )
        stage_diffs = np.diff(stage_matrix, axis=0) if len(stage_matrix) > 1 else np.empty((0, 3))
        tail_stage_diffs = stage_diffs[-min(4, len(stage_diffs)) :] if len(stage_diffs) else np.empty((0, 3))
        if len(tail_stage_diffs):
            stage_drift = np.mean(tail_stage_diffs, axis=0)
        else:
            stage_drift = np.array([0.02, 0.03, 0.06], dtype=float)
        if len(tail_stage_diffs) > 1:
            stage_cov = np.cov(tail_stage_diffs.T)
        else:
            stage_cov = np.diag([0.020, 0.016, 0.028])
        stage_cov = np.array(stage_cov, dtype=float)
        stage_cov += np.eye(3) * 0.0025

        estimated_years = sorted(national_counts_observed["estimated_plhiv"])
        log_estimated = np.array([math.log(national_counts_observed["estimated_plhiv"][year]) for year in estimated_years], dtype=float)
        estimated_diffs = np.diff(log_estimated) if len(log_estimated) > 1 else np.array([], dtype=float)
        tail_estimated_diffs = estimated_diffs[-min(4, len(estimated_diffs)) :] if len(estimated_diffs) else np.array([], dtype=float)
        estimated_growth_mu = float(np.mean(tail_estimated_diffs)) if len(tail_estimated_diffs) else 0.08
        estimated_growth_sigma = max(float(np.std(tail_estimated_diffs, ddof=0)) if len(tail_estimated_diffs) > 1 else 0.0, 0.02)

        national_rng = np.random.default_rng(42)
        national_draws = 1200
        exported_paths = 80
        stage_samples: dict[str, dict[int, list[float]]] = {stage: defaultdict(list) for stage in stage_id_map}
        count_samples: dict[str, dict[int, list[float]]] = {key: defaultdict(list) for key in national_counts_observed}
        stage_path_cache: dict[str, list[dict]] = {stage: [] for stage in stage_id_map}
        count_path_cache: dict[str, list[dict]] = {key: [] for key in national_counts_observed}
        bottleneck_counts: dict[int, dict[str, int]] = {year: {"diagnosis": 0, "treatment": 0, "suppression": 0, "all_met": 0} for year in forecast_years}

        latest_stage_logits = stage_matrix[-1] if len(stage_matrix) else np.array([_logit_percent(61), _logit_percent(64), _logit_percent(53)], dtype=float)
        latest_estimated_log = float(log_estimated[-1]) if len(log_estimated) else math.log(252800.0)
        for draw_index in range(national_draws):
            stage_logits = latest_stage_logits.copy()
            estimated_log = latest_estimated_log
            stage_paths = {stage: [(latest_hard_year, float(national_observed[stage].get(latest_hard_year, _inv_logit_percent(stage_logits[index]))))] for index, stage in enumerate(("diagnosis", "treatment", "suppression"))}
            count_paths = {
                "estimated_plhiv": [(latest_hard_year, float(national_counts_observed["estimated_plhiv"].get(latest_hard_year, math.exp(estimated_log))))],
                "diagnosed_plhiv": [(latest_hard_year, float(national_counts_observed["diagnosed_plhiv"].get(latest_hard_year, 0.0)))],
                "plhiv_on_art": [(latest_hard_year, float(national_counts_observed["plhiv_on_art"].get(latest_hard_year, 0.0)))],
                "suppressed_count": [(latest_hard_year, float(national_counts_observed["suppressed_count"].get(latest_hard_year, 0.0)))],
            }

            for year in forecast_years:
                stage_shock = national_rng.multivariate_normal(stage_drift, stage_cov)
                stage_shock = np.clip(stage_shock, -0.40, 0.40)
                stage_logits = np.clip(stage_logits + stage_shock, -3.4, 4.2)
                estimated_growth = float(np.clip(national_rng.normal(estimated_growth_mu, estimated_growth_sigma), -0.05, 0.22))
                estimated_log += estimated_growth

                diagnosis_pct = _inv_logit_percent(stage_logits[0])
                treatment_pct = _inv_logit_percent(stage_logits[1])
                suppression_pct = _inv_logit_percent(stage_logits[2])
                estimated_count = float(math.exp(estimated_log))
                diagnosed_count = float(estimated_count * (diagnosis_pct / 100.0))
                on_art_count = float(diagnosed_count * (treatment_pct / 100.0))
                suppressed_count = float(on_art_count * (suppression_pct / 100.0))

                for stage, value in (
                    ("diagnosis", diagnosis_pct),
                    ("treatment", treatment_pct),
                    ("suppression", suppression_pct),
                ):
                    stage_samples[stage][year].append(value)
                    stage_paths[stage].append((year, value))
                for count_key, value in (
                    ("estimated_plhiv", estimated_count),
                    ("diagnosed_plhiv", diagnosed_count),
                    ("plhiv_on_art", on_art_count),
                    ("suppressed_count", suppressed_count),
                ):
                    count_samples[count_key][year].append(value)
                    count_paths[count_key].append((year, value))

                shortfalls = {
                    "diagnosis": max(95.0 - diagnosis_pct, 0.0),
                    "treatment": max(95.0 - treatment_pct, 0.0),
                    "suppression": max(95.0 - suppression_pct, 0.0),
                }
                dominant_stage = max(shortfalls, key=shortfalls.get)
                if shortfalls[dominant_stage] <= 0:
                    dominant_stage = "all_met"
                bottleneck_counts[year][dominant_stage] += 1

            if draw_index < exported_paths:
                for stage in stage_id_map:
                    stage_path_cache[stage].append({
                        "draw": draw_index + 1,
                        "years": [int(year) for year, _ in stage_paths[stage]],
                        "values": [round(float(value), 1) for _, value in stage_paths[stage]],
                    })
                for count_key in national_counts_observed:
                    count_path_cache[count_key].append({
                        "draw": draw_index + 1,
                        "years": [int(year) for year, _ in count_paths[count_key]],
                        "values": [round(float(value), 0) for _, value in count_paths[count_key]],
                    })

        national_stage_values = {stage: dict(values) for stage, values in national_observed.items()}
        national_forecast_rows = []
        for year in forecast_years:
            forecast_row = {"year": year}
            for stage in ("diagnosis", "treatment", "suppression"):
                samples = np.array(stage_samples[stage].get(year, []), dtype=float)
                if samples.size:
                    forecast_row[stage] = round(float(np.median(samples)), 1)
                    forecast_row[f"{stage}_lower"] = round(float(np.quantile(samples, 0.1)), 1)
                    forecast_row[f"{stage}_upper"] = round(float(np.quantile(samples, 0.9)), 1)
                    national_stage_values[stage][year] = forecast_row[stage]
            for count_key in ("estimated_plhiv", "diagnosed_plhiv", "plhiv_on_art", "suppressed_count"):
                samples = np.array(count_samples[count_key].get(year, []), dtype=float)
                if samples.size:
                    forecast_row[count_key] = round(float(np.median(samples)), 0)
                    forecast_row[f"{count_key}_lower"] = round(float(np.quantile(samples, 0.1)), 0)
                    forecast_row[f"{count_key}_upper"] = round(float(np.quantile(samples, 0.9)), 0)
            national_forecast_rows.append(forecast_row)

        national_target_probabilities = []
        national_bottlenecks = []
        for year in forecast_years:
            target_row = {"year": year}
            for stage in ("diagnosis", "treatment", "suppression"):
                samples = np.array(stage_samples[stage].get(year, []), dtype=float)
                target_row[stage] = round(float(np.mean(samples >= 95.0)), 4) if samples.size else None
            national_target_probabilities.append(target_row)
            counts = bottleneck_counts.get(year, {})
            total = sum(counts.values()) or 1
            national_bottlenecks.append({
                "year": year,
                "diagnosis": round(counts.get("diagnosis", 0) / total, 4),
                "treatment": round(counts.get("treatment", 0) / total, 4),
                "suppression": round(counts.get("suppression", 0) / total, 4),
                "all_met": round(counts.get("all_met", 0) / total, 4),
            })

        observed_years = sorted(
            int(year)
            for year in (regional_yearly.get("years") or [])
            if int(year)
        )
        latest_observed_year = max(observed_years, default=latest_hard_year)

        region_histories = regional_yearly.get("region_histories", {}) or {}
        regions = regional_yearly.get("regions", []) or sorted(region_histories)
        all_years = sorted(set(observed_years) | set(forecast_years))

        estimated_histories: dict[str, dict] = {}
        rows_by_year: dict[str, list[dict]] = {}
        observed_years_by_stage = defaultdict(list)
        estimated_years_by_stage = defaultdict(list)
        forecast_years_by_stage = defaultdict(list)

        stage_residual_steps: dict[str, list[float]] = defaultdict(list)
        for region in regions:
            cascade_history = region_histories.get(region, {}).get("cascade", []) or []
            observed_lookup = {
                int(row.get("year") or 0): row
                for row in cascade_history
                if int(row.get("year") or 0)
            }
            for stage in ("diagnosis", "treatment", "suppression"):
                stage_years_with_obs = sorted(
                    year
                    for year, row in observed_lookup.items()
                    if _parse_numeric(row.get(stage)) is not None and national_stage_values.get(stage, {}).get(year) is not None
                )
                if len(stage_years_with_obs) < 2:
                    continue
                for previous_year, current_year in zip(stage_years_with_obs[:-1], stage_years_with_obs[1:]):
                    previous_value = float(_parse_numeric(observed_lookup[previous_year].get(stage)))
                    current_value = float(_parse_numeric(observed_lookup[current_year].get(stage)))
                    previous_residual = previous_value - national_stage_values[stage][previous_year]
                    current_residual = current_value - national_stage_values[stage][current_year]
                    stage_residual_steps[stage].append(current_residual - previous_residual)

        rng = np.random.default_rng(42)
        draws = national_draws
        phi = 0.82
        exported_paths = 80

        for region in regions:
            cascade_history = region_histories.get(region, {}).get("cascade", []) or []
            observed_lookup = {
                int(row.get("year") or 0): row
                for row in cascade_history
                if int(row.get("year") or 0)
            }
            stage_models = {}
            for stage in ("diagnosis", "treatment", "suppression"):
                anchors = []
                for year, row in observed_lookup.items():
                    value = _parse_numeric(row.get(stage))
                    national_value = national_stage_values.get(stage, {}).get(year)
                    if value is None or national_value is None:
                        continue
                    anchors.append((year, float(value), float(value) - float(national_value)))
                anchors.sort(key=lambda item: item[0])
                anchor_years = [item[0] for item in anchors]
                anchor_residuals = [item[2] for item in anchors]
                stage_steps = stage_residual_steps.get(stage, [])
                global_step = float(np.mean(stage_steps)) if stage_steps else 0.0
                sigma = float(np.std(stage_steps, ddof=0)) if len(stage_steps) > 1 else 0.0
                sigma = max(2.5, sigma + 0.75)
                if len(anchor_residuals) >= 2:
                    region_step = float(anchor_residuals[-1] - anchor_residuals[-2])
                else:
                    region_step = global_step
                stage_models[stage] = {
                    "anchor_years": anchor_years,
                    "anchor_residuals": anchor_residuals,
                    "step_mean": region_step,
                    "global_step_mean": global_step,
                    "sigma": sigma,
                    "draws": draws,
                    "phi": phi,
                    "status": "observed_only" if anchors else "unavailable",
                }

            region_rows = []
            region_paths = {}
            for year in all_years:
                row = {"year": year, "region": region}
                has_any_value = False
                observed_row = observed_lookup.get(year)
                for stage in ("diagnosis", "treatment", "suppression"):
                    national_value = national_stage_values.get(stage, {}).get(year)
                    if national_value is None:
                        row[f"{stage}"] = None
                        row[f"{stage}_status"] = "missing"
                        row[f"{stage}_lower"] = None
                        row[f"{stage}_upper"] = None
                        continue
                    observed_value = _parse_numeric((observed_row or {}).get(stage))
                    if observed_value is not None:
                        row[f"{stage}"] = float(observed_value)
                        row[f"{stage}_status"] = "observed"
                        row[f"{stage}_lower"] = float(observed_value)
                        row[f"{stage}_upper"] = float(observed_value)
                        row[f"{stage}_period"] = (observed_row or {}).get(f"{stage}_period", str(year))
                        row[f"{stage}_source_url"] = (observed_row or {}).get(f"{stage}_source_url", "")
                        row[f"{stage}_filename"] = (observed_row or {}).get(f"{stage}_filename", "")
                        observed_years_by_stage[stage].append(year)
                    else:
                        model = stage_models[stage]
                        latest_anchor = max(model["anchor_years"], default=None)
                        if not model["anchor_years"] or year <= latest_anchor:
                            row[f"{stage}"] = None
                            row[f"{stage}_status"] = "missing"
                            row[f"{stage}_lower"] = None
                            row[f"{stage}_upper"] = None
                            continue

                        cached = region_paths.get(stage)
                        if cached is None:
                            latest_anchor_row = observed_lookup.get(latest_anchor, {})
                            latest_anchor_value = float(_parse_numeric(latest_anchor_row.get(stage)) or 0.0)
                            latest_anchor_residual = float(model["anchor_residuals"][-1])
                            stage_simulations: list[list[tuple[int, float]]] = []
                            reach_years: list[int | None] = []
                            forecast_year_lookup: dict[int, list[float]] = defaultdict(list)
                            for _ in range(draws):
                                residual = latest_anchor_residual
                                path = [(latest_anchor, latest_anchor_value)]
                                reached_year = latest_anchor if latest_anchor_value >= 95 else None
                                for forward_year in forecast_years:
                                    national_forward_value = national_stage_values.get(stage, {}).get(forward_year)
                                    if national_forward_value is None:
                                        continue
                                    residual = float(
                                        np.clip(
                                            phi * residual + model["step_mean"] + rng.normal(0.0, model["sigma"]),
                                            -60.0,
                                            60.0,
                                        )
                                    )
                                    value = float(np.clip(float(national_forward_value) + residual, 0.0, 100.0))
                                    path.append((forward_year, value))
                                    forecast_year_lookup[forward_year].append(value)
                                    if reached_year is None and value >= 95:
                                        reached_year = forward_year
                                stage_simulations.append(path)
                                reach_years.append(reached_year)

                            sampled_paths = []
                            if stage_simulations:
                                stride = max(1, len(stage_simulations) // exported_paths)
                                for index, path in enumerate(stage_simulations[::stride][:exported_paths], start=1):
                                    sampled_paths.append({
                                        "draw": index,
                                        "years": [int(year_value) for year_value, _ in path],
                                        "values": [round(float(value), 1) for _, value in path],
                                    })

                            reach_probabilities = {}
                            for forward_year in forecast_years:
                                valid_draws = len(stage_simulations)
                                if not valid_draws:
                                    continue
                                reached = sum(
                                    1
                                    for reached_year in reach_years
                                    if reached_year is not None and reached_year <= forward_year
                                )
                                reach_probabilities[str(forward_year)] = round(reached / valid_draws, 4)

                            cached = {
                                "paths": sampled_paths,
                                "by_year": {
                                    forward_year: {
                                        "median": float(np.median(values)),
                                        "lower": float(np.quantile(values, 0.1)),
                                        "upper": float(np.quantile(values, 0.9)),
                                    }
                                    for forward_year, values in forecast_year_lookup.items()
                                    if values
                                },
                                "latest_anchor_year": latest_anchor,
                                "latest_anchor_value": round(latest_anchor_value, 1),
                                "reach_probabilities": reach_probabilities,
                            }
                            region_paths[stage] = cached

                        forecast_stats = cached["by_year"].get(year)
                        if not forecast_stats:
                            row[f"{stage}"] = None
                            row[f"{stage}_status"] = "missing"
                            row[f"{stage}_lower"] = None
                            row[f"{stage}_upper"] = None
                            continue

                        row[f"{stage}"] = round(float(forecast_stats["median"]), 1)
                        row[f"{stage}_status"] = "forecast"
                        row[f"{stage}_lower"] = round(max(0.0, float(forecast_stats["lower"])), 1)
                        row[f"{stage}_upper"] = round(min(100.0, float(forecast_stats["upper"])), 1)
                        row[f"{stage}_period"] = str(year)
                        row[f"{stage}_source_url"] = ""
                        row[f"{stage}_filename"] = "Experimental forward forecast"
                        forecast_years_by_stage[stage].append(year)
                    has_any_value = has_any_value or row[f"{stage}"] is not None
                leakage_row = next((item for item in anomaly_yearly.get("leakage_by_year", {}).get(str(year), []) if item.get("region") == region), None)
                if leakage_row:
                    row["leakage_burden"] = float(leakage_row.get("ltfu", 0.0)) + float(leakage_row.get("not_on_treatment", 0.0))
                    row["leakage_status"] = "observed"
                else:
                    row["leakage_burden"] = None
                    row["leakage_status"] = "missing"
                if has_any_value:
                    region_rows.append(row)
                    rows_by_year.setdefault(str(year), []).append(row)

            estimated_histories[region] = {
                "cascade": region_rows,
                "paths": {
                    stage: region_paths.get(stage, {}).get("paths", [])
                    for stage in ("diagnosis", "treatment", "suppression")
                },
                "model": {
                    stage: {
                        "anchor_years": stage_models[stage]["anchor_years"],
                        "latest_anchor_year": max(stage_models[stage]["anchor_years"], default=None),
                        "step_mean": round(stage_models[stage]["step_mean"], 2),
                        "global_step_mean": round(stage_models[stage]["global_step_mean"], 2),
                        "sigma": round(stage_models[stage]["sigma"], 2),
                        "draws": stage_models[stage]["draws"],
                        "phi": stage_models[stage]["phi"],
                        "reach_probabilities": region_paths.get(stage, {}).get("reach_probabilities", {}),
                        "status": stage_models[stage]["status"],
                    }
                    for stage in stage_models
                },
            }

        for year, rows in rows_by_year.items():
            rows.sort(
                key=lambda row: (
                    -1 if row.get("diagnosis_status") == "observed" and row.get("treatment_status") == "observed" and row.get("suppression_status") == "observed" else 0,
                    (
                        (95 - float(row["diagnosis"])) if row.get("diagnosis") is not None else 95
                    ) + (
                        (95 - float(row["treatment"])) if row.get("treatment") is not None else 95
                    ) + (
                        (95 - float(row["suppression"])) if row.get("suppression") is not None else 95
                    ),
                )
            )

        observed_stage_years = {
            stage: sorted(set(observed_years_by_stage[stage]))
            for stage in ("diagnosis", "treatment", "suppression")
        }
        estimated_stage_years = {
            stage: []
            for stage in ("diagnosis", "treatment", "suppression")
        }
        forecast_stage_years = {
            stage: sorted(set(forecast_years_by_stage[stage]))
            for stage in ("diagnosis", "treatment", "suppression")
        }
        national_observed_rows = []
        for year in common_national_years:
            national_observed_rows.append({
                "year": year,
                "diagnosis": round(float(national_observed["diagnosis"].get(year, math.nan)), 1),
                "treatment": round(float(national_observed["treatment"].get(year, math.nan)), 1),
                "suppression": round(float(national_observed["suppression"].get(year, math.nan)), 1),
                "estimated_plhiv": round(float(national_counts_observed["estimated_plhiv"].get(year, math.nan)), 0),
                "diagnosed_plhiv": round(float(national_counts_observed["diagnosed_plhiv"].get(year, math.nan)), 0),
                "plhiv_on_art": round(float(national_counts_observed["plhiv_on_art"].get(year, math.nan)), 0),
                "suppressed_count": round(float(national_counts_observed["suppressed_count"].get(year, math.nan)), 0),
            })

        contracts = []
        threshold_grid = {
            "diagnosis": [70, 80, 90, 95],
            "treatment": [70, 80, 90, 95],
            "suppression": [60, 70, 80, 90, 95],
        }
        for stage in ("diagnosis", "treatment", "suppression"):
            current_value = float(national_observed[stage].get(latest_hard_year, 0.0))
            for target_year in (2030, 2035):
                if target_year not in forecast_years:
                    continue
                samples = np.array(stage_samples[stage].get(target_year, []), dtype=float)
                if not samples.size:
                    continue
                for threshold in threshold_grid[stage]:
                    if threshold <= current_value:
                        continue
                    contracts.append({
                        "stage": stage,
                        "threshold": threshold,
                        "target_year": target_year,
                        "probability": round(float(np.mean(samples >= threshold)), 4),
                        "label": f"{stage.title()} reaches {threshold}% by {target_year}",
                    })

        latest_leakage_year = max((int(year) for year in anomaly_yearly.get("leakage_by_year", {}).keys()), default=None)
        latest_leakage_rows = anomaly_yearly.get("leakage_by_year", {}).get(str(latest_leakage_year), []) if latest_leakage_year else []
        latest_leakage_lookup = {row.get("region"): row for row in latest_leakage_rows}
        watch_year = 2030 if 2030 in forecast_years else (forecast_years[-1] if forecast_years else latest_hard_year)
        regional_watchlist = []
        for region in regions:
            history = estimated_histories.get(region, {}).get("cascade", [])
            target_row = next((row for row in history if int(row.get("year") or 0) == watch_year), None)
            if not target_row:
                continue
            shortfalls = {
                "diagnosis": max(95.0 - float(target_row.get("diagnosis") or 0.0), 0.0),
                "treatment": max(95.0 - float(target_row.get("treatment") or 0.0), 0.0),
                "suppression": max(95.0 - float(target_row.get("suppression") or 0.0), 0.0),
            }
            leak_row = latest_leakage_lookup.get(region, {})
            leak_alive = float(leak_row.get("alive", 0.0) or 0.0)
            leak_ltfu = float(leak_row.get("ltfu", 0.0) or 0.0)
            leak_off = float(leak_row.get("not_on_treatment", 0.0) or 0.0)
            leak_denom = leak_alive + leak_ltfu + leak_off
            leakage_rate = ((leak_ltfu + leak_off) / leak_denom * 100.0) if leak_denom > 0 else 0.0
            bottleneck_stage = max(shortfalls, key=shortfalls.get)
            mean_gap = float(np.mean(list(shortfalls.values())))
            risk_score = mean_gap + leakage_rate * 0.35
            regional_watchlist.append({
                "region": region,
                "target_year": watch_year,
                "diagnosis": round(float(target_row.get("diagnosis") or 0.0), 1),
                "treatment": round(float(target_row.get("treatment") or 0.0), 1),
                "suppression": round(float(target_row.get("suppression") or 0.0), 1),
                "mean_gap": round(mean_gap, 1),
                "bottleneck_stage": bottleneck_stage,
                "leakage_rate": round(leakage_rate, 1),
                "risk_score": round(risk_score, 1),
            })
        regional_watchlist.sort(key=lambda row: row["risk_score"], reverse=True)

        return {
            "years": all_years,
            "default_year": latest_hard_year,
            "default_region": "Region 6" if "Region 6" in regions else (regions[0] if regions else ""),
            "rows_by_year": rows_by_year,
            "regions": regions,
            "region_histories": estimated_histories,
            "observed_years_by_stage": observed_stage_years,
            "estimated_years_by_stage": estimated_stage_years,
            "forecast_years_by_stage": forecast_stage_years,
            "latest_observed_year": latest_observed_year,
            "latest_hard_year": latest_hard_year,
            "forecast_end_year": forecast_end_year,
            "coverage_note": f"Observed annual national seed values currently run through {latest_hard_year}. Observed regional anchors currently span {min(observed_years) if observed_years else latest_hard_year}-{latest_observed_year}. Forward forecasts start after the latest hard year and never overwrite observed rows.",
            "model_note": "Experimental forecasts combine a national logit-transition Monte Carlo model for diagnosis, treatment, and suppression with a regional AR(1) gap process anchored to observed region-level departures from the national path. Forecasts are forward-looking only and remain outside the official atlas.",
            "source_policy": [
                "Observed regional cascade values always override model forecasts.",
                "National experimental seed values are always taken from the latest hard annual official HARP year available in the normalized layer.",
                "Diagnosis, treatment, and suppression forecasts are simulated jointly on the logit scale from the latest official national stage values, then regional gaps are projected forward around that national path.",
                "No historical regional backfill is shown in this layer.",
            ],
            "national": {
                "latest_hard_year": latest_hard_year,
                "forecast_years": forecast_years,
                "observed": national_observed_rows,
                "forecast": national_forecast_rows,
                "paths": {
                    "stages": stage_path_cache,
                    "counts": count_path_cache,
                },
                "target_probabilities": national_target_probabilities,
                "bottlenecks": national_bottlenecks,
            },
            "regional": {
                "default_region": "Region 6" if "Region 6" in regions else (regions[0] if regions else ""),
                "watch_year": watch_year,
                "region_histories": estimated_histories,
            },
            "simulation": {
                "engine": "Joint logit-transition Monte Carlo with AR(1) regional gaps",
                "latest_hard_year": latest_hard_year,
                "regional_anchor_year": latest_observed_year,
                "draws": national_draws,
                "forecast_start_year": forecast_years[0] if forecast_years else latest_hard_year,
                "forecast_end_year": forecast_end_year,
                "future_only": True,
            },
            "outputs": {
                "contracts": contracts,
                "regional_watchlist": regional_watchlist[:10],
                "national_target_probabilities": national_target_probabilities,
                "bottleneck_probabilities": national_bottlenecks,
            },
        }

    def _build_experimental_regional_series(
        self,
        national_cascade: dict,
        regional_yearly: dict,
        anomaly_yearly: dict,
    ) -> dict:
        def _annualize_national_stage(row: dict) -> dict[int, float]:
            annual = {}
            for point in row.get("official_annual", []):
                year = int(point.get("year") or 0)
                value = _parse_numeric(point.get("value"))
                if year and value is not None:
                    annual[year] = float(value)
            return annual

        def _logit_percent(value: float) -> float:
            proportion = float(np.clip(float(value) / 100.0, 0.001, 0.999))
            return float(math.log(proportion / (1.0 - proportion)))

        def _inv_logit_percent(value: float) -> float:
            return float(100.0 / (1.0 + math.exp(-float(value))))

        def _safe_quantile(values: list[float], q: float, default: float = 0.0) -> float:
            array = np.asarray(values, dtype=float)
            if not array.size:
                return float(default)
            return float(np.quantile(array, q))

        stage_id_map = {
            "diagnosis": "first_95",
            "treatment": "second_95",
            "suppression": "third_95",
        }
        stages = ("diagnosis", "treatment", "suppression")
        official_harp = self._read_harp_annual_cascade_series()
        national_rows = {row.get("series_id"): row for row in national_cascade.get("rows", [])}

        national_observed = {
            "diagnosis": {int(point["year"]): float(point["value"]) for point in official_harp.get("first_95", [])},
            "treatment": {int(point["year"]): float(point["value"]) for point in official_harp.get("second_95", [])},
            "suppression": {int(point["year"]): float(point["value"]) for point in official_harp.get("third_95", [])},
        }
        for stage, series_id in stage_id_map.items():
            if not national_observed[stage]:
                national_observed[stage] = _annualize_national_stage(national_rows.get(series_id, {}))

        national_counts_observed = {
            "estimated_plhiv": {int(point["year"]): float(point["value"]) for point in official_harp.get("estimated_plhiv", [])},
            "diagnosed_plhiv": {int(point["year"]): float(point["value"]) for point in official_harp.get("diagnosed_plhiv", [])},
            "plhiv_on_art": {int(point["year"]): float(point["value"]) for point in official_harp.get("plhiv_on_art", [])},
            "suppressed_count": {int(point["year"]): float(point["value"]) for point in official_harp.get("suppressed_count", [])},
        }

        common_national_years = sorted(
            set(national_observed["diagnosis"].keys())
            & set(national_observed["treatment"].keys())
            & set(national_observed["suppression"].keys())
            & set(national_counts_observed["estimated_plhiv"].keys())
            & set(national_counts_observed["diagnosed_plhiv"].keys())
            & set(national_counts_observed["plhiv_on_art"].keys())
            & set(national_counts_observed["suppressed_count"].keys())
        )
        if not common_national_years:
            return {
                "years": [],
                "default_year": None,
                "default_region": "",
                "rows_by_year": {},
                "regions": [],
                "region_histories": {},
                "observed_years_by_stage": {stage: [] for stage in stages},
                "estimated_years_by_stage": {stage: [] for stage in stages},
                "forecast_years_by_stage": {stage: [] for stage in stages},
                "latest_observed_year": None,
                "latest_hard_year": None,
                "forecast_end_year": None,
                "coverage_note": "No official annual national seed was available for experimental forecasting.",
                "model_note": "No Bayesian experimental model could be fit because the official national seed series was unavailable.",
                "source_policy": [],
                "national": {"observed": [], "forecast": [], "paths": {"stages": {}, "counts": {}}, "target_probabilities": [], "bottlenecks": []},
                "regional": {"default_region": "", "watch_year": None, "region_histories": {}},
                "simulation": {"engine": "Unavailable", "future_only": True},
                "outputs": {"contracts": [], "regional_watchlist": [], "national_target_probabilities": [], "bottleneck_probabilities": []},
            }

        latest_hard_year = max(common_national_years)
        forecast_end_year = max(latest_hard_year + 10, 2035)
        forecast_years = list(range(latest_hard_year + 1, forecast_end_year + 1))
        years_diff = np.asarray(common_national_years[1:], dtype=int)
        design_matrix = np.column_stack([
            np.ones(len(years_diff), dtype=float),
            (years_diff >= 2023).astype(float),
        ])

        posterior_draws = 500
        burnin = 600
        thin = 4
        regression_rng = np.random.default_rng(42)

        stage_models: dict[str, dict] = {}
        last_stage_deltas: list[float] = []
        for stage in stages:
            stage_values = np.asarray([national_observed[stage][year] for year in common_national_years], dtype=float)
            stage_logits = np.asarray([_logit_percent(value) for value in stage_values], dtype=float)
            y = np.diff(stage_logits)
            model = _bayesian_regression_mcmc(
                y,
                design_matrix,
                draws=posterior_draws,
                burnin=burnin,
                thin=thin,
                rng=regression_rng,
                prior_sd=0.22,
                a0=3.0,
                b0=0.015,
            )
            model["years"] = years_diff.tolist()
            model["latest_value"] = float(stage_values[-1])
            model["latest_logit"] = float(stage_logits[-1])
            model["last_delta"] = float(y[-1]) if y.size else float(model["beta_mean"][0])
            stage_models[stage] = model
            last_stage_deltas.append(model["last_delta"])

        estimated_years = sorted(national_counts_observed["estimated_plhiv"])
        estimated_logs = np.asarray(
            [math.log(national_counts_observed["estimated_plhiv"][year]) for year in estimated_years],
            dtype=float,
        )
        growth_model = _bayesian_regression_mcmc(
            np.diff(estimated_logs),
            np.column_stack([
                np.ones(len(estimated_years) - 1, dtype=float),
                (np.asarray(estimated_years[1:], dtype=int) >= 2023).astype(float),
            ]),
            draws=posterior_draws,
            burnin=burnin,
            thin=thin,
            rng=np.random.default_rng(84),
            prior_sd=0.12,
            a0=3.0,
            b0=0.003,
        )
        growth_model["latest_log"] = float(estimated_logs[-1])
        growth_model["last_delta"] = float(np.diff(estimated_logs)[-1]) if len(estimated_logs) > 1 else 0.06

        residual_matrix = np.column_stack([stage_models[stage]["residuals"] for stage in stages])
        stage_corr = _shrink_correlation(residual_matrix, shrinkage=0.35)
        national_draws = min(
            len(stage_models["diagnosis"]["beta_draws"]),
            len(stage_models["treatment"]["beta_draws"]),
            len(stage_models["suppression"]["beta_draws"]),
            len(growth_model["beta_draws"]),
        )
        national_draws = max(national_draws, 1)
        exported_paths = min(80, national_draws)

        stage_samples: dict[str, dict[int, list[float]]] = {stage: defaultdict(list) for stage in stages}
        count_samples: dict[str, dict[int, list[float]]] = {key: defaultdict(list) for key in national_counts_observed}
        person_samples: dict[str, dict[int, list[float]]] = {stage: defaultdict(list) for stage in stages}
        stage_path_cache: dict[str, list[dict]] = {stage: [] for stage in stages}
        count_path_cache: dict[str, list[dict]] = {key: [] for key in national_counts_observed}
        bottleneck_counts: dict[int, dict[str, int]] = {year: {"diagnosis": 0, "treatment": 0, "suppression": 0, "all_met": 0} for year in forecast_years}
        person_bottleneck_counts: dict[int, dict[str, int]] = {year: {"diagnosis": 0, "treatment": 0, "suppression": 0} for year in forecast_years}
        first_hit_counts: dict[str, dict[int, int]] = {stage: {year: 0 for year in forecast_years} for stage in stages}

        latest_stage_logits = np.asarray([stage_models[stage]["latest_logit"] for stage in stages], dtype=float)
        prev_stage_deltas = np.asarray(last_stage_deltas, dtype=float)
        latest_estimated_log = float(growth_model["latest_log"])
        prev_growth_delta = float(growth_model["last_delta"])
        simulation_rng = np.random.default_rng(2025)

        for draw_index in range(national_draws):
            sample_index = draw_index
            stage_logits = latest_stage_logits.copy()
            estimated_log = latest_estimated_log
            previous_deltas = prev_stage_deltas.copy()
            previous_growth = prev_growth_delta
            reached = {
                stage: latest_hard_year if float(national_observed[stage][latest_hard_year]) >= 95.0 else None
                for stage in stages
            }
            stage_paths = {
                stage: [(latest_hard_year, float(national_observed[stage][latest_hard_year]))]
                for stage in stages
            }
            count_paths = {
                "estimated_plhiv": [(latest_hard_year, float(national_counts_observed["estimated_plhiv"][latest_hard_year]))],
                "diagnosed_plhiv": [(latest_hard_year, float(national_counts_observed["diagnosed_plhiv"][latest_hard_year]))],
                "plhiv_on_art": [(latest_hard_year, float(national_counts_observed["plhiv_on_art"][latest_hard_year]))],
                "suppressed_count": [(latest_hard_year, float(national_counts_observed["suppressed_count"][latest_hard_year]))],
            }

            for year in forecast_years:
                x_future = np.asarray([1.0, 1.0 if year >= 2023 else 0.0], dtype=float)
                mean_delta = np.asarray([
                    float(stage_models[stage]["beta_draws"][sample_index] @ x_future)
                    for stage in stages
                ], dtype=float)
                sigma_vec = np.asarray([
                    math.sqrt(max(float(stage_models[stage]["sigma_draws"][sample_index]), 1e-6))
                    for stage in stages
                ], dtype=float)
                stage_cov = np.diag(sigma_vec) @ stage_corr @ np.diag(sigma_vec)
                stage_shock = simulation_rng.multivariate_normal(np.zeros(len(stages)), stage_cov)
                current_deltas = mean_delta + 0.45 * (previous_deltas - mean_delta) + stage_shock
                current_deltas = np.clip(current_deltas, -0.35, 0.35)
                stage_logits = np.clip(stage_logits + current_deltas, -4.2, 4.6)
                previous_deltas = current_deltas

                mean_growth = float(growth_model["beta_draws"][sample_index] @ x_future)
                growth_sd = math.sqrt(max(float(growth_model["sigma_draws"][sample_index]), 1e-6))
                growth = float(mean_growth + 0.55 * (previous_growth - mean_growth) + simulation_rng.normal(0.0, growth_sd))
                growth = float(np.clip(growth, -0.02, 0.20))
                estimated_log += growth
                previous_growth = growth

                diagnosis_pct = _inv_logit_percent(stage_logits[0])
                treatment_pct = _inv_logit_percent(stage_logits[1])
                suppression_pct = _inv_logit_percent(stage_logits[2])
                estimated_count = float(math.exp(estimated_log))
                diagnosed_count = float(estimated_count * (diagnosis_pct / 100.0))
                on_art_count = float(diagnosed_count * (treatment_pct / 100.0))
                suppressed_count = float(on_art_count * (suppression_pct / 100.0))

                stage_values = {
                    "diagnosis": diagnosis_pct,
                    "treatment": treatment_pct,
                    "suppression": suppression_pct,
                }
                count_values = {
                    "estimated_plhiv": estimated_count,
                    "diagnosed_plhiv": diagnosed_count,
                    "plhiv_on_art": on_art_count,
                    "suppressed_count": suppressed_count,
                }
                person_shortfalls = {
                    "diagnosis": max(estimated_count - diagnosed_count, 0.0),
                    "treatment": max(diagnosed_count - on_art_count, 0.0),
                    "suppression": max(on_art_count - suppressed_count, 0.0),
                }

                for stage, value in stage_values.items():
                    stage_samples[stage][year].append(value)
                    stage_paths[stage].append((year, value))
                    if reached[stage] is None and value >= 95.0:
                        reached[stage] = year
                        first_hit_counts[stage][year] += 1
                for count_key, value in count_values.items():
                    count_samples[count_key][year].append(value)
                    count_paths[count_key].append((year, value))
                for stage, value in person_shortfalls.items():
                    person_samples[stage][year].append(value)

                rate_shortfalls = {
                    "diagnosis": max(95.0 - diagnosis_pct, 0.0),
                    "treatment": max(95.0 - treatment_pct, 0.0),
                    "suppression": max(95.0 - suppression_pct, 0.0),
                }
                rate_stage = max(rate_shortfalls, key=rate_shortfalls.get)
                if rate_shortfalls[rate_stage] <= 0:
                    rate_stage = "all_met"
                bottleneck_counts[year][rate_stage] += 1

                person_stage = max(person_shortfalls, key=person_shortfalls.get)
                person_bottleneck_counts[year][person_stage] += 1

            if draw_index < exported_paths:
                for stage in stages:
                    stage_path_cache[stage].append({
                        "draw": draw_index + 1,
                        "years": [int(year) for year, _ in stage_paths[stage]],
                        "values": [round(float(value), 1) for _, value in stage_paths[stage]],
                    })
                for count_key in count_paths:
                    count_path_cache[count_key].append({
                        "draw": draw_index + 1,
                        "years": [int(year) for year, _ in count_paths[count_key]],
                        "values": [round(float(value), 0) for _, value in count_paths[count_key]],
                    })

        national_stage_values = {stage: dict(values) for stage, values in national_observed.items()}
        national_forecast_rows = []
        for year in forecast_years:
            row = {"year": year}
            for stage in stages:
                values = stage_samples[stage].get(year, [])
                if values:
                    row[stage] = round(float(np.median(values)), 1)
                    row[f"{stage}_lower"] = round(_safe_quantile(values, 0.1), 1)
                    row[f"{stage}_upper"] = round(_safe_quantile(values, 0.9), 1)
                    national_stage_values[stage][year] = float(row[stage])
            for count_key in ("estimated_plhiv", "diagnosed_plhiv", "plhiv_on_art", "suppressed_count"):
                values = count_samples[count_key].get(year, [])
                if values:
                    row[count_key] = round(float(np.median(values)), 0)
                    row[f"{count_key}_lower"] = round(_safe_quantile(values, 0.1), 0)
                    row[f"{count_key}_upper"] = round(_safe_quantile(values, 0.9), 0)
            for stage in stages:
                values = person_samples[stage].get(year, [])
                if values:
                    row[f"{stage}_gap_count"] = round(float(np.median(values)), 0)
            national_forecast_rows.append(row)

        national_observed_rows = []
        for year in common_national_years:
            diagnosed = float(national_counts_observed["diagnosed_plhiv"][year])
            on_art = float(national_counts_observed["plhiv_on_art"][year])
            suppressed = float(national_counts_observed["suppressed_count"][year])
            estimated = float(national_counts_observed["estimated_plhiv"][year])
            national_observed_rows.append({
                "year": year,
                "diagnosis": round(float(national_observed["diagnosis"][year]), 1),
                "treatment": round(float(national_observed["treatment"][year]), 1),
                "suppression": round(float(national_observed["suppression"][year]), 1),
                "estimated_plhiv": round(estimated, 0),
                "diagnosed_plhiv": round(diagnosed, 0),
                "plhiv_on_art": round(on_art, 0),
                "suppressed_count": round(suppressed, 0),
                "diagnosis_gap_count": round(max(estimated - diagnosed, 0.0), 0),
                "treatment_gap_count": round(max(diagnosed - on_art, 0.0), 0),
                "suppression_gap_count": round(max(on_art - suppressed, 0.0), 0),
            })

        national_target_probabilities = []
        national_bottlenecks = []
        national_person_bottlenecks = []
        first_hit_probabilities = {stage: [] for stage in stages}
        for year in forecast_years:
            target_row = {"year": year}
            for stage in stages:
                samples = np.asarray(stage_samples[stage].get(year, []), dtype=float)
                target_row[stage] = round(float(np.mean(samples >= 95.0)), 4) if samples.size else None
            national_target_probabilities.append(target_row)

            total_rate = sum(bottleneck_counts[year].values()) or 1
            national_bottlenecks.append({
                "year": year,
                "diagnosis": round(bottleneck_counts[year]["diagnosis"] / total_rate, 4),
                "treatment": round(bottleneck_counts[year]["treatment"] / total_rate, 4),
                "suppression": round(bottleneck_counts[year]["suppression"] / total_rate, 4),
                "all_met": round(bottleneck_counts[year]["all_met"] / total_rate, 4),
            })

            total_people = sum(person_bottleneck_counts[year].values()) or 1
            national_person_bottlenecks.append({
                "year": year,
                "diagnosis": round(person_bottleneck_counts[year]["diagnosis"] / total_people, 4),
                "treatment": round(person_bottleneck_counts[year]["treatment"] / total_people, 4),
                "suppression": round(person_bottleneck_counts[year]["suppression"] / total_people, 4),
            })

        for stage in stages:
            for year in forecast_years:
                first_hit_probabilities[stage].append({
                    "year": year,
                    "probability": round(first_hit_counts[stage][year] / national_draws, 4),
                })

        threshold_grid = {
            "diagnosis": [70, 80, 90, 95],
            "treatment": [70, 80, 90, 95],
            "suppression": [60, 70, 80, 90, 95],
        }
        contracts = []
        for stage in stages:
            current_value = float(national_observed[stage].get(latest_hard_year, 0.0))
            for target_year in (2030, 2035):
                if target_year not in forecast_years:
                    continue
                samples = np.asarray(stage_samples[stage].get(target_year, []), dtype=float)
                if not samples.size:
                    continue
                for threshold in threshold_grid[stage]:
                    if threshold <= current_value:
                        continue
                    contracts.append({
                        "stage": stage,
                        "threshold": threshold,
                        "target_year": target_year,
                        "probability": round(float(np.mean(samples >= threshold)), 4),
                        "label": f"{stage.title()} reaches {threshold}% by {target_year}",
                    })
        contracts.sort(key=lambda item: item["probability"], reverse=True)

        observed_years = sorted(int(year) for year in (regional_yearly.get("years") or []) if int(year))
        latest_observed_year = max(observed_years, default=latest_hard_year)
        region_histories = regional_yearly.get("region_histories", {}) or {}
        regions = regional_yearly.get("regions", []) or sorted(region_histories)
        all_years = sorted(set(observed_years) | set(forecast_years))

        observed_years_by_stage = defaultdict(set)
        forecast_years_by_stage = defaultdict(set)
        rows_by_year: dict[str, list[dict]] = {}
        estimated_histories: dict[str, dict] = {}

        regional_stage_models = {}
        for stage in stages:
            step_observations: dict[str, float] = {}
            gap_previous: list[float] = []
            gap_current: list[float] = []
            region_anchor_lookup: dict[str, list[tuple[int, float, float]]] = {}
            for region in regions:
                cascade_history = region_histories.get(region, {}).get("cascade", []) or []
                anchors = []
                for row in cascade_history:
                    year = int(row.get("year") or 0)
                    if year not in national_stage_values[stage]:
                        continue
                    value = _parse_numeric(row.get(stage))
                    if value is None:
                        continue
                    gap = float(value) - float(national_stage_values[stage][year])
                    anchors.append((year, float(value), gap))
                anchors.sort(key=lambda item: item[0])
                region_anchor_lookup[region] = anchors
                if len(anchors) >= 2:
                    step_observations[region] = float(anchors[-1][2] - anchors[-2][2])
                    for previous_anchor, current_anchor in zip(anchors[:-1], anchors[1:]):
                        gap_previous.append(previous_anchor[2])
                        gap_current.append(current_anchor[2])

            if len(gap_previous) >= 2 and float(np.var(gap_previous, ddof=0)) > 1e-9:
                phi_value = float(np.cov(np.asarray(gap_previous), np.asarray(gap_current), ddof=0)[0, 1] / np.var(gap_previous, ddof=0))
                phi_value = float(np.clip(phi_value, 0.15, 0.92))
            else:
                phi_value = 0.65

            hierarchical = _hierarchical_step_mcmc(
                step_observations,
                draws=posterior_draws,
                burnin=burnin,
                thin=thin,
                rng=np.random.default_rng(100 + len(step_observations)),
            )
            regional_stage_models[stage] = {
                "phi": phi_value,
                "hierarchical": hierarchical,
                "anchors": region_anchor_lookup,
            }

        latest_leakage_year = max((int(year) for year in anomaly_yearly.get("leakage_by_year", {}).keys()), default=None)
        latest_leakage_rows = anomaly_yearly.get("leakage_by_year", {}).get(str(latest_leakage_year), []) if latest_leakage_year else []
        latest_leakage_lookup = {row.get("region"): row for row in latest_leakage_rows}
        regional_rng = np.random.default_rng(90210)
        watch_year = 2030 if 2030 in forecast_years else (forecast_years[-1] if forecast_years else latest_hard_year)

        for region in regions:
            observed_lookup = {
                int(row.get("year") or 0): row
                for row in (region_histories.get(region, {}).get("cascade", []) or [])
                if int(row.get("year") or 0)
            }
            region_rows = []
            region_paths = {}
            model_summary = {}

            for stage in stages:
                stage_model = regional_stage_models[stage]
                anchors = list(stage_model["anchors"].get(region, []))
                hier = stage_model["hierarchical"]
                phi_value = float(stage_model["phi"])
                latest_anchor_year = max((anchor[0] for anchor in anchors), default=None)
                latest_anchor_value = anchors[-1][1] if anchors else None
                latest_anchor_gap = anchors[-1][2] if anchors else None

                path_cache = {"paths": [], "by_year": {}, "reach_probabilities": {}}
                if anchors and latest_anchor_year is not None and latest_anchor_year < forecast_end_year:
                    forecast_year_lookup: dict[int, list[float]] = defaultdict(list)
                    reach_years: list[int | None] = []
                    theta_draws = hier["theta_draws"].get(region)
                    for draw_index in range(posterior_draws):
                        if theta_draws is not None and len(theta_draws):
                            theta = float(theta_draws[draw_index])
                        else:
                            theta = float(hier["mu_draws"][draw_index] + regional_rng.normal(0.0, math.sqrt(max(float(hier["tau2_draws"][draw_index]), 1e-6))))
                        theta = float(np.clip(theta, -6.0, 6.0))
                        sigma_step = math.sqrt(max(float(hier["sigma2_draws"][draw_index]), 1e-6))
                        gap = float(latest_anchor_gap)
                        path = [(latest_anchor_year, float(latest_anchor_value))]
                        reached_year = latest_anchor_year if float(latest_anchor_value) >= 95.0 else None
                        for forward_year in forecast_years:
                            national_forward_value = national_stage_values.get(stage, {}).get(forward_year)
                            if national_forward_value is None:
                                continue
                            gap = float(np.clip(phi_value * gap + theta + regional_rng.normal(0.0, sigma_step), -55.0, 55.0))
                            value = float(np.clip(float(national_forward_value) + gap, 0.0, 100.0))
                            path.append((forward_year, value))
                            forecast_year_lookup[forward_year].append(value)
                            if reached_year is None and value >= 95.0:
                                reached_year = forward_year
                        if draw_index < exported_paths:
                            path_cache["paths"].append({
                                "draw": draw_index + 1,
                                "years": [int(year) for year, _ in path],
                                "values": [round(float(value), 1) for _, value in path],
                            })
                        reach_years.append(reached_year)

                    path_cache["by_year"] = {
                        forward_year: {
                            "median": float(np.median(values)),
                            "lower": _safe_quantile(values, 0.1),
                            "upper": _safe_quantile(values, 0.9),
                        }
                        for forward_year, values in forecast_year_lookup.items()
                        if values
                    }
                    for forward_year in forecast_years:
                        if not reach_years:
                            continue
                        reached = sum(1 for value in reach_years if value is not None and value <= forward_year)
                        path_cache["reach_probabilities"][str(forward_year)] = round(reached / len(reach_years), 4)

                for year in all_years:
                    row = next((item for item in region_rows if int(item["year"]) == year), None)
                    if row is None:
                        row = {"year": year, "region": region}
                        region_rows.append(row)
                    national_value = national_stage_values.get(stage, {}).get(year)
                    if national_value is None:
                        row[stage] = None
                        row[f"{stage}_status"] = "missing"
                        row[f"{stage}_lower"] = None
                        row[f"{stage}_upper"] = None
                        continue
                    observed_row = observed_lookup.get(year)
                    observed_value = _parse_numeric((observed_row or {}).get(stage))
                    if observed_value is not None:
                        row[stage] = float(observed_value)
                        row[f"{stage}_status"] = "observed"
                        row[f"{stage}_lower"] = float(observed_value)
                        row[f"{stage}_upper"] = float(observed_value)
                        row[f"{stage}_period"] = (observed_row or {}).get(f"{stage}_period", str(year))
                        row[f"{stage}_source_url"] = (observed_row or {}).get(f"{stage}_source_url", "")
                        row[f"{stage}_filename"] = (observed_row or {}).get(f"{stage}_filename", "")
                        observed_years_by_stage[stage].add(year)
                    elif year in path_cache["by_year"]:
                        stats = path_cache["by_year"][year]
                        row[stage] = round(float(stats["median"]), 1)
                        row[f"{stage}_status"] = "forecast"
                        row[f"{stage}_lower"] = round(float(stats["lower"]), 1)
                        row[f"{stage}_upper"] = round(float(stats["upper"]), 1)
                        row[f"{stage}_period"] = str(year)
                        row[f"{stage}_source_url"] = ""
                        row[f"{stage}_filename"] = "Experimental Bayesian forecast"
                        forecast_years_by_stage[stage].add(year)
                    else:
                        row[stage] = None
                        row[f"{stage}_status"] = "missing"
                        row[f"{stage}_lower"] = None
                        row[f"{stage}_upper"] = None

                region_paths[stage] = path_cache["paths"]
                theta_draws = stage_model["hierarchical"]["theta_draws"].get(region, np.asarray([], dtype=float))
                step_posterior = theta_draws.tolist() if theta_draws.size else []
                model_summary[stage] = {
                    "anchor_years": [anchor[0] for anchor in anchors],
                    "latest_anchor_year": latest_anchor_year,
                    "step_mean": round(float(np.mean(theta_draws)) if theta_draws.size else float(stage_model["hierarchical"]["mu_mean"]), 2),
                    "global_step_mean": round(float(stage_model["hierarchical"]["mu_mean"]), 2),
                    "sigma": round(float(stage_model["hierarchical"]["sigma_mean"]), 2),
                    "draws": posterior_draws,
                    "phi": round(phi_value, 2),
                    "reach_probabilities": path_cache["reach_probabilities"],
                    "status": "anchored_bayesian" if anchors else "unavailable",
                    "step_interval": [
                        round(_safe_quantile(step_posterior, 0.1, default=stage_model["hierarchical"]["mu_mean"]), 2),
                        round(_safe_quantile(step_posterior, 0.9, default=stage_model["hierarchical"]["mu_mean"]), 2),
                    ],
                }

            for row in region_rows:
                leak = latest_leakage_lookup.get(region) if int(row["year"]) == latest_leakage_year else None
                if leak:
                    row["leakage_burden"] = float(leak.get("ltfu", 0.0)) + float(leak.get("not_on_treatment", 0.0))
                    row["leakage_status"] = "observed"
                else:
                    row["leakage_burden"] = None
                    row["leakage_status"] = "missing"
            region_rows.sort(key=lambda row: int(row["year"]))
            estimated_histories[region] = {
                "cascade": region_rows,
                "paths": region_paths,
                "model": model_summary,
            }
            for row in region_rows:
                if any(row.get(stage) is not None for stage in stages):
                    rows_by_year.setdefault(str(int(row["year"])), []).append(row)

        for year, rows in rows_by_year.items():
            rows.sort(
                key=lambda row: (
                    0 if all(row.get(f"{stage}_status") == "observed" for stage in stages) else 1,
                    sum(max(95.0 - float(row.get(stage) or 0.0), 0.0) for stage in stages),
                )
            )

        observed_stage_years = {stage: sorted(values) for stage, values in observed_years_by_stage.items()}
        for stage in stages:
            observed_stage_years.setdefault(stage, [])
        forecast_stage_years = {stage: sorted(values) for stage, values in forecast_years_by_stage.items()}
        for stage in stages:
            forecast_stage_years.setdefault(stage, [])

        regional_watchlist = []
        for region in regions:
            history = estimated_histories.get(region, {}).get("cascade", [])
            target_row = next((row for row in history if int(row.get("year") or 0) == watch_year), None)
            if not target_row:
                continue
            shortfalls = {
                stage: max(95.0 - float(target_row.get(stage) or 0.0), 0.0)
                for stage in stages
            }
            leak_row = latest_leakage_lookup.get(region, {})
            leak_alive = float(leak_row.get("alive", 0.0) or 0.0)
            leak_ltfu = float(leak_row.get("ltfu", 0.0) or 0.0)
            leak_off = float(leak_row.get("not_on_treatment", 0.0) or 0.0)
            leak_total = leak_alive + leak_ltfu + leak_off
            leakage_rate = ((leak_ltfu + leak_off) / leak_total * 100.0) if leak_total > 0 else 0.0
            mean_gap = float(np.mean(list(shortfalls.values())))
            bottleneck_stage = max(shortfalls, key=shortfalls.get)
            risk_score = mean_gap + leakage_rate * 0.35
            regional_watchlist.append({
                "region": region,
                "target_year": watch_year,
                "diagnosis": round(float(target_row.get("diagnosis") or 0.0), 1),
                "treatment": round(float(target_row.get("treatment") or 0.0), 1),
                "suppression": round(float(target_row.get("suppression") or 0.0), 1),
                "mean_gap": round(mean_gap, 1),
                "bottleneck_stage": bottleneck_stage,
                "leakage_rate": round(leakage_rate, 1),
                "risk_score": round(risk_score, 1),
            })
        regional_watchlist.sort(key=lambda row: row["risk_score"], reverse=True)

        return {
            "years": all_years,
            "default_year": latest_hard_year,
            "default_region": "Region 6" if "Region 6" in regions else (regions[0] if regions else ""),
            "rows_by_year": rows_by_year,
            "regions": regions,
            "region_histories": estimated_histories,
            "observed_years_by_stage": observed_stage_years,
            "estimated_years_by_stage": {stage: [] for stage in stages},
            "forecast_years_by_stage": forecast_stage_years,
            "latest_observed_year": latest_observed_year,
            "latest_hard_year": latest_hard_year,
            "forecast_end_year": forecast_end_year,
            "coverage_note": f"Observed annual national seed values currently run through {latest_hard_year}. Observed regional anchors currently span {min(observed_years) if observed_years else latest_hard_year}-{latest_observed_year}. Forecasts begin after the latest hard year and remain fully forward-looking.",
            "model_note": "Experimental forecasts use a Bayesian national transition model on diagnosis, treatment, suppression, and estimated PLHIV growth, then propagate hierarchical regional gaps around that national path. This layer remains exploratory and separate from the official atlas.",
            "source_policy": [
                "Observed regional cascade values always override model forecasts.",
                "National experimental seed values are always taken from the latest hard annual official HARP year available in the normalized layer.",
                "Diagnosis, treatment, and suppression are fit jointly as bounded transition processes and then simulated forward only after the latest hard year.",
                "Regional forecasts use hierarchical Bayesian gap dynamics anchored to observed region-level departures from the national path.",
                "No historical regional backfill is shown in this layer.",
            ],
            "national": {
                "latest_hard_year": latest_hard_year,
                "forecast_years": forecast_years,
                "observed": national_observed_rows,
                "forecast": national_forecast_rows,
                "paths": {
                    "stages": stage_path_cache,
                    "counts": count_path_cache,
                },
                "target_probabilities": national_target_probabilities,
                "bottlenecks": national_bottlenecks,
                "person_bottlenecks": national_person_bottlenecks,
                "first_hit_probabilities": first_hit_probabilities,
            },
            "regional": {
                "default_region": "Region 6" if "Region 6" in regions else (regions[0] if regions else ""),
                "watch_year": watch_year,
                "region_histories": estimated_histories,
            },
            "simulation": {
                "engine": "Bayesian MCMC transition model with hierarchical regional gaps",
                "latest_hard_year": latest_hard_year,
                "regional_anchor_year": latest_observed_year,
                "draws": posterior_draws,
                "forecast_start_year": forecast_years[0] if forecast_years else latest_hard_year,
                "forecast_end_year": forecast_end_year,
                "future_only": True,
            },
            "outputs": {
                "contracts": contracts,
                "regional_watchlist": regional_watchlist[:10],
                "national_target_probabilities": national_target_probabilities,
                "bottleneck_probabilities": national_bottlenecks,
                "person_bottleneck_probabilities": national_person_bottlenecks,
                "first_hit_probabilities": first_hit_probabilities,
            },
        }

    def _build_methodology(self, series: dict) -> dict:
        sections = [
            {
                "id": "national_cascade",
                "figure_key": "national_cascade",
                "title": "National 95-95-95 board",
                "question": "How close is the Philippines to the UNAIDS 95-95-95 targets?",
                "definition": "The national cascade compares diagnosed PLHIV, PLHIV on ART, and virally suppressed PLHIV on ART against the 95% target.",
                "coverage_window": "Published annual year-end context: 2018-2025, using the official DOH/HARP national accomplishment table shown in the 2025 year-end Philippines HIV treatment presentation.",
                "estimation_policy": "No synthetic values are injected into the published national cascade. The board uses observed official annual rows only.",
                "formulas": [
                    "1st 95 = diagnosed PLHIV / estimated PLHIV * 100",
                    "2nd 95 = PLHIV on ART / diagnosed PLHIV * 100",
                    "3rd 95 = virally suppressed PLHIV on ART / PLHIV on ART * 100",
                    "Undiagnosed people = estimated PLHIV - diagnosed PLHIV",
                    "Diagnosed but not on ART = diagnosed PLHIV - PLHIV on ART",
                    "On ART but not suppressed = PLHIV on ART - virally suppressed PLHIV on ART",
                ],
                "source_precedence": [
                    "Use the DOH/HARP year-end national accomplishment table for annual 2018-2025 cascade context and counts.",
                    "Use WHO only as an external validation checkpoint when the same national endpoint is also present in an official Philippines source.",
                    "Do not mix the older suppression-among-tested metric into the 3rd-95 line.",
                ],
                "construction": [
                    "The scorecards use the latest annual official row, 2025.",
                    "The observed trajectory uses the annual year-end HARP percentages from 2018 through 2025 for all three cascade stages.",
                    "The companion stage-count panel shows the observed 2025 counts directly: estimated PLHIV, diagnosed PLHIV, PLHIV on ART, and virally suppressed PLHIV on ART.",
                    "The 3rd 95 is corrected to virally suppressed among PLHIV on ART. It is not the higher suppression-among-tested metric.",
                ],
                "harmonization": [
                    "The publication view starts in 2018 because that is the earliest year visible in the official HARP accomplishment table now used as the anchor for all three cascade stages.",
                    "The trajectory and stage-count panel use the same annual source row so percentages and people stay internally consistent.",
                ],
                "caveats": [
                    "The publication board no longer shows quarterly points in the main trajectory panel. Quarterly surveillance remains available elsewhere for validation and interpretation.",
                    "Older official SHIP viral-load figures that publish suppression among those tested are excluded from the 3rd-95 annual line because they use a different denominator.",
                ],
                "reference_ids": ["unaids-dataset", "harp-annual-2018-2025", "ship-2023-q2", "ship-2023-q3", "ship-2023-q4", "ship-2024-q4", "ship-2025-q1", "ship-2025-q2", "ship-2025-q3", "ship-2025-q4", "who-2025-release"],
            },
            {
                "id": "regional_ladder",
                "figure_key": "regional_ladder",
                "title": "Regional stage matrix and yearly explorer",
                "question": "Which regions are closest to the 95-95-95 target, and how do regional stage values compare within one observed year?",
                "definition": "The publication figure is a stage matrix for the latest observed yearly regional snapshot. The explorer extends that same observed yearly layer into year and region selectors.",
                "coverage_window": "Yearly regional cascade coverage currently spans 2024-2025. The selected-region comparison shows the same yearly window because earlier official region-level 95-95-95 tables were not found in the reviewed corpus.",
                "estimation_policy": "No synthetic or fitted regional cascade percentages are injected into the publication figures. If an official region-level cascade table is absent for a year, the year remains unavailable in the explorer.",
                "formulas": [
                    "Diagnosis coverage = diagnosed PLHIV / estimated PLHIV * 100, using the percentage published in the source region-level cascade table.",
                    "Treatment coverage = PLHIV on ART / diagnosed PLHIV * 100, using the percentage published in the source region-level cascade table.",
                    "Suppression coverage = virally suppressed PLHIV on ART / PLHIV on ART * 100, using the percentage published in the source region-level cascade table.",
                    "Mean gap to target = average of (95 - diagnosis), (95 - treatment), and (95 - suppression).",
                ],
                "source_precedence": [
                    "Use official SHIP regional care-cascade annex tables when all three stages are published for the year.",
                    "Within a year, keep the latest quarter with complete diagnosis, treatment, and suppression percentages by region.",
                    "Do not backfill pre-2024 regional cascade percentages when the source table is absent.",
                ],
                "construction": [
                    "For each region, metric, and year, the app selects the latest structured quarter in that year.",
                    "Regions are included in the publication stage matrix only when all three cascade stages are present for that year.",
                    "The publication matrix orders regions by average gap to the 95 target and shows the three stage values directly in cells rather than in a separate ladder or gap companion plot.",
                    "The explorer compares annual latest diagnosis, treatment, and suppression values for the selected region using zero-based dumbbell comparisons.",
                    "A separate regional burden line was intentionally excluded from the overview because the current yearly regional burden layer is too sparse to support a headline comparison.",
                ],
                "harmonization": [
                    "The yearly selector is annual only. Quarterly roll-ups are intentionally hidden in this view.",
                    "The regional explorer uses only years with observed official regional cascade values. It does not stretch the x-axis back to years where no regional table exists.",
                ],
                "caveats": [
                    "Structured region-level cascade coverage currently starts in 2024 because the older official reports reviewed here do not publish a complete region-level 95-95-95 denominator table.",
                    "Reviewed older official reports, including 2019 Q4, October 2021, and December 2022, publish national ART totals and facility lists but not a region-level treatment-outcome table that can support yearly regional cascade backfill.",
                ],
                "reference_ids": ["ship-2019-q4", "ship-2021-oct", "ship-2022-dec", "ship-2024-q2", "ship-2024-q4", "ship-2025-q2", "ship-2025-q3", "ship-2025-q4"],
            },
            {
                "id": "anomaly_board",
                "figure_key": "anomaly_board",
                "title": "Performance versus treatment burden",
                "question": "Which regions perform above or below the fitted cascade pattern once treatment leakage burden is taken into account?",
                "definition": "Residuals compare observed regional coverage with the fitted regional relationship. Loss-from-care burden is shown against those residuals so underperformance and treatment leakage can be read in one frame.",
                "coverage_window": "Residuals are available for 2024-2025 because those are the years with region-level cascade percentages. Treatment leakage is available for 2023-2025 because 2023 can be backfilled from the official year-end SHIP treatment-outcome table.",
                "estimation_policy": "No synthetic residuals or synthetic treatment leakage values are injected. Residuals are computed only when observed region-level cascade percentages exist. Leakage is shown only when an observed treatment-outcome table or explicit official year-end treatment categories are available.",
                "formulas": [
                    "Treatment residual = observed treatment coverage - fitted treatment coverage from the diagnosis-to-treatment line",
                    "Suppression residual = observed suppression coverage - fitted suppression coverage from the treatment-to-suppression line",
                    "Documented treatment leakage = lost to follow-up + other documented off-treatment burden",
                    "For 2024 onward, other documented off-treatment burden = not on treatment from the structured treatment-outcome table",
                    "For 2023, other documented off-treatment burden = transout (overseas) + stopped from the official year-end treatment-outcome table",
                ],
                "source_precedence": [
                    "Residuals use yearly regional cascade rows only where official region-level cascade tables exist.",
                    "Leakage uses the structured treatment-outcome observations when they exist in the normalized layer.",
                    "If 2023 has no structured leakage rows, backfill from the official 2023 year-end SHIP treatment-outcome table instead of inferring values.",
                ],
                "construction": [
                    "Residuals are recomputed inside each year using the regions available in that year.",
                    "The publication panel positions each region by residual and loss-from-care rate, with point area scaling to total documented loss from care.",
                    "The explorer uses the same quadrant and summary cards instead of a second full-size leakage ranking chart.",
                ],
                "harmonization": [
                    "The anomaly explorer is annual only. Each year represents the latest structured comparable quarter inside that year.",
                    "Residual labels are rewritten into plain English rather than internal cascade shorthand.",
                    "Treatment leakage component names are harmonized across years so 2023 transout/stopped can be compared cautiously with 2024 onward not-on-treatment reporting.",
                ],
                "caveats": [
                    "Residuals are unavailable before 2024 because the older official reports reviewed here do not publish complete region-level cascade percentages.",
                    "Leakage is only partially comparable across years because 2023 publishes transout/stopped while 2024 onward publishes not-on-treatment.",
                    "Residuals are descriptive diagnostics, not causal estimates.",
                    "Older official reports reviewed for 2019 Q4, October 2021, and December 2022 do not support a defensible pre-2024 regional residual series because they lack a full region-level cascade denominator table.",
                ],
                "reference_ids": ["ship-2019-q4", "ship-2021-oct", "ship-2022-dec", "ship-2023-q4", "ship-2024-q2", "ship-2025-q4"],
            },
            {
                "id": "historical_board",
                "figure_key": "historical_board",
                "title": "Long-run burden and exposure shift",
                "question": "How has national HIV burden changed over time, and which long-run series have the strongest evidence coverage?",
                "definition": "The historical board combines direct source extraction from surveillance reports with official annual international estimates.",
                "coverage_window": "Published on a common 2015-2025 annual axis. Some underlying direct-extraction series begin before 2015, but the board uses a common presentation window to align with the cascade and key-population figures.",
                "estimation_policy": "No historical interpolation is applied. If a year fails consistency checks or the source cannot be resolved, the year remains missing rather than being smoothed or inferred.",
                "formulas": [
                    "Cumulative reported HIV cases = latest annual end-of-year cumulative count extracted directly from the Philippines surveillance corpus.",
                    "People living with HIV = official annual UNAIDS estimate for the Philippines.",
                    "New HIV infections = official annual UNAIDS estimate for the Philippines.",
                    "AIDS-related deaths = official annual UNAIDS estimate for the Philippines.",
                ],
                "source_precedence": [
                    "Use official annual UNAIDS estimates for PLHIV, new HIV infections, and AIDS-related deaths.",
                    "Use direct source extraction from the Philippines surveillance corpus for cumulative reported HIV cases where the official annual estimate is a different construct.",
                    "Suppress extracted historical series that fail consistency checks or contain unresolvable OCR noise.",
                ],
                "construction": [
                    "Cumulative reported HIV cases are built from direct extraction of annual end-of-year surveillance values from the local corpus.",
                    "PLHIV, new HIV infections, and AIDS-related deaths use official annual UNAIDS Philippines values.",
                    "Sexual-contact share is annualized from surveillance series and low-confidence outliers are suppressed.",
                ],
                "harmonization": [
                    "The publication view uses a common 2015-2025 annual window so the national historical panels align with the cascade and subgroup figures.",
                    "Missing years remain visible as gaps rather than being interpolated or backfilled.",
                ],
                "caveats": [
                    "Historical panels mix direct surveillance counts and international estimates, but each panel uses only one definition at a time.",
                ],
                "reference_ids": ["unaids-dataset", "ship-local-corpus"],
            },
            {
                "id": "key_populations_board",
                "figure_key": "key_populations_board",
                "title": "Key population sentinel panels",
                "question": "What does the current evidence base show for pregnant women, TGW, OFW, and youth-linked burden?",
                "definition": "Each panel shows the annual latest structured value for that subgroup, with missing years kept visible rather than interpolated away.",
                "coverage_window": "Published on a shared 2015-2025 axis. Underlying subgroup evidence windows differ: pregnant cumulative is strongest from 2017 onward, TGW cumulative from 2019 onward, OFW cumulative from 2010 onward, and youth share from 2015 onward.",
                "estimation_policy": "No subgroup gaps are artificially filled. The figures keep years without defensible observed values blank so the evidence window for each subgroup remains explicit.",
                "formulas": [
                    "Pregnant women diagnosed (cumulative) = annual latest cumulative number of diagnosed women reported pregnant at diagnosis.",
                    "TGW diagnosed (cumulative) = annual latest cumulative number of diagnosed transgender women in the structured series.",
                    "OFW cumulative burden = annual latest cumulative number of reported overseas Filipino workers among diagnosed cases.",
                    "Youth share of reported cases = annual latest percent of reported cases aged 15-24.",
                ],
                "source_precedence": [
                    "Use structured national subgroup observations first when the normalized layer contains a defensible annual latest value.",
                    "Backfill from direct source extraction in the local Philippines surveillance corpus when the normalized layer is missing a subgroup series.",
                    "Keep gaps visible when neither structured observations nor direct extraction produce a defensible yearly value.",
                ],
                "construction": [
                    "Pregnant women diagnosed uses the annual latest cumulative value taken from quarterly surveillance rows.",
                    "TGW diagnosed uses the annual latest cumulative value from the surveillance series.",
                    "OFW cumulative burden uses direct source extraction and annual latest cumulative counts.",
                    "Youth share uses the annual latest proportion of reported cases among people aged 15-24.",
                ],
                "harmonization": [
                    "All subgroup panels share the same 2015-2025 publication window.",
                    "Years outside a subgroup's observed range are shown as unavailable rather than visually interpolated.",
                ],
                "caveats": [
                    "Different subgroup panels have different evidence windows. A common x-axis does not imply continuous coverage.",
                ],
                "reference_ids": ["ship-local-corpus"],
            },
            {
                "id": "experimental_regional",
                "figure_key": "",
                "title": "Experimental forward scenarios",
                "question": "Given the latest hard observed national year-end cascade and the current regional anchors, what future national and regional paths are plausible, and which cascade stage is most likely to remain the bottleneck?",
                "definition": "The experimental layer keeps all observed rows intact, seeds the model from the latest hard official national year, fits Bayesian MCMC transition models for diagnosis, treatment, suppression, and estimated PLHIV growth, then projects hierarchical regional departures around that national path for exploration only.",
                "coverage_window": "Observed national seed values currently run through the latest hard official annual HARP year available in the normalized layer. Observed regional anchors span only the exported observed window. Forecast years begin after the latest hard year and extend through the exported horizon.",
                "estimation_policy": "This layer is forward-looking only. It does not backfill or overwrite any observed historical row. National forecasts are drawn from posterior Bayesian transition samples; regional forecasts are drawn from hierarchical Bayesian gap samples around the national path; outputs are reported as distributions, contracts, bottleneck probabilities, and regional watchlists rather than single deterministic values.",
                "formulas": [
                    "Delta logit(stage_t) ~ Normal(X_t beta_stage, sigma_stage^2) with posterior draws from Bayesian regression",
                    "logit(stage_t+1) = logit(stage_t) + mean-reverting Bayesian stage increment",
                    "Delta log(estimated PLHIV_t) ~ Normal(X_t beta_growth, sigma_growth^2)",
                    "estimated PLHIV_t+1 = estimated PLHIV_t * exp(mean-reverting Bayesian growth increment)",
                    "diagnosed count_t = estimated PLHIV_t * diagnosis_t",
                    "on ART count_t = diagnosed count_t * treatment_t",
                    "suppressed count_t = on ART count_t * suppression_t",
                    "regional gap_t+1 = phi * regional gap_t + region-specific Bayesian step + shock",
                    "regional stage_t = national stage_t + simulated regional gap_t",
                    "reach probability by year = share of simulated paths at or above threshold by that year",
                    "dominant bottleneck = stage with the largest remaining shortfall from 95 in a simulated year",
                ],
                "source_precedence": [
                    "Observed regional yearly cascade values always override forecasts.",
                    "The national seed is always the latest hard official annual HARP year present in the normalized layer.",
                    "Official annual national stage and count values anchor the forward national simulation.",
                    "Regional forecasts are derived only after the national simulation is built.",
                    "No historical synthetic backfill is shown in this layer.",
                ],
                "construction": [
                    "Extract official annual national diagnosis, treatment, suppression, estimated PLHIV, diagnosed PLHIV, PLHIV on ART, and suppressed counts.",
                    "Fit Bayesian transition regressions on annual diagnosis, treatment, and suppression movement on the logit scale.",
                    "Fit a Bayesian annual growth model for estimated PLHIV counts on the log scale.",
                    "Simulate forward national paths from posterior draws with cross-stage residual correlation and mean reversion from the latest hard year.",
                    "Use the simulated national stage paths as the common forward scaffold for all regions.",
                    "Fit hierarchical Bayesian region-level yearly gap steps relative to the national path using observed region anchors only.",
                    "Simulate forward regional gaps and combine them with the national path to produce exploratory regional scenarios.",
                    "Summarize the simulated future using percentile bands, reach-to-threshold probabilities, bottleneck attribution, and regional risk watchlists.",
                ],
                "harmonization": [
                    "Observed, forecast, and estimated outputs remain separate in both the payload and the UI.",
                    "National and regional simulations are forward-only and begin after the latest hard observed year.",
                    "All figure exports and summaries explicitly label this layer as experimental and non-official.",
                ],
                "caveats": [
                    "Observed regional yearly coverage is still sparse, so regional futures are weakly anchored compared with the national simulation.",
                    "The national engine is a Bayesian transition-and-gap model, not a full causal or policy-response model.",
                    "Bottleneck probabilities and target contracts are scenario outputs, not official forecasts.",
                    "No synthetic value is allowed to replace an observed annual or regional row.",
                ],
                "reference_ids": ["unaids-dataset", "harp-annual-2018-2025", "ship-2024-q2", "ship-2024-q4", "ship-2025-q2", "ship-2025-q3", "ship-2025-q4"],
            },
        ]
        return {
            "sections": sections,
            "by_figure": {section["figure_key"]: section for section in sections},
        }

    def _build_references(self, series: dict) -> dict:
        items = [
            {
                "id": "unaids-dataset",
                "title": "UNAIDS AIDSinfo dataset",
                "organization": "UNAIDS",
                "kind": "Official annual dataset",
                "url": "https://aidsinfo.unaids.org/dataset",
                "used_in": ["national_cascade", "historical_board"],
                "note": "Annual Philippines values for the first 95, second 95, people living with HIV, new HIV infections, and AIDS-related deaths.",
            },
            {
                "id": "harp-annual-2018-2025",
                "title": "Year-end national accomplishment against the 95-95-95 targets, 2018-2025",
                "organization": "Department of Health / HARP",
                "kind": "Official annual accomplishment table",
                "url": "",
                "used_in": ["national_cascade"],
                "note": "Transcribed from the official DOH/HARP annual accomplishment table provided by the user. Source file in repo: data/normalized/official_harp_95_2018_2025.csv.",
            },
            {
                "id": "who-2025-release",
                "title": "UNAIDS and WHO support DOH’s call for urgent action as the Philippines faces the fastest-growing HIV surge in Asia-Pacific",
                "organization": "WHO Philippines",
                "kind": "Official checkpoint article",
                "url": "https://www.who.int/philippines/news/detail/11-06-2025-unaids--who-support-doh-s-call-for-urgent-action-as-the-philippines-faces-the-fastest-growing-hiv-surge-in-the-asia-pacific-region",
                "used_in": ["national_cascade"],
                "note": "Used as an official cross-check for the 2025 Q1 cascade values.",
            },
            {
                "id": "ship-2023-q2",
                "title": "2023 Q2 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://drive.google.com/file/d/1k4i8dIi1WNnb5O-WuVanDGO4LbyzkQyu/view",
                "used_in": ["national_cascade"],
                "note": "Official 2023 Q2 checkpoint used in the quarterly national cascade.",
            },
            {
                "id": "ship-2023-q3",
                "title": "2023 Q3 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://drive.google.com/file/d/1QHHNlde6jmJx4rR7JhiZkROb9yOMmgTE/view",
                "used_in": ["national_cascade"],
                "note": "Official 2023 Q3 checkpoint used in the quarterly national cascade.",
            },
            {
                "id": "ship-2023-q4",
                "title": "2023 Q4 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://drive.google.com/file/d/1DOo4eEzBnoamfdzt8Bmj78b8kRNEQqjz/view",
                "used_in": ["national_cascade"],
                "note": "Official 2023 Q4 checkpoint used in the quarterly national cascade.",
            },
            {
                "id": "ship-2022-dec",
                "title": "December 2022 HIV/AIDS & ART registry of the Philippines (HARP)",
                "organization": "Department of Health / Epidemiology Bureau",
                "kind": "Official monthly surveillance report reviewed for backfill feasibility",
                "url": "https://drive.google.com/file/d/1GZawV3hka96kcGtesOWsAXWPipApDlk2/view",
                "used_in": ["regional_ladder", "anomaly_board"],
                "note": "Reviewed during regional treatment-history mining. Provides national PLHIV on ART context but no region-level treatment-outcome table usable for yearly regional backfill.",
            },
            {
                "id": "ship-2021-oct",
                "title": "October 2021 HIV/AIDS & ART registry of the Philippines (HARP)",
                "organization": "Department of Health / Epidemiology Bureau",
                "kind": "Official monthly surveillance report reviewed for backfill feasibility",
                "url": "https://drive.google.com/file/d/1qnv2JBb_wZsO_QXtLPS1HkKtdbYLUPNF/view",
                "used_in": ["regional_ladder", "anomaly_board"],
                "note": "Reviewed during regional treatment-history mining. Provides national PLHIV on ART totals and case distribution, but no region-level treatment-outcome table usable for yearly regional backfill.",
            },
            {
                "id": "ship-2019-q4",
                "title": "October-December 2019 HIV/AIDS & ART registry of the Philippines (HARP)",
                "organization": "Department of Health / Epidemiology Bureau",
                "kind": "Official quarterly surveillance report reviewed for backfill feasibility",
                "url": "https://drive.google.com/file/d/1XgUU5yRqmJO51WFoOt9RgOh4voZL-X-i/view",
                "used_in": ["regional_ladder", "anomaly_board"],
                "note": "Reviewed during regional treatment-history mining. The quarter-end report contains national ART context and treatment-hub listings, but not a region-level treatment-outcome table that can support yearly cascade backfill.",
            },
            {
                "id": "ship-2024-q1",
                "title": "2024 Q1 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://www.ship.ph/wp-content/uploads/2025/11/2024_Q1-HIV-AIDS-Surveillance-of-the-Philippines.pdf",
                "used_in": ["national_cascade"],
                "note": "Official 2024 Q1 quarterly cascade checkpoint used in the national 95-95-95 board.",
            },
            {
                "id": "ship-2024-q2",
                "title": "2024 Q2 HIV/AIDS surveillance reporting of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://drive.google.com/file/d/1J70r6TsqtY9--UeccT1sDMRQ3neRO7e-/view",
                "used_in": ["anomaly_board", "regional_ladder"],
                "note": "Quarterly regional cascade and treatment-outcome context.",
            },
            {
                "id": "ship-2024-q4",
                "title": "2024 Q4 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://www.ship.ph/wp-content/uploads/2025/11/2024_Q4-HIV-AIDS-Surveillance-of-the-Philippines-2.pdf",
                "used_in": ["national_cascade", "regional_ladder"],
                "note": "Official 2024 year-end national and regional cascade context.",
            },
            {
                "id": "ship-2025-q1",
                "title": "2025 Q1 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://drive.google.com/file/d/1m3wEpCQTQwOk6UyrXk0GNW9xbSMgh3D-/view",
                "used_in": ["national_cascade"],
                "note": "Official 2025 Q1 quarter-end checkpoint used in the national cascade.",
            },
            {
                "id": "ship-2025-q2",
                "title": "2025 Q2 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://www.ship.ph/wp-content/uploads/2025/11/2025_Q2-HIV-AIDS-Surveillance-Report-of-the-Philippines-1.pdf",
                "used_in": ["national_cascade", "regional_ladder"],
                "note": "Official quarterly cascade series for 2025.",
            },
            {
                "id": "ship-2025-q3",
                "title": "2025 Q3 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://www.ship.ph/wp-content/uploads/2025/12/HASP-REPORT-2025_-Q3_signed-1.pdf",
                "used_in": ["national_cascade", "regional_ladder"],
                "note": "Official quarterly cascade series for 2025.",
            },
            {
                "id": "ship-2025-q4",
                "title": "2025 Q4 HIV/AIDS surveillance report of the Philippines",
                "organization": "SHIP / Department of Health",
                "kind": "Official quarterly surveillance report",
                "url": "https://www.ship.ph/wp-content/uploads/2026/02/2025_Q4-HIV-AIDS-Surveillance-Report-of-the-Philippines-2.pdf",
                "used_in": ["national_cascade", "regional_ladder", "anomaly_board"],
                "note": "Official 2025 year-end national cascade, regional cascade, and treatment-outcome snapshot.",
            },
            {
                "id": "ship-local-corpus",
                "title": "Local HIV/STI report corpus used for direct extraction",
                "organization": "Department of Health / SHIP / local corpus",
                "kind": "Local surveillance corpus",
                "url": "",
                "used_in": ["historical_board", "key_populations_board"],
                "note": "Derived long-run and subgroup panels use direct extraction from the locally stored Philippines HIV/STI surveillance corpus.",
            },
            {
                "id": "design-ahead",
                "title": "AHEAD HIV dashboard and comparison patterns",
                "organization": "Health Resources and Services Administration / AHEAD",
                "kind": "Dashboard design reference",
                "url": "https://ahead.hiv.gov/about/",
                "used_in": ["regional_ladder", "anomaly_board", "key_populations_board"],
                "note": "Used as a reference for comparative regional layouts, summary framing, and public-health dashboard interaction patterns.",
            },
            {
                "id": "design-owid",
                "title": "Our World in Data HIV/AIDS presentation patterns",
                "organization": "Our World in Data",
                "kind": "Dashboard design reference",
                "url": "https://ourworldindata.org/hiv-aids",
                "used_in": ["national_cascade", "historical_board"],
                "note": "Used as a reference for long-run small multiples, annotation density, and uncluttered epidemiology figure framing.",
            },
            {
                "id": "design-unaids-inequalities",
                "title": "UNAIDS inequalities visualization framing",
                "organization": "UNAIDS",
                "kind": "Dashboard design reference",
                "url": "https://www.unaids.org/en/resources/presscentre/featurestories/2024/may/20240520_inequalities-visualization-tool",
                "used_in": ["regional_ladder", "anomaly_board"],
                "note": "Used as a reference for regional inequality framing and simplified explanation of within-country disparities.",
            },
        ]

        local_reference_urls = {}
        for series_key in ("historical", "key_populations"):
            for value in series.get(series_key, {}).values() if isinstance(series.get(series_key), dict) else []:
                if not isinstance(value, list):
                    continue
                for row in value:
                    if not isinstance(row, dict):
                        continue
                    filename = str(row.get("filename") or "").strip()
                    source_url = str(row.get("source_url") or "").strip()
                    if filename and source_url:
                        local_reference_urls[filename] = source_url
        for region_payload in series.get("regional_yearly", {}).get("region_histories", {}).values():
            for row in region_payload.get("burden", []):
                filename = str(row.get("filename") or "").strip()
                source_url = str(row.get("source_url") or "").strip()
                if filename and source_url:
                    local_reference_urls[filename] = source_url

        local_items = [
            {
                "id": f"local::{index}",
                "title": filename,
                "organization": "Local HIV/STI corpus",
                "kind": "Source PDF used in derived series",
                "url": url,
                "used_in": ["historical_board", "key_populations_board", "regional_ladder"],
                "note": "Local report used in direct source extraction for publication figures.",
            }
            for index, (filename, url) in enumerate(sorted(local_reference_urls.items()), start=1)
        ]

        items.extend(local_items)
        groups = [
            {"title": "Official international datasets", "item_ids": ["unaids-dataset", "who-2025-release"]},
            {"title": "Official Philippines annual target tables", "item_ids": ["harp-annual-2018-2025"]},
            {"title": "Official Philippines surveillance reports", "item_ids": ["ship-2019-q4", "ship-2021-oct", "ship-2022-dec", "ship-2023-q2", "ship-2023-q3", "ship-2023-q4", "ship-2024-q1", "ship-2024-q2", "ship-2024-q4", "ship-2025-q1", "ship-2025-q2", "ship-2025-q3", "ship-2025-q4"]},
            {"title": "Local derived-series corpus", "item_ids": ["ship-local-corpus"]},
            {"title": "Dashboard design references", "item_ids": ["design-ahead", "design-owid", "design-unaids-inequalities"]},
            {"title": "Local corpus reports used in derived historical and subgroup series", "item_ids": [item["id"] for item in local_items]},
        ]
        return {
            "items": items,
            "groups": groups,
        }

    def _extract_markdown_series(self, filename_source_map: dict[str, str] | None = None) -> dict:
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
                results["cases"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(cases_value), "filename": path.name, "source_url": (filename_source_map or {}).get(path.name, "")})
            sexual_value = self._extract_sexual_share(text)
            if sexual_value is not None:
                results["sexual_share"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(sexual_value), "filename": path.name, "source_url": (filename_source_map or {}).get(path.name, "")})
            ofw_value = self._extract_ofw_count(text)
            if ofw_value:
                results["ofw"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(ofw_value), "filename": path.name, "source_url": (filename_source_map or {}).get(path.name, "")})
            youth_value = self._extract_youth_share(text)
            if youth_value is not None:
                results["youth_share"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(youth_value), "filename": path.name, "source_url": (filename_source_map or {}).get(path.name, "")})
            pregnant_value = self._extract_pregnant_cumulative(text)
            if pregnant_value:
                results["pregnant_cumulative"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(pregnant_value), "filename": path.name, "source_url": (filename_source_map or {}).get(path.name, "")})
            tgw_value = self._extract_tgw_cumulative(text)
            if tgw_value:
                results["tgw_cumulative"].append({"year": year, "label": period_label, "sort_value": sort_value, "value": float(tgw_value), "filename": path.name, "source_url": (filename_source_map or {}).get(path.name, "")})

        deduped = {}
        for key, rows in results.items():
            by_period = {}
            for row in rows:
                current = by_period.get(row["label"])
                if current is None or row["value"] > current["value"]:
                    by_period[row["label"]] = row
            deduped[key] = [by_period[label] for label in sorted(by_period, key=_period_sort_value)]
        return deduped

    def _extract_year_end_treatment_outcomes(self, filename_source_map: dict[str, str] | None = None) -> dict[int, list[dict]]:
        latest_by_year: dict[int, dict] = {}
        for path in sorted(self.processed_dir.glob("*.md")):
            if path.name.startswith("_") or path.name.endswith(".markitdown.md"):
                continue
            period_label, year, month = _infer_period_from_filename(path.name)
            if year not in {2022, 2023}:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            rows = self._parse_treatment_outcome_table(text)
            if not rows:
                continue
            sort_value = year * 100 + month
            current = latest_by_year.get(year)
            candidate = {
                "year": year,
                "period_label": period_label or f"{year}-12",
                "sort_value": sort_value,
                "filename": path.name,
                "source_url": (filename_source_map or {}).get(path.name, ""),
                "rows": rows,
            }
            if current is None or sort_value > current["sort_value"]:
                latest_by_year[year] = candidate

        extracted = {}
        for year, payload in latest_by_year.items():
            extracted[year] = [
                {
                    "region": region,
                    "alive": values["alive"],
                    "ltfu": values["ltfu"],
                    "not_on_treatment": values["other_off_treatment"],
                    "period_label": payload["period_label"],
                    "filename": payload["filename"],
                    "source_url": payload["source_url"],
                    "sort_value": payload["sort_value"],
                }
                for region, values in sorted(payload["rows"].items())
            ]
        return extracted

    def _parse_treatment_outcome_table(self, text: str) -> dict[str, dict]:
        lines = [line.strip().replace("\u00a0", " ") for line in text.splitlines() if line.strip()]
        start_index = None
        for index, line in enumerate(lines):
            if "treatment outcome" not in line.lower():
                continue
            for probe in range(index, min(index + 16, len(lines))):
                normalized = re.sub(r"\s+", "", lines[probe]).lower()
                if normalized.startswith("stopped"):
                    start_index = probe + 1
                    break
            if start_index is not None:
                break
        if start_index is None:
            return {}

        rows: dict[str, dict] = {}
        current_region = ""
        values: list[float] = []

        def finalize_region() -> None:
            nonlocal current_region, values
            if current_region and len(values) >= 2:
                alive = float(values[0])
                ltfu = float(values[1])
                transout = float(values[2]) if len(values) >= 3 else 0.0
                stopped = float(values[3]) if len(values) >= 4 else 0.0
                rows[current_region] = {
                    "alive": alive,
                    "ltfu": ltfu,
                    "other_off_treatment": transout + stopped,
                }
            current_region = ""
            values = []

        for line in lines[start_index:]:
            lower = line.lower()
            if lower.startswith("table 3:") or lower.startswith("tuberculosis") or lower.startswith("viral load") or lower.startswith("mortality"):
                break
            normalized_region = _format_region(re.sub(r"\s+", "", line.upper()))
            if normalized_region in VALID_REGION_NAMES:
                finalize_region()
                current_region = normalized_region
                continue
            if not current_region:
                continue
            compact = re.sub(r"\s+", "", line).replace(",", "")
            if compact in {"-", "--"}:
                values.append(0.0)
                continue
            if re.fullmatch(r"\d+(?:\.\d+)?", compact):
                numeric = _parse_numeric(compact)
                if numeric is not None:
                    values.append(float(numeric))
                continue
        finalize_region()
        return rows

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
        figure_pdf_path = self.figure_dir / f"{basename}.pdf"
        figure.savefig(svg_path, format="svg", bbox_inches="tight")
        figure.savefig(figure_pdf_path, format="pdf", bbox_inches="tight")
        figure.savefig(png_path, format="png", dpi=360, bbox_inches="tight")
        plt.close(figure)
        svg = svg_path.read_text(encoding="utf-8")
        return PublicationFigure(
            title=title,
            note=note,
            svg=svg,
            svg_path=str(svg_path.relative_to(self.base_dir)).replace("\\", "/"),
            png_path=str(png_path.relative_to(self.base_dir)).replace("\\", "/"),
            figure_pdf_path=str(figure_pdf_path.relative_to(self.base_dir)).replace("\\", "/"),
        )

    def _wrap_text(self, text: str, width: int = 96) -> str:
        lines = []
        for block in str(text or "").splitlines():
            block = block.strip()
            if not block:
                lines.append("")
                continue
            lines.extend(textwrap.wrap(block, width=width) or [""])
        return "\n".join(lines)

    def _write_pdf_report(self, key: str, figure: PublicationFigure, methodology: dict, references: dict) -> str:
        from PyPDF2 import PdfReader, PdfWriter

        pdf_path = self.report_dir / f"{key}_report.pdf"
        figure_pdf_path = self.base_dir / figure.figure_pdf_path
        text_pdf_path = self.report_dir / f"{key}.__text__.pdf"
        items_by_id = {item["id"]: item for item in references.get("items", [])}
        relevant_refs = [items_by_id[ref_id] for ref_id in methodology.get("reference_ids", []) if ref_id in items_by_id]

        with PdfPages(text_pdf_path) as pdf:
            text_page = plt.figure(figsize=(8.5, 11))
            text_page.patch.set_facecolor("#fffaf0")
            text_page.text(0.08, 0.96, methodology.get("title", figure.title), fontsize=18, fontweight="bold", va="top")
            y = 0.91

            blocks = [
                ("Question", methodology.get("question", "")),
                ("Definition", methodology.get("definition", "")),
                ("Coverage window", methodology.get("coverage_window", "")),
                ("Estimation and gap policy", methodology.get("estimation_policy", "")),
                ("Formulas", "\n".join(f"- {item}" for item in methodology.get("formulas", []))),
                ("Source precedence", "\n".join(f"- {item}" for item in methodology.get("source_precedence", []))),
                ("Construction", "\n".join(f"- {item}" for item in methodology.get("construction", []))),
                ("Harmonization", "\n".join(f"- {item}" for item in methodology.get("harmonization", []))),
                ("Caveats", "\n".join(f"- {item}" for item in methodology.get("caveats", []))),
                ("Figure note", figure.note),
            ]

            for heading, body in blocks:
                if not str(body or "").strip():
                    continue
                text_page.text(0.08, y, heading, fontsize=11.5, fontweight="bold", va="top")
                y -= 0.024
                wrapped = self._wrap_text(body, width=96)
                text_page.text(0.09, y, wrapped, fontsize=9.6, va="top", linespacing=1.45)
                y -= 0.018 * (wrapped.count("\n") + 2)
                if y < 0.20:
                    pdf.savefig(text_page, bbox_inches="tight")
                    plt.close(text_page)
                    text_page = plt.figure(figsize=(8.5, 11))
                    text_page.patch.set_facecolor("#fffaf0")
                    y = 0.96

            if relevant_refs:
                if y < 0.24:
                    pdf.savefig(text_page, bbox_inches="tight")
                    plt.close(text_page)
                    text_page = plt.figure(figsize=(8.5, 11))
                    text_page.patch.set_facecolor("#fffaf0")
                    y = 0.96
                text_page.text(0.08, y, "References", fontsize=11.5, fontweight="bold", va="top")
                y -= 0.028
                for ref in relevant_refs:
                    entry = (
                        f"- {ref['title']} | {ref['organization']} | {ref.get('kind', 'Reference')}.\n"
                        f"  Role: {ref.get('note', '')}\n"
                        f"  URL: {ref.get('url', '')}"
                    )
                    wrapped = self._wrap_text(entry, width=96)
                    text_page.text(0.09, y, wrapped, fontsize=9.4, va="top", linespacing=1.4)
                    y -= 0.018 * (wrapped.count("\n") + 2)
                    if y < 0.12:
                        pdf.savefig(text_page, bbox_inches="tight")
                        plt.close(text_page)
                        text_page = plt.figure(figsize=(8.5, 11))
                        text_page.patch.set_facecolor("#fffaf0")
                        y = 0.96

            pdf.savefig(text_page, bbox_inches="tight")
            plt.close(text_page)

        writer = PdfWriter()
        for source_path in (figure_pdf_path, text_pdf_path):
            reader = PdfReader(str(source_path))
            for page in reader.pages:
                writer.add_page(page)
        with pdf_path.open("wb") as handle:
            writer.write(handle)
        if text_pdf_path.exists():
            try:
                text_pdf_path.unlink()
            except (PermissionError, FileNotFoundError):
                pass

        site_pdf_path = str(pdf_path.relative_to(self.base_dir)).replace("\\", "/")
        try:
            shutil.copy2(pdf_path, self.output_pdf_dir / pdf_path.name)
        except PermissionError:
            pass
        return site_pdf_path

    def _render_national_cascade(self, data: dict) -> PublicationFigure:
        self._base_style()
        fig = plt.figure(figsize=(14.6, 5.4))
        gs = GridSpec(1, 3, figure=fig, wspace=0.18)
        short_titles = {"first_95": "1st 95: Diagnosed", "second_95": "2nd 95: On ART", "third_95": "3rd 95: Suppressed"}
        legend_handles = [
            Line2D([0], [0], color="none", linewidth=0.0, marker="o", markerfacecolor="#fffdf8", markeredgecolor="#9eb5c7", markeredgewidth=1.4, markersize=6, label="Official annual context"),
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
                ax.scatter(annual_xs, annual_ys, facecolors="#fffdf8", edgecolors="#9eb5c7", linewidth=1.3, s=34, zorder=2)
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
                0.90,
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
        note = "Hollow markers show official year-end annual context. The highlighted green line and orange dots show official quarterly surveillance observations. The third 95 uses suppression among PLHIV on ART; annual 2018-2025 context comes from the DOH/HARP year-end table."
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

    def _render_regional_fingerprint_board(self, data: dict) -> PublicationFigure:
        self._base_style()
        years = data.get("years", []) or []
        latest_year = max(years, default=None)
        latest_rows = list(data.get("rows_by_year", {}).get(str(latest_year), [])) if latest_year else []
        histories = data.get("region_histories", {}) or {}

        if not latest_rows:
            fig, ax = plt.subplots(figsize=(12.5, 4.8))
            ax.axis("off")
            ax.text(
                0.5,
                0.52,
                "No observed yearly regional cascade rows are available for a fingerprint board.",
                ha="center",
                va="center",
                fontsize=14,
                color="#50615d",
            )
            return self._save_figure(
                fig,
                "regional_fingerprint_board",
                "Regional fingerprint board",
                "Fingerprint board unavailable because no observed yearly regional cascade rows were exported.",
            )

        def _latest_burden(region: str) -> float:
            burden_rows = histories.get(region, {}).get("burden", []) or []
            if not burden_rows:
                return 0.0
            ordered = sorted(burden_rows, key=lambda row: int(row.get("year") or 0))
            return float(ordered[-1].get("value") or 0.0)

        ranked_rows = sorted(latest_rows, key=lambda row: float(row.get("mean_gap") or 999))
        best_region = ranked_rows[0]["region"]
        widest_region = ranked_rows[-1]["region"]
        burden_region = max(
            (row["region"] for row in ranked_rows),
            key=lambda region: _latest_burden(region),
            default=best_region,
        )
        exemplar_regions = []
        for region in (best_region, burden_region, widest_region):
            if region and region not in exemplar_regions:
                exemplar_regions.append(region)
        exemplar_rows = [next(row for row in ranked_rows if row["region"] == region) for region in exemplar_regions]

        fig = plt.figure(figsize=(15.2, 5.8))
        gs = GridSpec(1, len(exemplar_rows), figure=fig, wspace=0.22)
        stage_keys = [("diagnosis", "Diagnosed"), ("treatment", "On ART"), ("suppression", "Suppressed")]
        delta_label_map = {"diagnosis": "Dx", "treatment": "ART", "suppression": "Supp"}

        for idx, row in enumerate(exemplar_rows):
            ax = fig.add_subplot(gs[0, idx])
            region = row["region"]
            history = sorted(histories.get(region, {}).get("cascade", []), key=lambda item: int(item.get("year") or 0))
            previous = history[-2] if len(history) > 1 else None
            y_positions = np.arange(len(stage_keys))[::-1]

            for y, (key, label) in zip(y_positions, stage_keys):
                value = float(row.get(key) or 0)
                ax.hlines(y, 0, 100, color="#ebefea", linewidth=8.2, zorder=1)
                ax.hlines(y, 0, value, color=CASCADE_COLORS[key], linewidth=8.2, zorder=2)
                ax.scatter(value, y, color=CASCADE_COLORS[key], s=120, edgecolors="#fffaf0", linewidth=1.8, zorder=3)
                if previous and previous.get(key) is not None:
                    delta = float(row.get(key) or 0) - float(previous.get(key) or 0)
                    ax.text(
                        100,
                        y,
                        f"{delta_label_map[key]} {delta:+.0f}",
                        ha="right",
                        va="center",
                        fontsize=10,
                        color="#556863",
                        fontweight="bold",
                    )
            ax.axvline(95, color="#c4561b", linewidth=1.2, linestyle=(0, (4, 3)), zorder=1)
            ax.set_xlim(0, 100)
            ax.set_ylim(-0.8, len(stage_keys) - 0.2)
            ax.set_yticks(y_positions)
            ax.set_yticklabels([label for _, label in stage_keys], fontsize=11)
            ax.set_xticks([0, 50, 95, 100])
            ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(value)}%"))
            self._finalize_axis(ax)
            ax.set_title(region, fontsize=16, fontfamily="DejaVu Serif", loc="left", pad=12)
            ax.text(
                0.0,
                1.02,
                f"{latest_year} observed snapshot · mean gap {float(row.get('mean_gap') or 0):.1f}",
                transform=ax.transAxes,
                fontsize=9.6,
                color="#5e6a66",
            )
            if idx == 0:
                ax.set_ylabel("Cascade stage", fontsize=12, fontweight="bold")
            else:
                ax.set_ylabel("")
            ax.set_xlabel("Coverage", fontsize=12, fontweight="bold", labelpad=10)

        fig.subplots_adjust(left=0.08, right=0.98, top=0.90, bottom=0.16)
        note = (
            f"Exemplar regions use observed yearly cascade rows only for {latest_year}. "
            f"{best_region} is closest to the combined target, {widest_region} is furthest away, "
            f"and {burden_region} carries the largest observed regional burden among the displayed fingerprints."
        )
        return self._save_figure(fig, "regional_fingerprint_board", "Regional fingerprint board", note)

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
        ltfu = [row["ltfu"] for row in leak_rows][::-1]
        not_on = [row["not_on_treatment"] for row in leak_rows][::-1]
        ypos = np.arange(len(regions))
        ax_right.barh(ypos, ltfu, color=SERIES_COLORS["ltfu"], label="Lost to follow-up")
        ax_right.barh(ypos, not_on, left=np.array(ltfu), color=SERIES_COLORS["not_on_treatment"], label="Not on treatment")
        ax_right.set_yticks(ypos)
        ax_right.set_yticklabels(regions, fontsize=11)
        ax_right.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(value/1000)}K" if value >= 1000 else f"{int(value)}"))
        self._finalize_axis(ax_right)
        ax_right.set_xlabel(f"Measured loss from care ({data['period_label']})")
        ax_right.legend(loc="upper left", frameon=False, fontsize=11)
        fig.subplots_adjust(left=0.24, right=0.98, top=0.92, bottom=0.13)
        note = f"Residuals show which regions are above or below the fitted cascade pattern. Leakage is restricted to lost to follow-up plus not on treatment in the {data['period_label']} snapshot."
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
            ("Cumulative reported HIV cases", data["cases"], SERIES_COLORS["cases"], "count", "Direct surveillance end-of-year cumulative count, shown on the shared 2015-2025 window."),
            ("People living with HIV", data["plhiv"], "#3565af", "count", "Official UNAIDS annual estimate, shown on the shared 2015-2025 window."),
            ("New HIV infections", data["new_infections"], "#b35323", "count", "Official UNAIDS annual estimate, shown on the shared 2015-2025 window."),
            ("AIDS-related deaths", data["aids_deaths"], "#8a3f2a", "count", "Official UNAIDS annual estimate, shown on the shared 2015-2025 window."),
        ]

        for ax, (title, points, color, unit, subtitle) in zip(axes, panel_specs):
            years, series, observed_years, observed_values = _complete_annual_series(points)
            observed_start, observed_end = self._annual_series_bounds(points)
            self._shade_unavailable(ax, PUBLICATION_START_YEAR, PUBLICATION_END_YEAR, observed_start, observed_end)
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
                ax.text(1.0, 1.03, f"{max(observed_start, PUBLICATION_START_YEAR)}-{min(observed_end, PUBLICATION_END_YEAR)}", transform=ax.transAxes, ha="right", fontsize=9.6, color="#0c6150", fontweight="bold")
            ax.set_xlim(PUBLICATION_START_YEAR - 0.5, PUBLICATION_END_YEAR + 0.5)
            ax.set_xticks(_year_ticks(PUBLICATION_START_YEAR, PUBLICATION_END_YEAR))
            self._finalize_axis(ax)

        fig.subplots_adjust(top=0.93, bottom=0.10, left=0.07, right=0.98)
        note = "Historical board combines direct surveillance counts with official UNAIDS annual estimates inside the shared 2015-2025 publication window. Shaded years indicate no observed or published value for that panel."
        return self._save_figure(fig, "historical_board", "Historical burden and exposure shift", note)

    def _render_key_population_board(self, data: dict) -> PublicationFigure:
        self._base_style()
        fig = plt.figure(figsize=(15.2, 10.2))
        gs = GridSpec(2, 2, figure=fig, hspace=0.52, wspace=0.20)

        panel_specs = [
            ("Pregnant women diagnosed (cumulative)", data["pregnant_cumulative"], SERIES_COLORS["pregnant"], "count", "Annual latest cumulative value from quarterly surveillance, shown on the shared 2015-2025 window."),
            ("TGW diagnosed (cumulative)", data["tgw_cumulative"], SERIES_COLORS["tgw"], "count", "Annual latest cumulative count from the surveillance series, shown on the shared 2015-2025 window."),
            ("OFW cumulative burden", data["ofw_cumulative"], SERIES_COLORS["ofw"], "count", "Annual latest cumulative count from direct source extraction, shown on the shared 2015-2025 window."),
            ("Youth share of reported cases", data["youth_share"], SERIES_COLORS["youth"], "percent", "Annual latest share among people aged 15-24, shown on the shared 2015-2025 window."),
        ]

        axes = [fig.add_subplot(gs[row, col]) for row in range(2) for col in range(2)]
        for ax, (title, points, color, unit, subtitle) in zip(axes, panel_specs):
            panel_points = _collapse_annual_plateaus(points) if title == "TGW diagnosed (cumulative)" else points
            years, series, observed_years, observed_values = _complete_annual_series(panel_points)
            observed_start, observed_end = self._annual_series_bounds(panel_points)
            self._shade_unavailable(ax, PUBLICATION_START_YEAR, PUBLICATION_END_YEAR, observed_start, observed_end)
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
                ax.text(1.0, 1.03, f"{max(observed_start, PUBLICATION_START_YEAR)}-{min(observed_end, PUBLICATION_END_YEAR)}", transform=ax.transAxes, ha="right", fontsize=9.6, color="#0c6150", fontweight="bold")
            ax.set_xlim(PUBLICATION_START_YEAR - 0.5, PUBLICATION_END_YEAR + 0.5)
            ax.set_xticks(_year_ticks(PUBLICATION_START_YEAR, PUBLICATION_END_YEAR))
            ax.yaxis.set_major_locator(MaxNLocator(5))
            self._finalize_axis(ax)

        fig.subplots_adjust(top=0.93, bottom=0.09, left=0.07, right=0.98)
        note = "All four panels share the same 2015 to 2025 x-axis. Shaded periods indicate years with no observed values for that specific series."
        return self._save_figure(fig, "key_populations_board", "Key population sentinel panels", note)
