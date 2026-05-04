# EpiGraph PH 🇵🇭

### Philippines HIV Surveillance Atlas

[![GitHub Pages](https://img.shields.io/badge/GitHub%20Pages-deployed-009ADE?style=flat-square)](https://gauravshajepal-hash.github.io/epigraph-ph/)
[![UNAIDS Data](https://img.shields.io/badge/UNAIDS%20Data-2025-0077B6?style=flat-square)](https://aidsinfo.unaids.org/)
[![License](https://img.shields.io/badge/license-MIT-27ae60?style=flat-square)](LICENSE)
[![Charts](https://img.shields.io/badge/charts-18%2B%20ECharts%20%2B%20Plotly-E67E22?style=flat-square)](https://gauravshajepal-hash.github.io/epigraph-ph/)

**EpiGraph PH** is an interactive, publication‑grade surveillance dashboard for the Philippines HIV epidemic. It integrates **DOH/HARP regional surveillance data** with **UNAIDS national estimates** to surface sub‑national inequalities, programmatic drift, and long‑run epidemiological burden.

---

## 📊 Dashboard Tabs

### 1. Overview

The landing view summarizing national performance, regional inequality, and historical context — all charts powered by **UNAIDS 1990–2024 estimates**.

| Card | Description |
|------|-------------|
| **National Cascade** | Year‑end target board (Diagnosis → Treatment → Suppression) using DOH/HARP 2018–2025 |
| **Regional Inequality** | Cross‑sectional regional stage matrix sorted by distance to 95‑95‑95 targets |
| **Anomalies** | Performance versus burden quadrant — gap between observed suppression and model expectation |
| **Historical View** | UNAIDS national estimates 1990–2024: PLHIV, New Infections, AIDS Deaths, Cumulative Cases in a 2×2 Plotly subplot grid — *UNAIDS data replaces DOH data as primary source* |
| **Key Populations** | Mixed UNAIDS KP Atlas + DOH: MSM, Sex Workers, Transgender Women, PWID, OFW Burden, Youth Share in a 3×2 area-chart grid |
| **PMTCT** | Decomposed cascade: Women Needing ARV (area) + Coverage % (area line, dual‑axis) and MTCT Rate (area line) — *"Received ARV" excluded due to incomplete program data* |

### 2. Explorer

Integrated analytical workspace for deep regional investigation.

| Feature | Description |
|---------|-------------|
| Regional Stage Matrix | Sorted by distance to target, column colors match cascade stages |
| Region Fingerprint | Current vs target [95‑95‑95] with year‑over‑year movement |
| Impact Bridge Matrix | Performance gap juxtaposed with raw leakage burden |
| Regional Rank Delta | Compact before‑vs‑after ranking on selected stage |
| Year / Region Selectors | Interactive dropdowns to explore any year‑region combination |

### 3. Experimental Estimates

Forward‑looking projections and strategic simulations.

| Feature | Description |
|---------|-------------|
| **National Forward Scenarios** | Distribution of simulated futures for selected cascade stage |
| **Regional Forward Paths** | Individual region departure around the national path |
| **Strategic Hurdles to 2035** | Identifies bottlenecks to achieving 95‑95‑95 targets |
| **Success Scores & Watchlist** | Probability of target achievement, regions requiring attention |
| **Data Foundation** | Technical constraints and historical anchors |

### 4. UNAIDS Compass

Curated UNAIDS reference data — epidemic metrics, prevention, and finance. *Formerly "UNAIDS Data" — renamed to reflect expanded scope.*

| Card | Content |
|------|---------|
| **Key Indicators** | Link to full Philippines factsheet on AIDSINFO portal |
| **Epidemic Overview** | 2×2 grid of interactive AIDSINFO iframes: PLHIV, New HIV Infections, AIDS‑Related Deaths, Treatment Cascade 95‑95‑95 — each with `↗ Open Full Chart` link and horizontal scroll for full x‑axis visibility |
| **Prevention** | Condom distribution (annual total) and PrEP coverage — side‑by‑side col‑span‑6 line charts |
| **Finance** | HIV expenditure by source: Total, Domestic Public, International, Global Fund — 4 mini area‑line charts in one card showing US$ trends over time |
| **Attribution** | Data source citation, UNAIDS AIDSINFO portal links, and usage notes |

### 5. Methods & Sources

Full analytical infrastructure documentation.

| Section | Details |
|---------|---------|
| **Core Methods** | Figure generation policy, formulas, coverage windows |
| **Reference Catalog** | 42+ source documents with direct PDF links |
| **Source Inventory** | Comprehensive table of all data sources, URLs, and metadata |
| **Cross‑Referencing** | Links to UNAIDS Compass tab for official national estimates |

---

## 🖱️ Click-to-Expand

**Every chart in the dashboard is click‑to‑expand.** Click anywhere on any chart (ECharts, Plotly, or iframe) to open it full‑screen in a new tab at **1100×700px** — ideal for screenshots, presentations, and detailed inspection.

| Chart Type | Expand Behavior |
|-----------|----------------|
| **ECharts** (`.chart-host`) | Cloned with full option set — interactive at full size |
| **Plotly** (`.publication-figure`) | Data + layout extracted and re‑rendered at 700px height, responsive |
| **AIDSINFO iframes** | Opens the full AIDSINFO portal page in a new tab |

Hover any chart to see a subtle ↗ indicator in the top‑right corner.

---

## 🏗 Architecture

```
epigraph-ph/
├── apps_script/          # Google Apps Script deployment
│   ├── Index.html        # Main dashboard (embedded in GAS)
│   ├── Code.gs           # Server‑side Apps Script logic
│   └── appsscript.json   # GAS project config
├── dist/                 # GitHub Pages deployment
│   ├── index.html        # Standalone single‑page dashboard
│   └── data/             # Normalized JSON/JSONL data feeds
│       ├── dashboard_feed.json
│       ├── publication_assets.json
│       ├── summary.json
│       ├── claims.jsonl
│       ├── observations.jsonl
│       └── review_queue_enriched.jsonl
└── README.md
```

### Key Dependencies

| Library | Purpose | Version |
|---------|---------|---------|
| [ECharts](https://echarts.apache.org/) | Interactive UNAIDS chart rendering | v5 |
| [Plotly.js](https://plotly.com/javascript/) | Publication‑grade multi‑panel figures | v2.35 |
| [Google Fonts](https://fonts.google.com/) | Fraunces, IBM Plex Sans, IBM Plex Mono | — |

### Rendering Pipeline

```
UNAIDS_CHARTS (inline JSON, 18 ECharts configs)
    │
    ├──→ ECharts mountUnaidsCharts()
    │       ├── Overview: overview-pmtct-need, overview-pmtct-rate
    │       └── UNAIDS Compass: unaids-condoms, unaids-prep, unaids-expenditure-*
    │
    ├──→ Plotly buildUnaidsHistoricalOption()
    │       └── Overview: publication-historical-board (2×2 subplot, 1990–2024)
    │
    ├──→ Plotly buildCombinedKPOption()
    │       └── Overview: publication-key-populations (3×2 area grid, UNAIDS + DOH)
    │
    └──→ AIDSINFO iframes
            └── UNAIDS Compass: 4 embedded iframes (2×2 grid, 720px width, horizontal scroll)
```

---

## 📈 UNAIDS Chart Inventory

Eighteen interactive ECharts and Plotly figures sourced from UNAIDS AIDSINFO 2025 datasets:

| # | Chart | Data Source | Rendering | Tab |
|---|-------|------------|-----------|-----|
| 01 | Epidemic Curve (PLHIV, New Infections, Deaths) | Estimates 2025 | Plotly 2×2 | Overview |
| 02 | 95‑95‑95 Treatment Cascade | Estimates 2025 | ECharts | Compass (iframe) |
| 03 | New Infections vs AIDS Deaths | Estimates 2025 | ECharts | Overview |
| 04 | Key Population Size Estimates | KP Atlas 2025 | Plotly 3×2 | Overview |
| 05 | KP HIV Prevalence | KP Atlas 2025 | ECharts | — |
| 06 | KP ART Coverage | KP Atlas 2025 | ECharts | — |
| 07 | PMTCT Cascade (Need ARV + Coverage + MTCT Rate) | Estimates 2025 | ECharts dual | Overview |
| 08 | Deaths Averted by ART | Estimates 2025 | ECharts | — |
| 09 | Epidemic Transition Points | Estimates 2025 | ECharts | — |
| 10 | Condom Distribution | GAM 2025 | ECharts line | Compass |
| 11 | PrEP Coverage | GAM 2025 | ECharts line | Compass |
| 12a-d | Expenditure by Source (Total, Domestic Public, International, Global Fund) | GAM 2025 | ECharts area ×4 | Compass |
| 13 | Policy & Legal Scorecard | NCPI 2025 | ECharts | — |

> Charts marked `—` are available in the `UNAIDS_CHARTS` inline data but not currently displayed in a tab.

---

## 🔬 Data Sources

| Source | Dataset | Coverage | Year |
|--------|---------|----------|------|
| **UNAIDS AIDSINFO** | Estimates | National PLHIV, incidence, mortality, PMTCT, cascade | 2025 |
| **UNAIDS AIDSINFO** | GAM (Global AIDS Monitoring) | Condom distribution, PrEP coverage, HIV expenditure | 2025 |
| **UNAIDS AIDSINFO** | Key Populations Atlas | MSM, TGW, SW, PWID population size estimates | 2025 |
| **UNAIDS AIDSINFO** | NCPI (National Commitments & Policies) | Legal environment, policy scorecard | 2025 |
| **DOH/HARP** | Philippine HIV Registry | Regional cascade stages, demographic surveillance | 2018–2025 |
| **World Bank** | WDI HIV indicators | Cross‑national economic & health indicators | — |

All UNAIDS data is presented with attribution as intellectual property of the Joint United Nations Programme on HIV/AIDS.

---

## 🎨 Design Philosophy

- **WHO / UNAIDS / World Bank‑inspired light theme** — clean white surfaces, blue accent (`#009ADE`), high‑contrast typography
- **Fraunces** serif for headlines and key numbers; **IBM Plex Sans** for body; **IBM Plex Mono** for data labels and technical content
- **Publication‑grade** figure styling with consistent color palettes per metric
- **Responsive grid** (12‑column CSS Grid) adapting from desktop to mobile
- **Fade‑in animations** and hover transitions for polished UX
- **Click‑to‑expand** on all charts — open any visualization full‑screen in a new tab

---

## 🚀 Deployment

### GitHub Pages (Production)

The `dist/` directory is deployed via GitHub Pages at:

```
https://gauravshajepal-hash.github.io/epigraph-ph/
```

Push to the `main` branch — the GitHub Actions workflow auto‑deploys.

### Google Apps Script

The `apps_script/` directory contains a Google Apps Script deployment for internal use with live DOH/HARP data feeds.

### Local Development

```bash
# Clone repo
git clone https://github.com/gauravshajepal-hash/epigraph-ph.git
cd epigraph-ph

# Serve locally
python3 -m http.server 8088 --directory dist

# Open browser
open http://localhost:8088/index.html
```

---

## 📝 Notes

- **UNAIDS Compass** (formerly "UNAIDS Data" → "Prevention & Finance") was renamed to reflect its expanded scope: epidemic metrics + prevention + finance in one tab
- The **Historical View** and **Key Populations** cards in the Overview now prioritize UNAIDS national estimates over DOH data, with DOH panels retained where available
- PMTCT **"Received ARV"** program data was excluded due to incomplete facility‑level reporting (12–116 reported recipients vs 375–734 estimated need)
- All expenditure values are in **US Dollars (US$)** as reported to UNAIDS GAM
- The 18 inline ECharts/Plotly configurations are embedded as `const UNAIDS_CHARTS` in the single‑page HTML — no external chart data fetches required
- **Epidemic Overview** iframes render at 720px fixed width with horizontal scroll on each wrapper to accommodate full x‑axis through 2024

---

## 🤝 Attribution

```
Data Source: UNAIDS AIDSINFO — Global HIV & AIDS Data Portal
URL:         https://aidsinfo.unaids.org/
Datasets:    Estimates 2025, GAM 2025, Key Populations 2025, NCPI 2025
Organization: Joint United Nations Programme on HIV/AIDS (UNAIDS)
License:     Freely available for non‑commercial use with attribution

EpiGraph PH overlays DOH/HARP regional surveillance data on top of
UNAIDS national estimates to surface sub‑national inequalities,
anomalies, and program drift not visible in national aggregates.
```

---

<p align="center">
  <sub>Built with ❤️ for the Philippines HIV response</sub>
</p>
