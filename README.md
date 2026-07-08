# Smart City Traffic & Pollution Analysis

## I. Introduction

This project analyzes traffic and pollution data from Madrid open data. It provides
a **Gradio interface with four interactive tabs** to explore air quality and traffic
insights, and it is being rebuilt into a reproducible pipeline: API ingestion →
Parquet + DuckDB → forecasting comparison. Full detail lives in `spec/` (mission,
tech stack, roadmap, one folder per feature); this README tracks progress phase by
phase and how to verify each one locally.

## II. Progress by phase

Full roadmap: `spec/constitution/roadmap.md`. Each phase below states what you can
run locally to confirm it works before the next one starts.

### ✅ Phase 0 — Reorganize `src/`, add ruff/pytest tooling

`src/interface/` moved to `src/app/gradio/` unchanged; added `src/core`,
`src/data/{ingest,transform,access}`, `src/ml` skeleton per
`spec/features/000-reorganizacion`.

```bash
uv sync
uv run ruff check .
uv run pytest
```

### ✅ Phase 1 — Catalog discovery (Madrid Open Data CKAN API)

Confirmed the real `dataset_id` for air quality, traffic, and districts against
the live API. Findings: `spec/features/001-descubrimiento-catalago/findings.md`.

```bash
# list all resources for one dataset, one year — avoids pulling the whole catalog
uv run python src/data/ingest/catalog_probe.py --dataset aire_diario --year 2024

# also preview the first rows of the matching CSV
uv run python src/data/ingest/catalog_probe.py --dataset aire_diario --year 2024 --preview
```

### ⏳ Phase 2 — API ingestion (pending)

Replace manual CSV downloads with a CKAN-based ingestor writing partitioned
Parquet. Spec: `spec/features/002-ingesta-api-madrid`.

### ⏳ Phase 3 — Drop Spark/HDFS, read via DuckDB (pending)

`src/app/gradio/` stops depending on a running HDFS/Spark session. Spec:
`spec/features/003-simplificacion-infra`.

### ⏳ Phase 4 — Forecasting comparison (pending)

### ⏳ Phase 5 — Forecast exposure (pending)

## III. Repository structure

```text
MadridSmartData/
├── spec/                       # mission, tech stack, roadmap, one folder per feature
├── data/                       # raw + processed data (not versioned, see data/README.md)
├── src/
│   ├── core/                   # transversal config/logging (added when it has a consumer)
│   ├── data/
│   │   ├── ingest/             # CKAN API client, catalog probe            (phase 1-2)
│   │   ├── transform/          # cleaning, CSV → Parquet                   (phase 2)
│   │   └── access/             # DuckDB queries                           (phase 3)
│   ├── ml/                     # forecasting: features, models, evaluation (phase 4)
│   ├── app/gradio/             # Gradio UI — entry point
│   ├── ingest/                 # legacy manual CSV→Parquet scripts, retired in phase 2
│   └── preprocessing/          # legacy Spark ETL, retired in phase 2/3
└── tests/                      # mirrors src/
```

## IV. Technologies

- Python 3.11+
- Gradio → interactive UI
- DuckDB → SQL over local Parquet, no server (replaces Spark/HDFS as of phase 3)
- PySpark & HDFS → still used by `src/ingest/` and `src/preprocessing/` until
  phases 2-3 retire them; see `spec/constitution/tech-stack.md` for why they're
  being dropped
- scikit-learn → forecasting models (phase 4)

## V. Setup

1. Clone the repository
```bash
git clone <repo_url>
cd MadridSmartData
```
2. Install dependencies with [uv](https://docs.astral.sh/uv/)
```bash
pip install uv
uv sync
```
3. Activate the environment
```bash
source .venv/bin/activate  # or .venv/Scripts/activate on Windows
```
4. Run the Gradio app (currently still requires a local HDFS/Spark setup —
   see `src/app/gradio/README.md` and phase 3 above for when that goes away)
```bash
python src/app/gradio/interface.py
```

## VI. Gradio Results

Example insights available in the current interface:

- **Gas concentration by district:** compare pollutant levels (NO2, PM10, O3...)
  across districts for a given month/year.
- **Traffic intensity and district status:** visualize traffic intensity/occupancy
  and relate it to air quality per district.
- **Traffic & pollution relationships:** correlations between traffic patterns and
  pollutant concentrations.

<p align="center">
  <img src="images/ImageReadme_1.png" alt="Daily evolution of NO2 and traffic intensity." width="600">
</p>

<p align="center">
  <img src="images/ImageReadme_2.png" alt="NO2 concentration by district in February 2022." width="600">
</p>

## License

This project is licensed under the GNU General Public License v3.0 (GPLv3) — see
the [LICENSE](LICENSE) file for details.
