<div align="center">
<img src="https://skillicons.dev/icons?i=python,duckdb,airflow,docker,git,sklearn,pandas&theme=light" />
<br/>
</div>

# data/

Not tracked in git (only this README is). Everything here is generated
locally by the pipeline scripts — nothing is downloaded by hand.

```
data/
├── bronze/   # raw files, as downloaded from Madrid's open data API
├── silver/   # cleaned versions of the same data
└── gold/     # final tables + trained ML models, ready for the dashboard
```

All files are `.parquet` (a compact table format), except the ML models
(`.joblib`) and their metrics (`.json`).

## Regenerating the data

1. `python -m src.data.bronze.pipeline --dataset <name> --years <year>`
2. `python -m src.data.run_pipeline`
3. `python -m src.ml.train`

## Reading the data

Queries use [DuckDB](https://duckdb.org) to read the `.parquet` files
directly — no database server needed. Default paths are `data/bronze|silver|gold/...`,
overridable via environment variables (see `src/dashboard/.env.template`).
