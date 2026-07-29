Paths to `data/bronze/`, `data/silver/` and `data/gold/` are fixed relative
constants in each module (no `.env`, no env vars) -- the app always runs from
the repo root (`python -m src.dashboard.interface`) against the local
Medallion layout that `src/data/bronze/`, `src/data/silver/` and
`src/data/gold/` always produce.

Run the app:

```
python -m src.dashboard.interface
```

No Spark, no HDFS -- everything reads local Parquet through DuckDB
(`src/data/access/queries.py`), plus `geopandas` for the district map.

## Structure

- `interface.py` -- entry point, wires 4 tabs
- `components/` -- reusable pieces: `filters.py` (dropdown options),
  `map.py` (choropleth + markers + color scales), `kpi.py` (KPI card text),
  `resumen.py` (summary tab)
- `tabs/overview.py` -- interactive dashboard: shared filters, KPIs,
  map, temporal evolution, collapsible air↔traffic correlation
- `tabs/resumen.py` -- catalog summary tab (districts/stations/points, coverage)
- `tabs/tabla.py` -- daily readings table, filterable by station
- `tabs/prediction.py` -- real history + future prediction per station,
  reads from `data/gold/ml/pred_<gas>_<N>m.parquet` (written by
  `python -m src.ml.main`); no retraining
