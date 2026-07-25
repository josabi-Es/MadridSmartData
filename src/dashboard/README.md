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

## Estructura

- `interface.py` -- entry point, cablea las 2 pestañas
- `components/` -- piezas reusables: `filters.py` (opciones de los
  desplegables), `map.py` (choropleth + marcadores + escalas de color),
  `kpi.py` (texto de las tarjetas de indicadores)
- `tabs/overview.py` -- dashboard interactivo: filtros compartidos, KPIs,
  mapa, evolución temporal, correlación aire↔tráfico colapsable
- `tabs/prediction.py` -- real vs. predicho del modelo ganador de fase 4,
  sin reentrenar nada
