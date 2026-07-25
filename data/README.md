# data/

No versionado en git (`.gitignore`: `data/*`, excepto este README). Layout
Medallion, generado localmente por `src/data/bronze/`, `src/data/silver/`,
`src/data/gold/` y `src/ml/train.py` — no se descarga nada a mano.

```
data/
├── bronze/     # crudo, tal cual llega de la API CKAN de Madrid
│   ├── aire/, trafico/, estaciones_aire/, trafico_puntos_medida/, distritos/
│   └── cada dataset trae su manifest.json (evita re-descargas)
│
├── silver/     # limpio, dividido por dominio: negativos->NULL en tráfico
│   │           # (src/data/silver/trafico.py), ancho->largo + código de gas
│   │           # ->nombre en aire (src/data/silver/aire.py), spatial join a
│   │           # distrito (src/data/silver/district_join.py, compartido)
│   ├── aire/, trafico/, estaciones_aire/
│
└── gold/       # modelo dimensional (src/data/gold/) + artefactos ML (src/ml/train.py)
    ├── dim_distrito.parquet, dim_estacion_aire.parquet,
    │   dim_punto_trafico.parquet, dim_magnitud.parquet
    ├── fact_calidad_aire.parquet, fact_trafico.parquet
    └── ml_<variable>_<año>.joblib/.json/.parquet — sin subcarpetas, el año
        en el nombre evita que un reentreno futuro con más histórico pise
        el anterior; la app siempre lee el año más reciente por variable.
```

## Cómo se regenera

1. `bronze/`: `python -m src.data.bronze.pipeline --dataset <nombre> --years <año>` (manual, un dataset/año por comando, ver `src/data/bronze/pipeline.py`)
2. `silver/` + `gold/dim_*` + `gold/fact_*`: `python -m src.data.run_pipeline` (encadena `silver/{aire,trafico,district_join}.py` -> `gold/dimensions.py` -> `gold/facts.py`, idempotente)
3. `gold/ml_*`: `python -m src.ml.train`

## Configurar rutas

Las rutas están hardcodeadas por defecto a `data/bronze|silver|gold/...`,
pero cada consumidor (`src/dashboard/*`, `src/ml/train.py`,
`src/data/run_pipeline.py`) las lee vía variables de entorno
(`DATA_AIRQUALITY_PATH`, `DATA_TRAFFIC_PATH`, `ESTACIONES_DISTRITO_PATH`,
`ML_MODELS_DIR`, `SILVER_AIR_PATH`, `SILVER_TRAFFIC_PATH`...) — ver
`src/dashboard/.env.template` para la lista completa y overridearlas sin
tocar código. Catálogo de tablas gold (grano, clave, columnas, script):
`src/data/gold/_catalog.yml`.
