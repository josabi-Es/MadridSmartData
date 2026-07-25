This project uses .env files to configure paths for your data. Follow these steps to set it up:

1. Copy the template to create your own .env file:

```
cp .env.template .env
```

2. Edit the .env file if your Parquet lives somewhere other than `data/bronze/`
   and `data/processed/` (see the ingestion pipeline in `src/data/ingest/` and
   cleaning in `src/data/transform/`).

3. Run the app:

```
python -m src.app.gradio.interface
```

No Spark, no HDFS -- everything reads local Parquet through DuckDB
(`src/data/access/queries.py`), plus `geopandas` for the district map.
