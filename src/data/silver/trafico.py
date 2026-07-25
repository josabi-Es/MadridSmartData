"""Bronze -> silver cleaning for traffic, per official field docs in findings.md."""

from pathlib import Path

import duckdb

TRAFFIC_METRICS = ["intensidad", "ocupacion", "carga", "vmed"]


def clean_traffic(bronze_path: str, out_path: str) -> None:
    """Turn negative sentinel values into NULL for the 4 traffic metrics.

    Per the official CKAN doc (208627-81 PDF): a negative value means "no
    data" for intensidad/ocupacion/carga/vmed. NaN literals already become
    true NULLs during bronze ingestion (columns are typed DOUBLE).
    """
    def clip(c: str) -> str:
        if c not in TRAFFIC_METRICS:
            return c
        return f"CASE WHEN {c} < 0 THEN NULL ELSE {c} END AS {c}"

    all_columns = duckdb.sql(f"SELECT * FROM '{bronze_path}' LIMIT 0").columns
    columns = [clip(c) for c in all_columns]
    query = f"SELECT {', '.join(columns)} FROM '{bronze_path}'"
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    # Traffic is 15-min resolution across ~4,700 sensors -- a month can be
    # 10M+ rows. duckdb defaults to one thread per core, which spikes RAM
    # hard in memory-constrained containers (hit an OOM in the Airflow
    # container). Cap it here rather than tuning the container's memory.
    duckdb.sql("SET threads=2; SET memory_limit='1GB';")
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")
