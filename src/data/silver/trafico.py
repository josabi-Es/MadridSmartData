"""Bronze -> silver cleaning for traffic, per official field docs in findings.md."""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

BRONZE_TRAFFIC_PATH = os.getenv("BRONZE_TRAFFIC_PATH", "data/bronze/trafico/*.parquet")
SILVER_TRAFFIC_PATH = os.getenv("SILVER_TRAFFIC_PATH", "data/silver/trafico.parquet")

TRAFFIC_METRICS = ["intensidad", "ocupacion", "carga", "vmed"]


# Conservative sanity cap for intensidad (veh/h). Not from an official
# source (unlike carga's documented 0-100) -- picked from the real data
# distribution: p99.99 is ~6,744 with a long ambiguous tail up to ~20,000
# that could still be real dense traffic, but isolated spikes above that
# (e.g. 27,943 surrounded by neighbors of 2,400-5,200) are sensor glitches.
INTENSIDAD_MAX = 20000


def clean_traffic(bronze_path: str, out_path: str) -> None:
    """Turn negative/NaN sentinel values into NULL, drop incomplete rows.

    Per the official CKAN doc (208627-81 PDF): a negative value means "no
    data" for intensidad/ocupacion/carga/vmed. NaN literals do NOT become
    true NULLs during bronze ingestion despite the columns being typed
    DOUBLE -- confirmed real NaNs slip through (26,105 in vmed, 67,870 in
    ocupacion) since `NaN < 0` is false in SQL, so they're handled here too.
    Rows left with any invalid metric are dropped rather than imputed --
    imputing by (mes, dia) group would mix years, leaking future data into
    the past for time-series use.
    """
    def clip(c: str) -> str:
        if c not in TRAFFIC_METRICS:
            return c
        cap = f" OR {c} > {INTENSIDAD_MAX}" if c == "intensidad" else ""
        return f"CASE WHEN {c} < 0 OR isnan({c}){cap} THEN NULL ELSE {c} END AS {c}"

    all_columns = duckdb.sql(f"SELECT * FROM '{bronze_path}' LIMIT 0").columns
    columns = [clip(c) for c in all_columns]
    metrics_not_null = " AND ".join(f"{c} IS NOT NULL" for c in TRAFFIC_METRICS)
    query = (
        f"SELECT DISTINCT * FROM (SELECT {', '.join(columns)} FROM '{bronze_path}') "
        f"WHERE {metrics_not_null}"
    )
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    # Traffic is 15-min resolution across ~4,700 sensors -- a month can be
    # 10M+ rows. duckdb defaults to one thread per core, which spikes RAM
    # hard in memory-constrained containers (hit an OOM in the Airflow
    # container). Cap it here rather than tuning the container's memory.
    # DISTINCT over 52M rows needs headroom the old 1GB cap didn't have.
    duckdb.sql("SET threads=2; SET memory_limit='2GB';")
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


def main() -> None:
    clean_traffic(BRONZE_TRAFFIC_PATH, SILVER_TRAFFIC_PATH)


if __name__ == "__main__":
    main()
