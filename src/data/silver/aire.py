"""Bronze -> silver cleaning for air quality, per official field docs in findings.md."""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

BRONZE_AIR_PATH = os.getenv("BRONZE_AIR_PATH", "data/bronze/aire/*.parquet")
SILVER_AIR_PATH = os.getenv("SILVER_AIR_PATH", "data/silver/aire/all.parquet")

# Official magnitud codes, per the station structure doc (212629-2 PDF).
MAGNITUD_LABELS = {
    1: "SO2", 6: "CO", 7: "NO", 8: "NO2", 9: "PM2.5", 10: "PM10",
    12: "NOx", 14: "O3", 20: "TOL", 30: "BEN", 35: "EBE", 42: "TCH", 43: "CH4",
    44: "NMHC",
}  # fmt: skip


def unpivot_air_quality(bronze_path: str, out_path: str) -> None:
    """Turn the wide D01..D31/V01..V31 bronze layout into one row per day.

    Days that don't exist for a given month (D31 in April, etc.) are NOT
    NULL in the source -- they're padded with dato=0.0, validez='N'. The
    only reliable filter is the real calendar: day <= last day of ANO/MES.
    """
    magnitud_case = "CASE MAGNITUD " + " ".join(
        f"WHEN {code} THEN '{label}'" for code, label in MAGNITUD_LABELS.items()
    ) + " ELSE CAST(MAGNITUD AS VARCHAR) END"

    days = [
        f"""
        SELECT ESTACION AS estacion, {magnitud_case} AS magnitud,
               MAKE_DATE(CAST(ANO AS INTEGER), CAST(MES AS INTEGER), {d}) AS fecha,
               D{d:02d} AS dato, V{d:02d} AS validez
        FROM '{bronze_path}'
        WHERE {d} <= EXTRACT(DAY FROM LAST_DAY(
            MAKE_DATE(CAST(ANO AS INTEGER), CAST(MES AS INTEGER), 1)
        ))
        """
        for d in range(1, 32)
    ]
    query = " UNION ALL ".join(days)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


def main() -> None:
    unpivot_air_quality(BRONZE_AIR_PATH, SILVER_AIR_PATH)


if __name__ == "__main__":
    main()
