"""Silver -> gold fact tables: FACT_CALIDAD_AIRE, FACT_TRAFICO_DIARIO.

Grain of FACT_CALIDAD_AIRE: ID_AIRE x FECHA x ID_MAGNITUD. Grain of
FACT_TRAFICO_DIARIO: ID_TRAFICO x FECHA(day) x ID_MEDIDA_TRAFICO -- traffic's
raw 15-min resolution is never materialized in gold (89M+ rows, too heavy
for Power BI import mode); FACT_TRAFICO_DIARIO aggregates straight from
silver and unpivots INTENSIDAD/OCUPACION/CARGA/VMED into rows, mirroring how
DIM_MAGNITUD/FACT_CALIDAD_AIRE model gases.

All keys uppercase and homogenized (ID_AIRE, ID_TRAFICO, COD_DIS) to match
the dimensions in `dimensions.py`, so Power BI can infer relationships by
column name.
"""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from src.utils.logger_config import logger

load_dotenv()

SILVER_AIR_PATH = os.getenv("SILVER_AIR_PATH", "data/silver/aire.parquet")
SILVER_TRAFFIC_PATH = os.getenv("SILVER_TRAFFIC_PATH", "data/silver/trafico.parquet")
GOLD_DIR = os.getenv("GOLD_DIR", "data/gold")


def build_fact_calidad_aire(
    silver_air_path: str, dim_estacion_path: str, dim_magnitud_path: str, out_path: str
) -> None:
    """Join silver air readings to DIM_ESTACION_AIRE for COD_DIS and to
    DIM_MAGNITUD for ID_MAGNITUD -- MAGNITUD text never lands in the fact,
    only the numeric FK (smaller, and it's what Power BI's VertiPaq wants).

    VALIDEZ is dropped: silver already keeps only VALIDEZ='V' rows.
    """
    query = f"""
        SELECT a.FECHA, a.ID_AIRE, e.COD_DIS, m.ID_MAGNITUD, a.DATO
        FROM '{silver_air_path}' a
        JOIN '{dim_estacion_path}' e ON a.ID_AIRE = e.ID_AIRE
        JOIN '{dim_magnitud_path}' m ON a.MAGNITUD = m.MAGNITUD
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


def build_fact_trafico_diario(
    silver_traffic_path: str, dim_punto_path: str, dim_medida_path: str, out_path: str
) -> None:
    """Daily, unpivoted aggregate straight from silver traffic (15-min, 89M+
    rows -- too heavy to materialize/import as-is). Rolls up to one row per
    point/day/metric, mirroring FACT_CALIDAD_AIRE's ID_MAGNITUD pattern so a
    single measure + a MEDIDA slicer can switch between
    INTENSIDAD/OCUPACION/CARGA/VMED.
    """
    query = f"""
        WITH diario AS (
            SELECT
                CAST(t.FECHA AS DATE) AS FECHA,
                t.ID_TRAFICO,
                p.COD_DIS,
                AVG(t.INTENSIDAD) AS INTENSIDAD,
                AVG(t.OCUPACION) AS OCUPACION,
                AVG(t.CARGA) AS CARGA,
                AVG(t.VMED) AS VMED
            FROM '{silver_traffic_path}' t
            JOIN '{dim_punto_path}' p ON t.ID_TRAFICO = p.ID_TRAFICO
            GROUP BY 1, 2, 3
        ), largo AS (
            UNPIVOT diario
            ON INTENSIDAD, OCUPACION, CARGA, VMED
            INTO NAME MEDIDA VALUE VALOR
        )
        SELECT l.FECHA, l.ID_TRAFICO, l.COD_DIS, m.ID_MEDIDA_TRAFICO, l.VALOR
        FROM largo l
        JOIN '{dim_medida_path}' m ON l.MEDIDA = m.MEDIDA
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


def main(target: str = "all") -> None:
    fact_aire_path = f"{GOLD_DIR}/fact_calidad_aire.parquet"
    dim_estacion_path = f"{GOLD_DIR}/dim_estacion_aire.parquet"
    dim_punto_path = f"{GOLD_DIR}/dim_punto_trafico.parquet"
    dim_magnitud_path = f"{GOLD_DIR}/dim_magnitud.parquet"
    dim_medida_trafico_path = f"{GOLD_DIR}/dim_medida_trafico.parquet"
    fact_trafico_diario_path = f"{GOLD_DIR}/fact_trafico_diario.parquet"

    if target in ("fact_calidad_aire", "all"):
        build_fact_calidad_aire(
            SILVER_AIR_PATH, dim_estacion_path, dim_magnitud_path, fact_aire_path
        )
        logger.info("fact_calidad_aire -> %s", fact_aire_path)

    if target in ("fact_trafico_diario", "all"):
        build_fact_trafico_diario(
            SILVER_TRAFFIC_PATH, dim_punto_path, dim_medida_trafico_path,
            fact_trafico_diario_path,
        )  # fmt: skip
        logger.info("fact_trafico_diario -> %s", fact_trafico_diario_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target", default="all",
        choices=["all", "fact_calidad_aire", "fact_trafico_diario"],
    )  # fmt: skip
    main(parser.parse_args().target)
