"""Silver -> gold fact tables: fact_calidad_aire, fact_trafico.

Grain of fact_calidad_aire: estacion x fecha x magnitud. Grain of
fact_trafico: punto x fecha, with intensidad/ocupacion/carga/vmed as
columns -- traffic has no dim_magnitud equivalent (spec.md), those are
properties of the reading, not rows of a catalog.
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
    silver_air_path: str, dim_estacion_path: str, out_path: str
) -> None:
    """Join silver air readings to dim_estacion_aire for cod_dis."""
    query = f"""
        SELECT a.estacion, e.COD_DIS AS cod_dis, a.magnitud, a.fecha,
               a.dato, a.validez
        FROM '{silver_air_path}' a
        JOIN '{dim_estacion_path}' e ON a.estacion = e.CODIGO_CORTO
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


def build_fact_trafico(
    silver_traffic_path: str, dim_punto_path: str, out_path: str
) -> None:
    """Join silver traffic readings to dim_punto_trafico for distrito."""
    query = f"""
        SELECT t.id, p.distrito, t.fecha,
               t.intensidad, t.ocupacion, t.carga, t.vmed, t.error
        FROM '{silver_traffic_path}' t
        JOIN '{dim_punto_path}' p ON t.id = p.id
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


def main(target: str = "all") -> None:
    fact_aire_path = f"{GOLD_DIR}/fact_calidad_aire.parquet"
    fact_trafico_path = f"{GOLD_DIR}/fact_trafico.parquet"
    dim_estacion_path = f"{GOLD_DIR}/dim_estacion_aire.parquet"
    dim_punto_path = f"{GOLD_DIR}/dim_punto_trafico.parquet"

    if target in ("fact_calidad_aire", "all"):
        build_fact_calidad_aire(SILVER_AIR_PATH, dim_estacion_path, fact_aire_path)
        logger.info("fact_calidad_aire -> %s", fact_aire_path)

    if target in ("fact_trafico", "all"):
        build_fact_trafico(SILVER_TRAFFIC_PATH, dim_punto_path, fact_trafico_path)
        logger.info("fact_trafico -> %s", fact_trafico_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target", default="all",
        choices=["all", "fact_calidad_aire", "fact_trafico"],
    )  # fmt: skip
    main(parser.parse_args().target)
