"""Bronze/silver -> gold dimension tables: catalogs, no time series."""

import os
from pathlib import Path

import duckdb
import geopandas as gpd
from dotenv import load_dotenv

from src.data.silver.district_join import assign_district
from src.utils.logger_config import logger

load_dotenv()

DISTRITOS_PATH = os.getenv("DISTRITOS_PATH", "data/bronze/distritos/latest.parquet")
ESTACIONES_AIRE_PATH = os.getenv(
    "ESTACIONES_AIRE_BRONZE_PATH", "data/bronze/estaciones_aire/latest.parquet"
)
TRAFFIC_POINTS_PATH = os.getenv(
    "TRAFFIC_POINTS_PATH", "data/bronze/trafico_puntos_medida/*.parquet"
)
GOLD_DIR = os.getenv("GOLD_DIR", "data/gold")


def build_dim_estacion_aire(
    stations_path: str, distritos_path: str, out_path: str
) -> None:
    """Air stations with their district resolved -- delegates to the spatial
    join already used for silver, just pointed at a gold output path."""
    assign_district(stations_path, distritos_path, out_path)


def build_dim_punto_trafico(points_path: str, out_path: str) -> None:
    """Traffic points deduplicated by id -- the glob has ~1.9x duplication
    from snapshots; keep the latest one per sensor (highest snapshot date)."""
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (id) *
            FROM '{points_path}'
            ORDER BY id, 1 DESC
        ) TO '{out_path}' (FORMAT PARQUET)
    """)


def build_dim_magnitud(out_path: str) -> None:
    """Static catalog of the air quality gases -- no source table for this,
    the codes are the official MAGNITUD_LABELS mapping used to unpivot air."""
    from src.data.silver.aire import MAGNITUD_LABELS

    rows = [
        {"codigo": code, "magnitud": label} for code, label in MAGNITUD_LABELS.items()
    ]
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES {
            ', '.join(f"({r['codigo']}, '{r['magnitud']}')" for r in rows)
        }) AS t(codigo, magnitud)) TO '{out_path}' (FORMAT PARQUET)
    """)


def build_dim_distrito(
    distritos_path: str, dim_estacion_path: str, dim_punto_path: str, out_path: str
) -> None:
    """Districts + coverage columns (n_estaciones_aire, n_puntos_trafico,
    cobertura_aire) -- flags districts with traffic but no air station."""
    distritos = gpd.read_parquet(distritos_path)
    distritos["COD_DIS"] = distritos["COD_DIS"].astype(str)

    n_estaciones = duckdb.sql(f"""
        SELECT CAST(COD_DIS AS VARCHAR) AS COD_DIS, count(*) AS n_estaciones_aire
        FROM '{dim_estacion_path}'
        WHERE COD_DIS IS NOT NULL
        GROUP BY COD_DIS
    """).df()
    n_puntos = duckdb.sql(f"""
        SELECT CAST(distrito AS VARCHAR) AS COD_DIS, count(*) AS n_puntos_trafico
        FROM '{dim_punto_path}'
        WHERE distrito IS NOT NULL
        GROUP BY distrito
    """).df()

    distritos = distritos.merge(n_estaciones, on="COD_DIS", how="left")
    distritos = distritos.merge(n_puntos, on="COD_DIS", how="left")
    distritos["n_estaciones_aire"] = (
        distritos["n_estaciones_aire"].fillna(0).astype(int)
    )
    distritos["n_puntos_trafico"] = distritos["n_puntos_trafico"].fillna(0).astype(int)
    distritos["cobertura_aire"] = distritos["n_estaciones_aire"] > 0

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    distritos.to_parquet(out_path)


def main(target: str = "all") -> None:
    dim_estacion_path = f"{GOLD_DIR}/dim_estacion_aire.parquet"
    dim_punto_path = f"{GOLD_DIR}/dim_punto_trafico.parquet"
    dim_distrito_path = f"{GOLD_DIR}/dim_distrito.parquet"
    dim_magnitud_path = f"{GOLD_DIR}/dim_magnitud.parquet"

    if target in ("dim_estacion_aire", "all"):
        build_dim_estacion_aire(
            ESTACIONES_AIRE_PATH, DISTRITOS_PATH, dim_estacion_path
        )
        logger.info("dim_estacion_aire -> %s", dim_estacion_path)

    if target in ("dim_punto_trafico", "all"):
        build_dim_punto_trafico(TRAFFIC_POINTS_PATH, dim_punto_path)
        logger.info("dim_punto_trafico -> %s", dim_punto_path)

    if target in ("dim_distrito", "all"):
        build_dim_distrito(
            DISTRITOS_PATH, dim_estacion_path, dim_punto_path, dim_distrito_path
        )
        logger.info("dim_distrito -> %s", dim_distrito_path)

    if target in ("dim_magnitud", "all"):
        build_dim_magnitud(dim_magnitud_path)
        logger.info("dim_magnitud -> %s", dim_magnitud_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target",
        default="all",
        choices=["all", "dim_estacion_aire", "dim_punto_trafico",
                 "dim_distrito", "dim_magnitud"],
    )  # fmt: skip
    main(parser.parse_args().target)
