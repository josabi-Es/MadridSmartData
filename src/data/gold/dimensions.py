"""Silver -> gold dimension tables: catalogs, no time series.

All column names are UPPERCASE and homogenized project-wide (ID_AIRE,
ID_TRAFICO, COD_DIS), matching the FACT_* tables in `facts.py` -- this is
what lets Power BI infer relationships by column name instead of a manual
mapping per pair of tables.
"""

import os
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

from src.utils.logger_config import logger

load_dotenv()

SILVER_ESTACIONES_PATH = os.getenv(
    "SILVER_ESTACIONES_PATH", "data/silver/estaciones_aire.parquet"
)
SILVER_AIR_PATH = os.getenv("SILVER_AIR_PATH", "data/silver/aire.parquet")
SILVER_TRAFFIC_PATH = os.getenv("SILVER_TRAFFIC_PATH", "data/silver/trafico.parquet")
DISTRITOS_PATH = os.getenv("DISTRITOS_PATH", "data/bronze/distritos/latest.parquet")
TRAFFIC_POINTS_PATH = os.getenv(
    "TRAFFIC_POINTS_PATH", "data/bronze/trafico_puntos_medida/*.parquet"
)
GOLD_DIR = os.getenv("GOLD_DIR", "data/gold")

GAS_FLAGS = ["NO2", "SO2", "CO", "PM10", "PM2_5", "O3", "BTX"]


def build_dim_estacion_aire(silver_estaciones_path: str, out_path: str) -> None:
    """Trim silver's full station table down to what Power BI needs.

    Drops redundant/raw fields (ETRS89 strings, ALTITUD, COD_TIPO/NOM_TIPO,
    street reference, FECHA_ALTA, district NOMBRE -- already reachable via
    COD_DIS -> DIM_DISTRITO) and turns the gas flags ("X"/None) into 1/0,
    which is both smaller and directly usable as a PBI measure.
    """
    df = duckdb.sql(f"SELECT * FROM '{silver_estaciones_path}'").df()

    keep = ["ID_AIRE", "ESTACION", "DIRECCION", "LATITUD", "LONGITUD", "COD_DIS"]
    out = df[keep].copy()
    for gas in GAS_FLAGS:
        out[gas] = df[gas].notna().astype(int)

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)


# Comunidad de Madrid bounding box, generous margin -- same range used in
# src/data/silver/district_join.py for air stations. Traffic points have no
# silver stage of their own (bronze -> gold direct), so the sanity filter
# lives here instead; a handful of points carry bad lat/long (bronze
# capture errors, e.g. one found sitting in the sea) that would otherwise
# reach dim_punto_trafico and the map visuals downstream.
MADRID_LAT_RANGE = (39.85, 41.20)
MADRID_LON_RANGE = (-4.60, -3.05)


def build_dim_punto_trafico(points_path: str, out_path: str) -> None:
    """Traffic points deduplicated by id -- the glob has ~1.9x duplication
    from snapshots; keep the latest one per sensor (highest snapshot date).

    Trimmed to the columns Power BI needs; COD_DIS zero-padded to match
    DIM_DISTRITO. Points with no district in the source (a handful, bad
    bronze capture) are dropped rather than kept under an unknown-district
    sentinel -- DIM_DISTRITO has no such row to resolve them against.
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    query = f"""
        SELECT
            id AS ID_TRAFICO,
            LPAD(CAST(CAST(distrito AS INTEGER) AS VARCHAR), 2, '0') AS COD_DIS,
            nombre AS NOMBRE,
            latitud AS LATITUD,
            longitud AS LONGITUD
        FROM (
            SELECT DISTINCT ON (id) *
            FROM '{points_path}'
            ORDER BY id, 1 DESC
        )
        WHERE distrito IS NOT NULL
          AND latitud BETWEEN {MADRID_LAT_RANGE[0]} AND {MADRID_LAT_RANGE[1]}
          AND longitud BETWEEN {MADRID_LON_RANGE[0]} AND {MADRID_LON_RANGE[1]}
    """
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


def build_dim_magnitud(out_path: str) -> None:
    """Static catalog of the air quality gases -- no source table for this,
    the codes are the official MAGNITUD_LABELS mapping used to unpivot air.
    """
    from src.data.silver.aire import MAGNITUD_LABELS

    rows = [
        {"ID_MAGNITUD": code, "MAGNITUD": label}
        for code, label in MAGNITUD_LABELS.items()
    ]
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES {
            ', '.join(f"({r['ID_MAGNITUD']}, '{r['MAGNITUD']}')" for r in rows)
        }) AS t(ID_MAGNITUD, MAGNITUD)) TO '{out_path}' (FORMAT PARQUET)
    """)


def build_dim_medida_trafico(out_path: str) -> None:
    """Static catalog of traffic metrics -- lets FACT_TRAFICO_DIARIO be
    unpivoted (one row per metric) instead of one column per metric,
    mirroring FACT_CALIDAD_AIRE/DIM_MAGNITUD.
    """
    rows = [
        (1, "INTENSIDAD"),
        (2, "OCUPACION"),
        (3, "CARGA"),
        (4, "VMED"),
    ]
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES {
            ', '.join(f"({i}, '{m}')" for i, m in rows)
        }) AS t(ID_MEDIDA_TRAFICO, MEDIDA)) TO '{out_path}' (FORMAT PARQUET)
    """)


# NO2/SO2/CO/PM10/PM2_5/O3 map 1:1 to an ID_MAGNITUD (see dim_magnitud).
# BTX excluded on purpose: it's one station flag covering 6 different
# ID_MAGNITUD (TOL/BEN/EBE/MXY/PXY/OXY), no single id to point it at.
GAS_TO_ID_MAGNITUD = {"NO2": 8, "SO2": 1, "CO": 6, "PM10": 10, "PM2_5": 9, "O3": 14}
GAS_TO_MAGNITUD_NOMBRE = {"PM2_5": "PM2.5"}  # el resto coincide tal cual


def build_dim_distrito_magnitud(dim_estacion_path: str, out_path: str) -> None:
    """COD_DIS + gas: one row per gas measured by >=1 station in that
    district. Small lookup table, easy to drop straight into a Power BI
    table visual without parsing anything.

    Built from DIM_ESTACION_AIRE's gas flags (not FACT_CALIDAD_AIRE) to
    avoid a build-order dependency on facts -- same reasoning as
    build_dim_fecha reading silver instead of the facts.
    """
    gases = list(GAS_TO_ID_MAGNITUD)
    agg = ", ".join(f"max({g}) AS {g}" for g in gases)
    flags = duckdb.sql(f"""
        SELECT COD_DIS, {agg}
        FROM '{dim_estacion_path}'
        GROUP BY COD_DIS
    """).df()

    rows = [
        {
            "COD_DIS": row.COD_DIS,
            "ID_MAGNITUD": GAS_TO_ID_MAGNITUD[g],
            "MAGNITUD": GAS_TO_MAGNITUD_NOMBRE.get(g, g),
        }
        for row in flags.itertuples()
        for g in gases
        if getattr(row, g) == 1
    ]
    out = pd.DataFrame(rows, columns=["COD_DIS", "ID_MAGNITUD", "MAGNITUD"])
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)


def build_dim_distrito(
    distritos_path: str, dim_estacion_path: str, dim_punto_path: str, out_path: str
) -> None:
    """Districts + coverage columns (N_ESTACIONES_AIRE, N_PUNTOS_TRAFICO,
    COBERTURA_AIRE, COBERTURA_TRAFICO) -- flags districts with traffic but
    no air station, and vice versa.

    COD_DIS here is bronze's COD_DIS_TX (zero-padded) -- the same value
    every other table now calls COD_DIS, so this is the canonical district
    key for the whole star schema. No sentinel/unknown-district row: every
    consumer (DIM_PUNTO_TRAFICO, facts) only ever carries a real district
    code, so there's nothing to resolve against.
    """
    distritos = gpd.read_parquet(distritos_path)
    distritos["COD_DIS"] = distritos["COD_DIS_TX"].astype(str)
    keep = ["COD_DIS", "NOMBRE", "geometry"]
    distritos = distritos[keep].rename(columns={"geometry": "GEOMETRY"})
    distritos.columns = [c.upper() for c in distritos.columns]
    # rename() on the active geometry column loses GeoDataFrame's internal
    # tracking of which column is "the" geometry -- set_geometry re-points it
    # so to_parquet writes correct geo metadata (pointing at "GEOMETRY", not
    # the no-longer-existing "geometry").
    distritos = distritos.set_geometry("GEOMETRY")

    n_estaciones = duckdb.sql(f"""
        SELECT COD_DIS, count(*) AS N_ESTACIONES_AIRE
        FROM '{dim_estacion_path}'
        GROUP BY COD_DIS
    """).df()
    n_puntos = duckdb.sql(f"""
        SELECT COD_DIS, count(*) AS N_PUNTOS_TRAFICO
        FROM '{dim_punto_path}'
        GROUP BY COD_DIS
    """).df()

    distritos = distritos.merge(n_estaciones, on="COD_DIS", how="left")
    distritos = distritos.merge(n_puntos, on="COD_DIS", how="left")
    distritos["N_ESTACIONES_AIRE"] = (
        distritos["N_ESTACIONES_AIRE"].fillna(0).astype(int)
    )
    distritos["N_PUNTOS_TRAFICO"] = distritos["N_PUNTOS_TRAFICO"].fillna(0).astype(int)
    distritos["COBERTURA_AIRE"] = distritos["N_ESTACIONES_AIRE"] > 0
    distritos["COBERTURA_TRAFICO"] = distritos["N_PUNTOS_TRAFICO"] > 0

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    distritos.to_parquet(out_path)


def build_dim_geometria(distritos_path: str, out_path: str) -> None:
    """District boundaries as WKT text, keyed by COD_DIS -- the one table
    any Power BI mapping visual should read geometry from.

    Split out of DIM_DISTRITO on purpose: DIM_DISTRITO's own GEOMETRY column
    stays in its native CRS (EPSG:25830, WKB) for Gradio/geopandas, which is
    opaque to Power BI's parquet connector. This table reprojects to WGS84
    (EPSG:4326, lon/lat -- what every mapping visual expects) and writes it
    as plain text (`to_wkt()`), a data type Power BI can actually read.

    CENTROID_LAT/CENTROID_LON: centroid computed in the native (metric) CRS
    -- correct area centroid -- then reprojected to WGS84. Power BI's core
    "Map" visual (bubble/point, not the geocoded Filled Map) plots directly
    from Latitude/Longitude columns, no location-name geocoding involved --
    every district places correctly, unlike Filled Map's NOMBRE_GEO lookup
    which fails for ambiguous/compound Spanish names.
    """
    distritos = gpd.read_parquet(distritos_path)
    distritos["COD_DIS"] = distritos["COD_DIS_TX"].astype(str)
    centroide = distritos.geometry.centroid.set_crs(distritos.crs)
    centroide_wgs84 = centroide.to_crs("EPSG:4326")
    distritos["GEOMETRY_WKT"] = distritos["geometry"].to_crs("EPSG:4326").to_wkt()
    distritos["CENTROID_LAT"] = centroide_wgs84.y
    distritos["CENTROID_LON"] = centroide_wgs84.x
    out = pd.DataFrame(
        distritos[["COD_DIS", "GEOMETRY_WKT", "CENTROID_LAT", "CENTROID_LON"]]
    )

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)


def build_dim_fecha(
    silver_air_path: str, silver_traffic_path: str, out_path: str
) -> None:
    """One row per calendar day spanning both facts' date range.

    Built from silver (not from the facts) to avoid a build-order dependency
    -- silver's FECHA range is identical to what ends up in FACT_CALIDAD_AIRE
    /FACT_TRAFICO, since the facts don't filter dates any further. FACT_TRAFICO
    joins on CAST(FECHA AS DATE) since its own FECHA keeps 15-min resolution
    (hour-level analysis is a Power BI DAX/Power Query concern, not persisted
    here).
    """
    query = f"""
        WITH bounds AS (
            SELECT min(d) AS f0, max(d) AS f1 FROM (
                SELECT FECHA AS d FROM '{silver_air_path}'
                UNION ALL
                SELECT CAST(FECHA AS DATE) AS d FROM '{silver_traffic_path}'
            )
        )
        SELECT
            d AS FECHA,
            year(d) AS ANIO,
            month(d) AS MES
        FROM bounds, generate_series(f0, f1, INTERVAL 1 DAY) AS t(d)
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


def main(target: str = "all") -> None:
    dim_estacion_path = f"{GOLD_DIR}/dim_estacion_aire.parquet"
    dim_punto_path = f"{GOLD_DIR}/dim_punto_trafico.parquet"
    dim_distrito_path = f"{GOLD_DIR}/dim_distrito.parquet"
    dim_distrito_magnitud_path = f"{GOLD_DIR}/dim_distrito_magnitud.parquet"
    dim_geometria_path = f"{GOLD_DIR}/dim_geometria.parquet"
    dim_magnitud_path = f"{GOLD_DIR}/dim_magnitud.parquet"
    dim_medida_trafico_path = f"{GOLD_DIR}/dim_medida_trafico.parquet"
    dim_fecha_path = f"{GOLD_DIR}/dim_fecha.parquet"

    if target in ("dim_estacion_aire", "all"):
        build_dim_estacion_aire(SILVER_ESTACIONES_PATH, dim_estacion_path)
        logger.info("dim_estacion_aire -> %s", dim_estacion_path)

    if target in ("dim_punto_trafico", "all"):
        build_dim_punto_trafico(TRAFFIC_POINTS_PATH, dim_punto_path)
        logger.info("dim_punto_trafico -> %s", dim_punto_path)

    if target in ("dim_distrito", "all"):
        build_dim_distrito(
            DISTRITOS_PATH, dim_estacion_path, dim_punto_path, dim_distrito_path
        )
        logger.info("dim_distrito -> %s", dim_distrito_path)

    if target in ("dim_distrito_magnitud", "all"):
        build_dim_distrito_magnitud(dim_estacion_path, dim_distrito_magnitud_path)
        logger.info("dim_distrito_magnitud -> %s", dim_distrito_magnitud_path)

    if target in ("dim_geometria", "all"):
        build_dim_geometria(DISTRITOS_PATH, dim_geometria_path)
        logger.info("dim_geometria -> %s", dim_geometria_path)

    if target in ("dim_magnitud", "all"):
        build_dim_magnitud(dim_magnitud_path)
        logger.info("dim_magnitud -> %s", dim_magnitud_path)

    if target in ("dim_medida_trafico", "all"):
        build_dim_medida_trafico(dim_medida_trafico_path)
        logger.info("dim_medida_trafico -> %s", dim_medida_trafico_path)

    if target in ("dim_fecha", "all"):
        build_dim_fecha(SILVER_AIR_PATH, SILVER_TRAFFIC_PATH, dim_fecha_path)
        logger.info("dim_fecha -> %s", dim_fecha_path)

    if target == "all":
        _self_check_dim_distrito_magnitud(dim_distrito_magnitud_path, dim_magnitud_path)


def _self_check_dim_distrito_magnitud(path: str, dim_magnitud_path: str) -> None:
    valid = duckdb.sql(f"SELECT ID_MAGNITUD, MAGNITUD FROM '{dim_magnitud_path}'").df()
    rows = duckdb.sql(f"SELECT COD_DIS, ID_MAGNITUD, MAGNITUD FROM '{path}'").df()
    dup = rows.duplicated(subset=["COD_DIS", "ID_MAGNITUD"]).sum()
    assert dup == 0, f"{dup} pares COD_DIS+ID_MAGNITUD duplicados"
    assert set(rows["ID_MAGNITUD"]) <= set(valid["ID_MAGNITUD"]), "ID_MAGNITUD fuera de catalogo"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target",
        default="all",
        choices=["all", "dim_estacion_aire", "dim_punto_trafico", "dim_distrito",
                 "dim_distrito_magnitud", "dim_geometria", "dim_magnitud",
                 "dim_medida_trafico", "dim_fecha"],
    )  # fmt: skip
    main(parser.parse_args().target)
