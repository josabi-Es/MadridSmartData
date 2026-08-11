"""Spatial join shared by air stations (and gold dims) -- district assignment."""

import os
from pathlib import Path

import duckdb
import geopandas as gpd
from dotenv import load_dotenv
from shapely.geometry import Point

load_dotenv()

BRONZE_ESTACIONES_PATH = os.getenv(
    "BRONZE_ESTACIONES_PATH", "data/bronze/estaciones_aire/latest.parquet"
)
DISTRITOS_PATH = os.getenv("DISTRITOS_PATH", "data/bronze/distritos/latest.parquet")
SILVER_ESTACIONES_PATH = os.getenv(
    "SILVER_ESTACIONES_PATH", "data/silver/estaciones_aire.parquet"
)


# Bronze column -> silver column, for the columns whose name isn't already
# a valid uppercase identifier. Everything else is already uppercase with
# no accents/spaces and is kept as-is.
COLUMN_RENAME = {
    "CODIGO_CORTO": "ID_AIRE",
    "Fecha alta": "FECHA_ALTA",
}


# Comunidad de Madrid bounding box, generous margin (not the tighter
# municipio-only box) -- wide enough to keep every real station/point,
# tight enough to drop stray bad coordinates (e.g. a station geocoded into
# the sea) before they ever reach silver/gold.
MADRID_LAT_RANGE = (39.85, 41.20)
MADRID_LON_RANGE = (-4.60, -3.05)


def assign_district(stations_path: str, districts_path: str, out_path: str) -> None:
    """Spatial-join air stations (lon/lat) to their containing district.

    Air station metadata has no district field (confirmed in
    001-descubrimiento-catalago/findings.md), so this join is only needed
    for air -- traffic points already carry `distrito` directly.

    Uses `COD_DIS_TX` (zero-padded, e.g. "09") as the district key instead
    of `COD_DIS` (no padding, e.g. "9") -- COD_DIS_TX becomes the project's
    one and only COD_DIS from here on, so every table joins on the same
    representation.

    Stations outside `MADRID_LAT_RANGE`/`MADRID_LON_RANGE` are dropped
    before the spatial join -- a handful of stations carry bad
    lat/long (bronze capture errors), which would otherwise join to no
    district (silently dropped by the `within` predicate anyway) or,
    worse, resolve to a nonsense location downstream.
    """
    stations = duckdb.sql(f"""
        SELECT * FROM '{stations_path}'
        WHERE LATITUD BETWEEN {MADRID_LAT_RANGE[0]} AND {MADRID_LAT_RANGE[1]}
          AND LONGITUD BETWEEN {MADRID_LON_RANGE[0]} AND {MADRID_LON_RANGE[1]}
    """).df()
    stations_gdf = gpd.GeoDataFrame(
        stations,
        geometry=[Point(xy) for xy in zip(stations["LONGITUD"], stations["LATITUD"])],
        crs="EPSG:4326",
    )
    districts = gpd.read_parquet(districts_path).to_crs("EPSG:4326")

    district_cols = districts[["COD_DIS_TX", "NOMBRE", "geometry"]].rename(
        columns={"COD_DIS_TX": "COD_DIS"}
    )
    joined = gpd.sjoin(stations_gdf, district_cols, how="left", predicate="within")
    joined = joined.drop(columns=["geometry", "index_right"])
    joined = joined.rename(columns=COLUMN_RENAME)

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY (SELECT * FROM joined) TO '{out_path}' (FORMAT PARQUET)")


def main() -> None:
    assign_district(BRONZE_ESTACIONES_PATH, DISTRITOS_PATH, SILVER_ESTACIONES_PATH)


if __name__ == "__main__":
    main()
