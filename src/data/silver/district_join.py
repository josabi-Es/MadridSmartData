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
    "SILVER_ESTACIONES_PATH", "data/silver/estaciones_aire/latest.parquet"
)


def assign_district(stations_path: str, districts_path: str, out_path: str) -> None:
    """Spatial-join air stations (lon/lat) to their containing district.

    Air station metadata has no district field (confirmed in
    001-descubrimiento-catalago/findings.md), so this join is only needed
    for air -- traffic points already carry `distrito` directly.
    """
    stations = duckdb.sql(f"SELECT * FROM '{stations_path}'").df()
    stations_gdf = gpd.GeoDataFrame(
        stations,
        geometry=[Point(xy) for xy in zip(stations["LONGITUD"], stations["LATITUD"])],
        crs="EPSG:4326",
    )
    districts = gpd.read_parquet(districts_path).to_crs("EPSG:4326")

    district_cols = districts[["COD_DIS", "NOMBRE", "geometry"]]
    joined = gpd.sjoin(stations_gdf, district_cols, how="left", predicate="within")
    joined = joined.drop(columns=["geometry", "index_right"])

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY (SELECT * FROM joined) TO '{out_path}' (FORMAT PARQUET)")


def main() -> None:
    assign_district(BRONZE_ESTACIONES_PATH, DISTRITOS_PATH, SILVER_ESTACIONES_PATH)


if __name__ == "__main__":
    main()
