"""Spatial join shared by air stations (and gold dims) -- district assignment."""

from pathlib import Path

import duckdb
import geopandas as gpd
from shapely.geometry import Point


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
