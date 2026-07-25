"""Bronze -> processed cleaning, per official field docs in findings.md."""

from pathlib import Path

import duckdb
import geopandas as gpd
from shapely.geometry import Point

TRAFFIC_METRICS = ["intensidad", "ocupacion", "carga", "vmed"]

# Official magnitud codes, per the station structure doc (212629-2 PDF).
MAGNITUD_LABELS = {
    1: "SO2", 6: "CO", 7: "NO", 8: "NO2", 9: "PM2.5", 10: "PM10",
    12: "NOx", 14: "O3", 20: "TOL", 30: "BEN", 35: "EBE", 42: "TCH", 43: "CH4",
    44: "NMHC",
}  # fmt: skip


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
    duckdb.sql(f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET)")


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
