import duckdb
import geopandas as gpd
from shapely.geometry import Point, Polygon

from src.data.transform.clean import assign_district, clean_traffic


def test_clean_traffic_turns_negative_values_into_null(tmp_path):
    bronze_path = tmp_path / "bronze.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (1, 100.0, 5.0, 20.0, 30.0),
            (2, -1.0, -1.0, -1.0, -1.0)
        ) AS t(id, intensidad, ocupacion, carga, vmed))
        TO '{bronze_path}' (FORMAT PARQUET)
    """)
    out_path = tmp_path / "processed.parquet"

    clean_traffic(str(bronze_path), str(out_path))

    query = f"SELECT id, intensidad, carga FROM '{out_path}' ORDER BY id"
    result = duckdb.sql(query).fetchall()
    assert result[0] == (1, 100.0, 20.0)
    assert result[1][1] is None
    assert result[1][2] is None


def test_clean_traffic_creates_missing_output_dir(tmp_path):
    bronze_path = tmp_path / "bronze.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (1, 10.0, 5.0, 20.0, 30.0))
              AS t(id, intensidad, ocupacion, carga, vmed))
        TO '{bronze_path}' (FORMAT PARQUET)
    """)
    out_path = tmp_path / "nested" / "processed.parquet"

    clean_traffic(str(bronze_path), str(out_path))

    assert out_path.exists()


def test_assign_district_matches_point_to_containing_polygon(tmp_path):
    stations_path = tmp_path / "stations.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (4, 0.5, 0.5),
            (8, 2.5, 0.5)
        ) AS t(CODIGO, LONGITUD, LATITUD))
        TO '{stations_path}' (FORMAT PARQUET)
    """)

    districts_path = tmp_path / "districts.parquet"
    districts = gpd.GeoDataFrame(
        {"COD_DIS": ["01", "02"], "NOMBRE": ["Centro", "Salamanca"]},
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs="EPSG:4326",
    )
    districts.to_parquet(districts_path)

    out_path = tmp_path / "stations_district.parquet"
    assign_district(str(stations_path), str(districts_path), str(out_path))

    query = f"SELECT CODIGO, COD_DIS, NOMBRE FROM '{out_path}' ORDER BY CODIGO"
    result = duckdb.sql(query).fetchall()
    assert result == [(4, "01", "Centro"), (8, "02", "Salamanca")]


def test_assign_district_leaves_unmatched_station_with_null(tmp_path):
    stations_path = tmp_path / "stations.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (99, 50.0, 50.0)) AS t(CODIGO, LONGITUD, LATITUD))
        TO '{stations_path}' (FORMAT PARQUET)
    """)

    districts_path = tmp_path / "districts.parquet"
    districts = gpd.GeoDataFrame(
        {"COD_DIS": ["01"], "NOMBRE": ["Centro"]},
        geometry=[Point(0.5, 0.5).buffer(0.5)],
        crs="EPSG:4326",
    )
    districts.to_parquet(districts_path)

    out_path = tmp_path / "stations_district.parquet"
    assign_district(str(stations_path), str(districts_path), str(out_path))

    result = duckdb.sql(f"SELECT COD_DIS FROM '{out_path}'").fetchone()
    assert result == (None,)
