import duckdb
import geopandas as gpd
from shapely.geometry import Polygon

from src.data.gold.dimensions import (
    build_dim_distrito,
    build_dim_estacion_aire,
    build_dim_magnitud,
    build_dim_punto_trafico,
)


def test_build_dim_estacion_aire_resolves_district(tmp_path):
    stations_path = tmp_path / "stations.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (4, 0.5, 0.5)) AS t(CODIGO, LONGITUD, LATITUD))
        TO '{stations_path}' (FORMAT PARQUET)
    """)
    distritos_path = tmp_path / "distritos.parquet"
    gpd.GeoDataFrame(
        {"COD_DIS": ["01"], "NOMBRE": ["Centro"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    ).to_parquet(distritos_path)

    out_path = tmp_path / "dim_estacion_aire.parquet"
    build_dim_estacion_aire(str(stations_path), str(distritos_path), str(out_path))

    result = duckdb.sql(f"SELECT CODIGO, COD_DIS FROM '{out_path}'").fetchall()
    assert result == [(4, "01")]


def test_build_dim_punto_trafico_copies_all_columns(tmp_path):
    points_path = tmp_path / "puntos.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 5, 'URB', 'Punto A'))
              AS t(id, distrito, tipo_elem, nombre))
        TO '{points_path}' (FORMAT PARQUET)
    """)

    out_path = tmp_path / "dim_punto_trafico.parquet"
    build_dim_punto_trafico(str(points_path), str(out_path))

    result = duckdb.sql(
        f"SELECT id, distrito, tipo_elem, nombre FROM '{out_path}'"
    ).fetchall()
    assert result == [(100, 5, "URB", "Punto A")]


def test_build_dim_distrito_adds_coverage_columns(tmp_path):
    # COD_DIS real va sin cero a la izquierda ("1", no "01") -- mismo
    # formato que bronze/distritos y silver/estaciones_aire de verdad.
    distritos_path = tmp_path / "distritos.parquet"
    gpd.GeoDataFrame(
        {"COD_DIS": ["1", "2"], "NOMBRE": ["Centro", "Tetuán"]},
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs="EPSG:4326",
    ).to_parquet(distritos_path)

    dim_estacion_path = tmp_path / "dim_estacion_aire.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (4, '1'), (8, '1'))
              AS t(CODIGO, COD_DIS))
        TO '{dim_estacion_path}' (FORMAT PARQUET)
    """)

    dim_punto_path = tmp_path / "dim_punto_trafico.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 1), (200, 1), (300, 2))
              AS t(id, distrito))
        TO '{dim_punto_path}' (FORMAT PARQUET)
    """)

    out_path = tmp_path / "dim_distrito.parquet"
    build_dim_distrito(
        str(distritos_path), str(dim_estacion_path), str(dim_punto_path), str(out_path)
    )

    result = gpd.read_parquet(out_path).sort_values("COD_DIS")
    row_1 = result[result["COD_DIS"] == "1"].iloc[0]
    row_2 = result[result["COD_DIS"] == "2"].iloc[0]

    # distrito 1: 2 estaciones aire, mucho tráfico (2 puntos) -> cobertura
    assert row_1["n_estaciones_aire"] == 2
    assert row_1["n_puntos_trafico"] == 2
    assert bool(row_1["cobertura_aire"]) is True

    # distrito 2: mucho tráfico (1 punto) pero SIN estación de aire -> sin cobertura
    assert row_2["n_estaciones_aire"] == 0
    assert row_2["n_puntos_trafico"] == 1
    assert bool(row_2["cobertura_aire"]) is False


def test_build_dim_magnitud_lists_all_gases(tmp_path):
    out_path = tmp_path / "dim_magnitud.parquet"
    build_dim_magnitud(str(out_path))

    result = duckdb.sql(
        f"SELECT magnitud FROM '{out_path}' WHERE codigo = 8"
    ).fetchone()
    assert result == ("NO2",)
    count = duckdb.sql(f"SELECT count(*) FROM '{out_path}'").fetchone()[0]
    assert count == 14
