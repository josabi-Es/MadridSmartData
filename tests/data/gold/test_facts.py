import duckdb

from src.data.gold.facts import build_fact_calidad_aire, build_fact_trafico


def test_build_fact_calidad_aire_joins_district_onto_silver_readings(tmp_path):
    silver_air_path = tmp_path / "aire.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (4, 'NO2', DATE '2024-01-01', 30.0, 'V')
        ) AS t(estacion, magnitud, fecha, dato, validez))
        TO '{silver_air_path}' (FORMAT PARQUET)
    """)
    dim_estacion_path = tmp_path / "dim_estacion_aire.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (4, '01'))
              AS t(CODIGO_CORTO, COD_DIS))
        TO '{dim_estacion_path}' (FORMAT PARQUET)
    """)

    out_path = tmp_path / "fact_calidad_aire.parquet"
    build_fact_calidad_aire(str(silver_air_path), str(dim_estacion_path), str(out_path))

    result = duckdb.sql(
        f"SELECT estacion, cod_dis, magnitud, dato, validez FROM '{out_path}'"
    ).fetchall()
    assert result == [(4, "01", "NO2", 30.0, "V")]


def test_build_fact_trafico_joins_district_onto_silver_readings(tmp_path):
    silver_traffic_path = tmp_path / "trafico.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (100, DATE '2024-01-01', 50.0, 10.0, 20.0, 30.0, 'N')
        ) AS t(id, fecha, intensidad, ocupacion, carga, vmed, error))
        TO '{silver_traffic_path}' (FORMAT PARQUET)
    """)
    dim_punto_path = tmp_path / "dim_punto_trafico.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 5)) AS t(id, distrito))
        TO '{dim_punto_path}' (FORMAT PARQUET)
    """)

    out_path = tmp_path / "fact_trafico.parquet"
    build_fact_trafico(str(silver_traffic_path), str(dim_punto_path), str(out_path))

    result = duckdb.sql(
        f"SELECT id, distrito, intensidad, carga, error FROM '{out_path}'"
    ).fetchall()
    assert result == [(100, 5, 50.0, 20.0, "N")]


def test_build_fact_calidad_aire_creates_missing_output_dir(tmp_path):
    silver_air_path = tmp_path / "aire.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (4, 'NO2', DATE '2024-01-01', 30.0, 'V'))
              AS t(estacion, magnitud, fecha, dato, validez))
        TO '{silver_air_path}' (FORMAT PARQUET)
    """)
    dim_estacion_path = tmp_path / "dim_estacion_aire.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (4, '01')) AS t(CODIGO_CORTO, COD_DIS))
        TO '{dim_estacion_path}' (FORMAT PARQUET)
    """)

    out_path = tmp_path / "nested" / "fact_calidad_aire.parquet"
    build_fact_calidad_aire(str(silver_air_path), str(dim_estacion_path), str(out_path))

    assert out_path.exists()
