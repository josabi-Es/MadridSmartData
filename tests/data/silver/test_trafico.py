import duckdb

from src.data.silver.trafico import clean_traffic


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
