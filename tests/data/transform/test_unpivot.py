import datetime

import duckdb

from src.data.transform.clean import unpivot_air_quality


def _write_bronze_fixture(path):
    day_cols = ", ".join(f"D{d:02d} DOUBLE, V{d:02d} VARCHAR" for d in range(1, 32))
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE bronze (
            ESTACION INTEGER, MAGNITUD INTEGER, ANO INTEGER, MES INTEGER, {day_cols}
        )
    """)
    # Real source data pads non-existent days with 0.0/'N', not NULL -- April
    # has 30 days, so day 31 (the last pair) must still be dropped on calendar
    # grounds even though it carries a non-null placeholder value.
    values = ", ".join("0.0, 'N'" for _ in range(29))
    duckdb.sql(f"""
        INSERT INTO bronze VALUES (4, 8, 2024, 4, 12.5, 'V', 9.0, 'N', {values})
    """)
    duckdb.sql(f"COPY bronze TO '{path}' (FORMAT PARQUET)")


def test_unpivot_air_quality_keeps_one_row_per_real_calendar_day(tmp_path):
    bronze_path = tmp_path / "bronze.parquet"
    _write_bronze_fixture(bronze_path)
    out_path = tmp_path / "long.parquet"

    unpivot_air_quality(str(bronze_path), str(out_path))

    rows = duckdb.sql(f"SELECT * FROM '{out_path}' ORDER BY fecha").fetchall()
    # April has 30 real days -- all of them, even validez='N' placeholders.
    assert len(rows) == 30
    assert rows[0][2] == datetime.date(2024, 4, 1)
    assert rows[0][3] == 12.5
    assert rows[0][4] == "V"
    assert rows[1][2] == datetime.date(2024, 4, 2)
    assert rows[1][4] == "N"


def test_unpivot_air_quality_maps_magnitud_code_to_label(tmp_path):
    bronze_path = tmp_path / "bronze.parquet"
    _write_bronze_fixture(bronze_path)
    out_path = tmp_path / "long.parquet"

    unpivot_air_quality(str(bronze_path), str(out_path))

    magnitud = duckdb.sql(f"SELECT DISTINCT magnitud FROM '{out_path}'").fetchone()
    assert magnitud == ("NO2",)


def test_unpivot_air_quality_drops_day_that_does_not_exist_in_month(tmp_path):
    bronze_path = tmp_path / "bronze.parquet"
    _write_bronze_fixture(bronze_path)
    out_path = tmp_path / "long.parquet"

    unpivot_air_quality(str(bronze_path), str(out_path))

    april_31st = duckdb.sql(
        f"SELECT * FROM '{out_path}' WHERE fecha = DATE '2024-05-01'"
    ).fetchall()
    assert april_31st == []
    max_day = duckdb.sql(f"SELECT max(fecha) FROM '{out_path}'").fetchone()[0]
    assert max_day == datetime.date(2024, 4, 30)
