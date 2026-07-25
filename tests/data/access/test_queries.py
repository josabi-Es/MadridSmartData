import datetime

import duckdb

from src.data.access.queries import (
    daily_average_air_by_district,
    daily_average_traffic_by_district,
    district_monthly_average,
    get_magnitudes,
    get_stations,
    get_traffic_districts,
    monthly_average,
    monthly_average_traffic,
)


def _write_air_fixture(path):
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (4, 'NO2', DATE '2024-01-01', 10.0, 'V'),
            (4, 'NO2', DATE '2024-01-02', 20.0, 'V'),
            (4, 'NO2', DATE '2024-02-01', 30.0, 'V'),
            (4, 'NO2', DATE '2024-01-03', 999.0, 'N'),
            (8, 'PM10', DATE '2024-01-01', 5.0, 'V')
        ) AS t(estacion, magnitud, fecha, dato, validez))
        TO '{path}' (FORMAT PARQUET)
    """)


def test_get_stations_returns_sorted_distinct_ids(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    assert get_stations(str(path)) == ["4", "8"]


def test_get_magnitudes_all_stations(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    assert get_magnitudes(str(path)) == ["NO2", "PM10"]


def test_get_magnitudes_filtered_by_station(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    assert get_magnitudes(str(path), estacion_id="8") == ["PM10"]


def test_monthly_average_ignores_invalid_readings(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    result = monthly_average(str(path), estacion_id="4", magnitud="NO2")

    assert result == [
        (datetime.datetime(2024, 1, 1), 15.0),
        (datetime.datetime(2024, 2, 1), 30.0),
    ]


def _write_traffic_fixture(path):
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (3906, DATE '2024-01-01', 100.0, 'N'),
            (3906, DATE '2024-01-02', 200.0, 'N'),
            (3906, DATE '2024-01-03', 900.0, 'E'),
            (9999, DATE '2024-01-01', 5.0, 'N')
        ) AS t(id, fecha, intensidad, error))
        TO '{path}' (FORMAT PARQUET)
    """)


def test_monthly_average_traffic_ignores_errored_readings(tmp_path):
    path = tmp_path / "trafico.parquet"
    _write_traffic_fixture(path)

    result = monthly_average_traffic(
        str(path), id_trafico="3906", variable="intensidad"
    )

    assert result == [(datetime.datetime(2024, 1, 1), 150.0)]


def test_district_monthly_average_groups_by_district(tmp_path):
    air_path = tmp_path / "aire.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (4, 'NO2', DATE '2024-03-01', 10.0, 'V'),
            (4, 'NO2', DATE '2024-03-02', 20.0, 'V'),
            (8, 'NO2', DATE '2024-03-01', 100.0, 'V'),
            (4, 'NO2', DATE '2024-04-01', 999.0, 'V')
        ) AS t(estacion, magnitud, fecha, dato, validez))
        TO '{air_path}' (FORMAT PARQUET)
    """)
    stations_path = tmp_path / "estaciones.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (4, '01', 'Centro'),
            (8, '02', 'Salamanca')
        ) AS t(CODIGO_CORTO, COD_DIS, NOMBRE))
        TO '{stations_path}' (FORMAT PARQUET)
    """)

    result = district_monthly_average(
        str(air_path), str(stations_path), gas="NO2", anio=2024, mes=3
    )

    assert result == [("01", 15.0), ("02", 100.0)]


def test_daily_average_air_by_district(tmp_path):
    air_path = tmp_path / "aire.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (4, 'NO2', DATE '2024-03-01', 10.0, 'V'),
            (4, 'NO2', DATE '2024-03-01', 20.0, 'V'),
            (8, 'NO2', DATE '2024-03-01', 999.0, 'V')
        ) AS t(estacion, magnitud, fecha, dato, validez))
        TO '{air_path}' (FORMAT PARQUET)
    """)
    stations_path = tmp_path / "estaciones.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (4, '01'), (8, '02'))
              AS t(CODIGO_CORTO, COD_DIS))
        TO '{stations_path}' (FORMAT PARQUET)
    """)

    result = daily_average_air_by_district(
        str(air_path), str(stations_path), gas="NO2", distrito="01"
    )

    assert result == [(datetime.date(2024, 3, 1), 15.0)]


def test_daily_average_traffic_by_district(tmp_path):
    traffic_path = tmp_path / "trafico.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (100, TIMESTAMP '2024-03-01 12:00:00', 50.0, 'N'),
            (100, TIMESTAMP '2024-03-01 08:00:00', 999.0, 'N'),
            (200, TIMESTAMP '2024-03-01 12:00:00', 999.0, 'N')
        ) AS t(id, fecha, intensidad, error))
        TO '{traffic_path}' (FORMAT PARQUET)
    """)
    points_path = tmp_path / "puntos.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 5), (200, 6)) AS t(id, distrito))
        TO '{points_path}' (FORMAT PARQUET)
    """)

    result = daily_average_traffic_by_district(
        str(traffic_path), str(points_path), variable="intensidad", distrito="5"
    )

    assert result == [(datetime.date(2024, 3, 1), 50.0)]


def test_get_traffic_districts_returns_sorted_distinct(tmp_path):
    points_path = tmp_path / "puntos.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 5), (200, 2), (300, 5))
              AS t(id, distrito))
        TO '{points_path}' (FORMAT PARQUET)
    """)

    assert get_traffic_districts(str(points_path)) == [2, 5]
