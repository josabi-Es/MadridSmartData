import datetime

import duckdb

from src.data.access.queries import (
    count_stations_by_district,
    count_traffic_points_by_district,
    daily_average_air_by_district,
    daily_average_traffic_by_district,
    daily_values_by_station,
    district_monthly_average,
    district_monthly_traffic_average,
    estaciones_aire_coords,
    get_magnitudes,
    get_stations,
    get_traffic_districts,
    monthly_average,
    monthly_average_traffic,
    traffic_points_by_district,
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


def test_daily_values_by_station_returns_raw_daily_readings(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    result = daily_values_by_station(str(path), estacion_id="4", magnitud="NO2")

    assert result == [
        (datetime.date(2024, 1, 1), 10.0),
        (datetime.date(2024, 1, 2), 20.0),
        (datetime.date(2024, 2, 1), 30.0),
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


def test_get_traffic_districts_excludes_null_distrito(tmp_path):
    points_path = tmp_path / "puntos.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 5), (200, NULL), (300, 2))
              AS t(id, distrito))
        TO '{points_path}' (FORMAT PARQUET)
    """)

    assert get_traffic_districts(str(points_path)) == [2, 5]


def test_district_monthly_traffic_average_groups_by_district(tmp_path):
    traffic_path = tmp_path / "trafico.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (100, TIMESTAMP '2024-03-01 08:00:00', 10.0, 'N'),
            (100, TIMESTAMP '2024-03-02 08:00:00', 20.0, 'N'),
            (200, TIMESTAMP '2024-03-01 08:00:00', 100.0, 'N'),
            (100, TIMESTAMP '2024-04-01 08:00:00', 999.0, 'N'),
            (100, TIMESTAMP '2024-03-03 08:00:00', 999.0, 'E')
        ) AS t(id, fecha, intensidad, error))
        TO '{traffic_path}' (FORMAT PARQUET)
    """)
    points_path = tmp_path / "puntos.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 5), (200, 6)) AS t(id, distrito))
        TO '{points_path}' (FORMAT PARQUET)
    """)

    result = district_monthly_traffic_average(
        str(traffic_path), str(points_path), variable="intensidad", anio=2024, mes=3
    )

    assert result == [(5, 15.0), (6, 100.0)]


def test_district_monthly_traffic_average_rejects_unknown_variable(tmp_path):
    traffic_path = tmp_path / "trafico.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, TIMESTAMP '2024-03-01 08:00:00', 10.0, 'N'))
              AS t(id, fecha, intensidad, error))
        TO '{traffic_path}' (FORMAT PARQUET)
    """)
    points_path = tmp_path / "puntos.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 5)) AS t(id, distrito))
        TO '{points_path}' (FORMAT PARQUET)
    """)

    try:
        district_monthly_traffic_average(
            str(traffic_path), str(points_path), variable="nope", anio=2024, mes=3
        )
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for unknown traffic variable")


def test_traffic_points_by_district_returns_id_name_and_coords(tmp_path):
    points_path = tmp_path / "puntos.parquet"
    duckdb.sql(f"""
        COPY (SELECT id, distrito, nombre,
                     CAST(longitud AS DOUBLE) AS longitud,
                     CAST(latitud AS DOUBLE) AS latitud
              FROM (VALUES
                (100, 5, 'Punto A', -3.70, 40.42),
                (200, 6, 'Punto B', -3.68, 40.41),
                (300, 5, 'Punto C', -3.71, 40.43)
              ) AS t(id, distrito, nombre, longitud, latitud))
        TO '{points_path}' (FORMAT PARQUET)
    """)

    result = traffic_points_by_district(str(points_path), distrito="5")

    assert result == [
        (100, "Punto A", -3.70, 40.42),
        (300, "Punto C", -3.71, 40.43),
    ]


def test_estaciones_aire_coords_returns_all_stations(tmp_path):
    stations_path = tmp_path / "estaciones.parquet"
    duckdb.sql(f"""
        COPY (SELECT CODIGO_CORTO, ESTACION,
                     CAST(LONGITUD AS DOUBLE) AS LONGITUD,
                     CAST(LATITUD AS DOUBLE) AS LATITUD,
                     COD_DIS
              FROM (VALUES
                (4, 'Plaza de España', -3.71, 40.42, '01'),
                (8, 'Escuelas Aguirre', -3.68, 40.42, '04')
              ) AS t(CODIGO_CORTO, ESTACION, LONGITUD, LATITUD, COD_DIS))
        TO '{stations_path}' (FORMAT PARQUET)
    """)

    result = estaciones_aire_coords(str(stations_path))

    assert result == [
        (8, "Escuelas Aguirre", -3.68, 40.42, "04"),
        (4, "Plaza de España", -3.71, 40.42, "01"),
    ]


def test_count_stations_by_district(tmp_path):
    stations_path = tmp_path / "estaciones.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            (4, '01', 'Centro'),
            (8, '01', 'Centro'),
            (11, '02', 'Salamanca')
        ) AS t(CODIGO_CORTO, COD_DIS, NOMBRE))
        TO '{stations_path}' (FORMAT PARQUET)
    """)

    assert count_stations_by_district(str(stations_path), distrito="01") == 2
    assert count_stations_by_district(str(stations_path), distrito="02") == 1


def test_count_traffic_points_by_district(tmp_path):
    points_path = tmp_path / "puntos.parquet"
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES (100, 5), (200, 6), (300, 5))
              AS t(id, distrito))
        TO '{points_path}' (FORMAT PARQUET)
    """)

    assert count_traffic_points_by_district(str(points_path), distrito="5") == 2
    assert count_traffic_points_by_district(str(points_path), distrito="6") == 1
