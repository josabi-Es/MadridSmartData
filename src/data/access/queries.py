"""DuckDB read-only queries over processed Parquet, replacing the Spark reads."""

import functools

import duckdb


@functools.lru_cache(maxsize=1)
def get_traffic_districts(points_path: str) -> tuple[str, ...]:
    """Sorted distinct district codes from the traffic sensor locations.

    Cached for the session.
    """
    query = f"""
        SELECT DISTINCT COD_DIS FROM '{points_path}'
        ORDER BY COD_DIS
    """
    return tuple(r[0] for r in duckdb.sql(query).fetchall())


def get_stations(path: str) -> list[str]:
    """Distinct station ids, sorted, as strings (matches the old Spark UI contract)."""
    rows = duckdb.sql(
        f"SELECT DISTINCT ID_AIRE FROM '{path}' ORDER BY ID_AIRE"
    ).fetchall()
    return [str(r[0]) for r in rows]


def get_magnitudes(path: str, estacion_id: str | None = None) -> list[str]:
    """Distinct magnitudes, optionally filtered to one station."""
    where = f"WHERE ID_AIRE = {int(estacion_id)}" if estacion_id else ""
    query = f"SELECT DISTINCT MAGNITUD FROM '{path}' {where} ORDER BY MAGNITUD"
    return [r[0] for r in duckdb.sql(query).fetchall()]


def monthly_average(
    path: str, estacion_id: str, magnitud: str
) -> list[tuple[str, float]]:
    """Monthly average of DATO for one station/magnitud, valid readings only."""
    query = f"""
        SELECT date_trunc('month', FECHA) AS mes, avg(DATO) AS media
        FROM '{path}'
        WHERE ID_AIRE = {int(estacion_id)}
          AND MAGNITUD = '{magnitud}'
          AND VALIDEZ = 'V'
        GROUP BY mes
        ORDER BY mes
    """
    return duckdb.sql(query).fetchall()


def daily_values_by_station(
    path: str, estacion_id: str, magnitud: str
) -> list[tuple[str, float]]:
    """Raw daily readings for one station/magnitud, valid only, unaggregated."""
    query = f"""
        SELECT CAST(FECHA AS DATE) AS dia, DATO
        FROM '{path}'
        WHERE ID_AIRE = {int(estacion_id)}
          AND MAGNITUD = '{magnitud}'
          AND VALIDEZ = 'V'
        ORDER BY dia
    """
    return duckdb.sql(query).fetchall()


TRAFFIC_VARIABLES = {"INTENSIDAD", "OCUPACION", "CARGA", "VMED"}


def monthly_average_traffic(
    path: str, id_trafico: str, variable: str
) -> list[tuple[str, float]]:
    """Monthly average of a traffic variable for one sensor id (already
    error-free -- silver drops ERROR != 'N' rows)."""
    if variable.upper() not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        SELECT date_trunc('month', FECHA) AS mes, avg({variable}) AS media
        FROM '{path}'
        WHERE ID_TRAFICO = {int(id_trafico)}
        GROUP BY mes
        ORDER BY mes
    """
    return duckdb.sql(query).fetchall()


def district_monthly_average(
    air_path: str, stations_path: str, gas: str, anio: int, mes: int
) -> list[tuple[str, float]]:
    """Average of `gas` per district for one year/month, joined on station id."""
    query = f"""
        SELECT s.COD_DIS, avg(a.DATO) AS valor_medio
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.ID_AIRE = s.ID_AIRE
        WHERE a.MAGNITUD = '{gas}'
          AND a.VALIDEZ = 'V'
          AND extract(year FROM a.FECHA) = {int(anio)}
          AND extract(month FROM a.FECHA) = {int(mes)}
        GROUP BY s.COD_DIS
        ORDER BY s.COD_DIS
    """
    return duckdb.sql(query).fetchall()


@functools.lru_cache(maxsize=32)
def get_air_variables_by_district(
    air_path: str, stations_path: str, distrito: str
) -> tuple[str, ...]:
    """Gases with at least one valid reading in this district's stations.

    Some districts' station(s) don't measure every gas -- without this,
    picking e.g. PM2.5 in a district that only tracks NO2/O3 just shows an
    empty chart with no explanation. Cached for the session.
    """
    query = f"""
        SELECT DISTINCT a.MAGNITUD
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.ID_AIRE = s.ID_AIRE
        WHERE a.VALIDEZ = 'V' AND s.COD_DIS = '{distrito}'
        ORDER BY a.MAGNITUD
    """
    return tuple(r[0] for r in duckdb.sql(query).fetchall())


def daily_traffic_series(path: str, id_trafico: str, variable: str) -> list[tuple]:
    """Real daily series (day, value) for one traffic sensor, aggregated like
    `src/ml/core/features.py` (raw readings are 15-min)."""
    if variable.upper() not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        SELECT CAST(FECHA AS DATE) AS dia, avg({variable}) AS valor
        FROM '{path}'
        WHERE ID_TRAFICO = {int(id_trafico)}
        GROUP BY dia
        ORDER BY dia
    """
    return duckdb.sql(query).fetchall()


def daily_average_air_by_district(
    air_path: str, stations_path: str, gas: str, distrito: str
) -> list[tuple]:
    """Daily average of `gas` for one district, joined on station id."""
    query = f"""
        SELECT CAST(a.FECHA AS DATE) AS dia, avg(a.DATO) AS media
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.ID_AIRE = s.ID_AIRE
        WHERE a.MAGNITUD = '{gas}' AND a.VALIDEZ = 'V' AND s.COD_DIS = '{distrito}'
        GROUP BY dia
        ORDER BY dia
    """
    return duckdb.sql(query).fetchall()


def daily_average_traffic_by_district(
    traffic_path: str, points_path: str, variable: str, distrito: str
) -> list[tuple]:
    """Daily average (noon reading only) of a traffic variable for one district."""
    if variable.upper() not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        SELECT CAST(t.FECHA AS DATE) AS dia, avg(t.{variable}) AS media
        FROM '{traffic_path}' t
        JOIN '{points_path}' p ON t.ID_TRAFICO = p.ID_TRAFICO
        WHERE EXTRACT(hour FROM t.FECHA) = 12
          AND p.COD_DIS = '{distrito}'
        GROUP BY dia
        ORDER BY dia
    """
    return duckdb.sql(query).fetchall()


def district_monthly_traffic_average(
    traffic_path: str, points_path: str, variable: str, anio: int, mes: int
) -> list[tuple]:
    """Average of a traffic variable per district, for one year/month."""
    if variable.upper() not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        SELECT p.COD_DIS, avg(t.{variable}) AS valor_medio
        FROM '{traffic_path}' t
        JOIN '{points_path}' p ON t.ID_TRAFICO = p.ID_TRAFICO
        WHERE extract(year FROM t.FECHA) = {int(anio)}
          AND extract(month FROM t.FECHA) = {int(mes)}
        GROUP BY p.COD_DIS
        ORDER BY p.COD_DIS
    """
    return duckdb.sql(query).fetchall()


def traffic_points_by_district(points_path: str, distrito: str) -> list[tuple]:
    """(id, nombre, longitud, latitud) of every traffic point in one district.

    Coordinates included so the dashboard map can drop individual markers
    once a district is picked, instead of listing ~4.962 points at once.
    """
    query = f"""
        SELECT ID_TRAFICO, NOMBRE, LONGITUD, LATITUD
        FROM '{points_path}'
        WHERE COD_DIS = '{distrito}'
        ORDER BY NOMBRE
    """
    return duckdb.sql(query).fetchall()


def estaciones_aire_coords(stations_path: str) -> list[tuple]:
    """(ID_AIRE, ESTACION, LONGITUD, LATITUD, COD_DIS) for every station.

    Only 24 stations total, so unlike traffic points these are cheap to
    always show on the map without waiting for a district filter.
    """
    query = f"""
        SELECT ID_AIRE, ESTACION, LONGITUD, LATITUD, COD_DIS
        FROM '{stations_path}'
        ORDER BY ESTACION
    """
    return duckdb.sql(query).fetchall()


def count_stations_by_district(stations_path: str, distrito: str) -> int:
    """How many air-quality stations sit in one district."""
    query = f"""
        SELECT count(*) FROM '{stations_path}' WHERE COD_DIS = '{distrito}'
    """
    return duckdb.sql(query).fetchone()[0]


def count_traffic_points_by_district(points_path: str, distrito: str) -> int:
    """How many traffic measuring points sit in one district."""
    query = f"""
        SELECT count(*) FROM '{points_path}' WHERE COD_DIS = '{distrito}'
    """
    return duckdb.sql(query).fetchone()[0]


@functools.lru_cache(maxsize=32)
def get_air_districts_by_variable(
    air_path: str, stations_path: str, gas: str
) -> list[str]:
    """Districts with at least one valid reading of `gas` -- the reverse
    cascade of `get_air_variables_by_district`, for narrowing the distrito
    dropdown once a gas is picked. Cached for the session."""
    query = f"""
        SELECT DISTINCT s.COD_DIS
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.ID_AIRE = s.ID_AIRE
        WHERE a.MAGNITUD = '{gas}' AND a.VALIDEZ = 'V'
        ORDER BY s.COD_DIS
    """
    return tuple(r[0] for r in duckdb.sql(query).fetchall())


@functools.lru_cache(maxsize=32)
def get_air_periods_by_district(
    air_path: str, stations_path: str, gas: str, distrito: str
) -> tuple[tuple[int, int], ...]:
    """Distinct (year, month) with a valid `gas` reading in this district.
    Cached for the session."""
    query = f"""
        SELECT DISTINCT extract(year FROM a.FECHA)::INT AS anio,
               extract(month FROM a.FECHA)::INT AS mes
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.ID_AIRE = s.ID_AIRE
        WHERE a.MAGNITUD = '{gas}' AND a.VALIDEZ = 'V' AND s.COD_DIS = '{distrito}'
        ORDER BY anio, mes
    """
    return tuple(duckdb.sql(query).fetchall())


@functools.lru_cache(maxsize=32)
def get_traffic_periods_by_district(
    traffic_path: str, points_path: str, variable: str, distrito: str
) -> tuple[tuple[int, int], ...]:
    """(year, month) tuples for all months between min and max fecha with a
    valid reading of a traffic variable in this district. Assumes complete
    monthly blocks (no gaps of single months within coverage range).
    No hour filter (unlike `daily_average_traffic_by_district`).
    Cached for the session."""
    if variable.upper() not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    # Query only min/max instead of DISTINCT to avoid expensive GROUP BY on 52M rows
    query = f"""
        SELECT min(t.FECHA)::DATE AS fecha_min, max(t.FECHA)::DATE AS fecha_max
        FROM '{traffic_path}' t
        JOIN '{points_path}' p ON t.ID_TRAFICO = p.ID_TRAFICO
        WHERE p.COD_DIS = '{distrito}'
    """
    result = duckdb.sql(query).fetchall()
    if not result or result[0][0] is None:
        return ()

    fecha_min, fecha_max = result[0]
    # Generate all consecutive (year, month) tuples between min/max
    periodos = []
    current = fecha_min.replace(day=1)
    while current <= fecha_max:
        periodos.append((current.year, current.month))
        # Move to next month (handles year boundary)
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return tuple(periodos)
