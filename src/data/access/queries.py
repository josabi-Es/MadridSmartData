"""DuckDB read-only queries over processed Parquet, replacing the Spark reads."""

import functools

import duckdb


@functools.lru_cache(maxsize=1)
def get_traffic_districts(points_path: str) -> tuple[int, ...]:
    """Sorted distinct district ids from the traffic sensor locations.

    A handful of points ship with no district assigned at all (source data
    gap) -- excluded, or callers get a phantom "None" district option.
    Cached for the session.
    """
    query = f"""
        SELECT DISTINCT distrito FROM '{points_path}'
        WHERE distrito IS NOT NULL
        ORDER BY distrito
    """
    return tuple(r[0] for r in duckdb.sql(query).fetchall())


def get_stations(path: str) -> list[str]:
    """Distinct station ids, sorted, as strings (matches the old Spark UI contract)."""
    rows = duckdb.sql(
        f"SELECT DISTINCT estacion FROM '{path}' ORDER BY estacion"
    ).fetchall()
    return [str(r[0]) for r in rows]


def get_magnitudes(path: str, estacion_id: str | None = None) -> list[str]:
    """Distinct magnitudes, optionally filtered to one station."""
    where = f"WHERE estacion = {int(estacion_id)}" if estacion_id else ""
    query = f"SELECT DISTINCT magnitud FROM '{path}' {where} ORDER BY magnitud"
    return [r[0] for r in duckdb.sql(query).fetchall()]


def monthly_average(
    path: str, estacion_id: str, magnitud: str
) -> list[tuple[str, float]]:
    """Monthly average of `dato` for one station/magnitud, valid readings only."""
    query = f"""
        SELECT date_trunc('month', fecha) AS mes, avg(dato) AS media
        FROM '{path}'
        WHERE estacion = {int(estacion_id)}
          AND magnitud = '{magnitud}'
          AND validez = 'V'
        GROUP BY mes
        ORDER BY mes
    """
    return duckdb.sql(query).fetchall()


def daily_values_by_station(
    path: str, estacion_id: str, magnitud: str
) -> list[tuple[str, float]]:
    """Raw daily readings for one station/magnitud, valid only, unaggregated."""
    query = f"""
        SELECT CAST(fecha AS DATE) AS dia, dato
        FROM '{path}'
        WHERE estacion = {int(estacion_id)}
          AND magnitud = '{magnitud}'
          AND validez = 'V'
        ORDER BY dia
    """
    return duckdb.sql(query).fetchall()


TRAFFIC_VARIABLES = {"intensidad", "ocupacion", "carga", "vmed"}


def monthly_average_traffic(
    path: str, id_trafico: str, variable: str
) -> list[tuple[str, float]]:
    """Monthly average of a traffic variable for one sensor id, error='N' only."""
    if variable not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        SELECT date_trunc('month', fecha) AS mes, avg({variable}) AS media
        FROM '{path}'
        WHERE id = {int(id_trafico)} AND error = 'N'
        GROUP BY mes
        ORDER BY mes
    """
    return duckdb.sql(query).fetchall()


def district_monthly_average(
    air_path: str, stations_path: str, gas: str, anio: int, mes: int
) -> list[tuple[str, float]]:
    """Average of `gas` per district for one year/month, joined on station id."""
    query = f"""
        SELECT s.COD_DIS, avg(a.dato) AS valor_medio
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.estacion = s.CODIGO_CORTO
        WHERE a.magnitud = '{gas}'
          AND a.validez = 'V'
          AND extract(year FROM a.fecha) = {int(anio)}
          AND extract(month FROM a.fecha) = {int(mes)}
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
        SELECT DISTINCT a.magnitud
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.estacion = s.CODIGO_CORTO
        WHERE a.validez = 'V' AND s.COD_DIS = '{distrito}'
        ORDER BY a.magnitud
    """
    return tuple(r[0] for r in duckdb.sql(query).fetchall())


def daily_traffic_series(path: str, id_trafico: str, variable: str) -> list[tuple]:
    """Real daily series (day, value) for one traffic sensor, aggregated like
    `src/ml/features.py::build_traffic_features` (raw readings are 15-min)."""
    if variable not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        SELECT CAST(fecha AS DATE) AS dia, avg({variable}) AS valor
        FROM '{path}'
        WHERE id = {int(id_trafico)} AND error = 'N'
        GROUP BY dia
        ORDER BY dia
    """
    return duckdb.sql(query).fetchall()


def daily_average_air_by_district(
    air_path: str, stations_path: str, gas: str, distrito: str
) -> list[tuple]:
    """Daily average of `gas` for one district, joined on station id."""
    query = f"""
        SELECT CAST(a.fecha AS DATE) AS dia, avg(a.dato) AS media
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.estacion = s.CODIGO_CORTO
        WHERE a.magnitud = '{gas}' AND a.validez = 'V' AND s.COD_DIS = '{distrito}'
        GROUP BY dia
        ORDER BY dia
    """
    return duckdb.sql(query).fetchall()


def daily_average_traffic_by_district(
    traffic_path: str, points_path: str, variable: str, distrito: str
) -> list[tuple]:
    """Daily average (noon reading only) of a traffic variable for one district."""
    if variable not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        SELECT CAST(t.fecha AS DATE) AS dia, avg(t.{variable}) AS media
        FROM '{traffic_path}' t
        JOIN '{points_path}' p ON t.id = p.id
        WHERE t.error = 'N'
          AND EXTRACT(hour FROM t.fecha) = 12
          AND CAST(p.distrito AS VARCHAR) = '{distrito}'
        GROUP BY dia
        ORDER BY dia
    """
    return duckdb.sql(query).fetchall()


def district_monthly_traffic_average(
    traffic_path: str, points_path: str, variable: str, anio: int, mes: int
) -> list[tuple]:
    """Average of a traffic variable per district, for one year/month."""
    if variable not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        SELECT p.distrito, avg(t.{variable}) AS valor_medio
        FROM '{traffic_path}' t
        JOIN '{points_path}' p ON t.id = p.id
        WHERE t.error = 'N'
          AND extract(year FROM t.fecha) = {int(anio)}
          AND extract(month FROM t.fecha) = {int(mes)}
        GROUP BY p.distrito
        ORDER BY p.distrito
    """
    return duckdb.sql(query).fetchall()


def traffic_points_by_district(points_path: str, distrito: str) -> list[tuple]:
    """(id, nombre, longitud, latitud) of every traffic point in one district.

    Coordinates included so the dashboard map can drop individual markers
    once a district is picked, instead of listing ~4.962 points at once.
    """
    query = f"""
        SELECT id, nombre, longitud, latitud
        FROM '{points_path}'
        WHERE CAST(distrito AS VARCHAR) = '{distrito}'
        ORDER BY nombre
    """
    return duckdb.sql(query).fetchall()


def estaciones_aire_coords(stations_path: str) -> list[tuple]:
    """(codigo_corto, estacion, longitud, latitud, cod_dis) for every station.

    Only 24 stations total, so unlike traffic points these are cheap to
    always show on the map without waiting for a district filter.
    """
    query = f"""
        SELECT CODIGO_CORTO, ESTACION, LONGITUD, LATITUD, COD_DIS
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
        SELECT count(*) FROM '{points_path}'
        WHERE CAST(distrito AS VARCHAR) = '{distrito}'
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
        JOIN '{stations_path}' s ON a.estacion = s.CODIGO_CORTO
        WHERE a.magnitud = '{gas}' AND a.validez = 'V'
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
        SELECT DISTINCT extract(year FROM a.fecha)::INT AS anio,
               extract(month FROM a.fecha)::INT AS mes
        FROM '{air_path}' a
        JOIN '{stations_path}' s ON a.estacion = s.CODIGO_CORTO
        WHERE a.magnitud = '{gas}' AND a.validez = 'V' AND s.COD_DIS = '{distrito}'
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
    if variable not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    # Query only min/max instead of DISTINCT to avoid expensive GROUP BY on 52M rows
    query = f"""
        SELECT min(t.fecha)::DATE AS fecha_min, max(t.fecha)::DATE AS fecha_max
        FROM '{traffic_path}' t
        JOIN '{points_path}' p ON t.id = p.id
        WHERE t.error = 'N' AND CAST(p.distrito AS VARCHAR) = '{distrito}'
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
