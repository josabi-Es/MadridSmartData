"""Options for dashboard dropdowns -- no business logic."""

from src.data.access.queries import (
    TRAFFIC_VARIABLES,
    get_air_districts_by_variable,
    get_air_periods_by_district,
    get_air_variables_by_district,
    get_traffic_districts,
    get_traffic_periods_by_district,
)

TRAFFIC_POINTS_PATH = "data/gold/dim_punto_trafico.parquet"
AIRQUALITY_PATH = "data/silver/aire.parquet"
TRAFFIC_PATH = "data/silver/trafico.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire.parquet"

AIR_VARIABLES = ["NO2", "PM10", "PM2.5", "O3", "NOx"]

# Full range -- initial state of dropdowns and fallback if a filter combo has no periods with data.
ANIOS_FALLBACK = [2020, 2021, 2022, 2023, 2024]
MESES_FALLBACK = list(range(1, 13))

UNITS = {
    "NO2": "µg/m³", "PM10": "µg/m³", "PM2.5": "µg/m³", "O3": "µg/m³", "NOx": "µg/m³",
    "INTENSIDAD": "veh/h", "OCUPACION": "%", "CARGA": "%", "VMED": "km/h",
}  # fmt: skip


def obtener_distritos(dominio=None, variable=None):
    """Traffic: always 21 districts (points cover all).

    Air: if gas chosen, only districts with real readings of that gas --
    if none (data gap), falls back to full list rather than empty dropdown.
    """
    if dominio == "Aire" and variable:
        disponibles = get_air_districts_by_variable(
            AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, variable
        )
        # COD_DIS is zero-padded ("01", not "1") everywhere now -- both lists
        # already share that representation, no normalization needed.
        return sorted(disponibles) or get_traffic_districts(TRAFFIC_POINTS_PATH)
    return get_traffic_districts(TRAFFIC_POINTS_PATH)


def obtener_variables(dominio, distrito=None):
    """Traffic: always 4 variables (same schema at every point).

    Air: if district chosen, only gases with real readings there --
    if none (data gap), falls back to full list rather than empty dropdown.
    """
    if dominio != "Aire":
        return sorted(TRAFFIC_VARIABLES)
    if not distrito:
        return AIR_VARIABLES
    disponibles = get_air_variables_by_district(
        AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, str(distrito)
    )
    return disponibles or AIR_VARIABLES


def _periodos_disponibles(dominio, variable, distrito):
    if dominio == "Aire":
        return get_air_periods_by_district(
            AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, variable, str(distrito)
        )
    return get_traffic_periods_by_district(
        TRAFFIC_PATH, TRAFFIC_POINTS_PATH, variable, str(distrito)
    )


def obtener_anios(dominio, variable, distrito):
    """Years with at least one real data point for domain/variable/district.

    Without district/variable chosen (app startup) falls to full range --
    same as if combo has no data.
    """
    if not variable or not distrito:
        return ANIOS_FALLBACK
    anios = sorted({anio for anio, _ in _periodos_disponibles(dominio, variable, distrito)})
    return anios or ANIOS_FALLBACK


def obtener_meses(dominio, variable, distrito, anio):
    """Months with at least one real data point for that year, within domain/variable/district."""
    if not variable or not distrito or not anio:
        return MESES_FALLBACK
    meses = sorted(
        mes
        for a, mes in _periodos_disponibles(dominio, variable, distrito)
        if a == int(anio)
    )
    return meses or MESES_FALLBACK
