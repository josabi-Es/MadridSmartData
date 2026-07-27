"""Opciones para los desplegables del dashboard -- sin lógica de negocio."""

from src.data.access.queries import (
    TRAFFIC_VARIABLES,
    get_air_districts_by_variable,
    get_air_periods_by_district,
    get_air_variables_by_district,
    get_traffic_districts,
    get_traffic_periods_by_district,
)

TRAFFIC_POINTS_PATH = "data/bronze/trafico_puntos_medida/*.parquet"
AIRQUALITY_PATH = "data/silver/aire/all.parquet"
TRAFFIC_PATH = "data/silver/trafico/all.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire/latest.parquet"

AIR_VARIABLES = ["NO2", "PM10", "PM2.5", "O3", "NOx"]

# Rango completo -- estado inicial de los desplegables y fallback si una
# combinación filtro no tiene ningún periodo con datos.
ANIOS_FALLBACK = [2020, 2021, 2022, 2023, 2024]
MESES_FALLBACK = list(range(1, 13))

UNITS = {
    "NO2": "µg/m³", "PM10": "µg/m³", "PM2.5": "µg/m³", "O3": "µg/m³", "NOx": "µg/m³",
    "intensidad": "veh/h", "ocupacion": "%", "carga": "%", "vmed": "km/h",
}  # fmt: skip


def obtener_distritos(dominio=None, variable=None):
    """Tráfico: los 21 distritos siempre (los puntos los cubren todos).

    Aire: si hay gas elegido, solo los distritos con lectura real de ese
    gas -- si no hay ninguno (hueco de datos), cae a la lista completa
    antes que dejar el desplegable vacío.
    """
    if dominio == "Aire" and variable:
        disponibles = get_air_districts_by_variable(
            AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, variable
        )
        # COD_DIS es VARCHAR sin cero a la izquierda ("1", no "01") -- se
        # normaliza a int para que el desplegable no mezcle tipos con la
        # lista de tráfico (que ya viene en int).
        return sorted(int(d) for d in disponibles) or get_traffic_districts(
            TRAFFIC_POINTS_PATH
        )
    return get_traffic_districts(TRAFFIC_POINTS_PATH)


def obtener_variables(dominio, distrito=None):
    """Tráfico: las 4 variables siempre (mismo esquema en todo punto).

    Aire: si hay distrito elegido, solo los gases con lectura real ahí --
    si no hay ninguno (hueco de datos), se cae de vuelta a la lista
    completa antes que dejar el desplegable vacío.
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
    """Años con al menos un dato real para dominio/variable/distrito.

    Sin distrito/variable elegidos (arranque de la app) cae al rango
    completo -- igual que si la combinación no tiene ningún dato.
    """
    if not variable or not distrito:
        return ANIOS_FALLBACK
    anios = sorted({anio for anio, _ in _periodos_disponibles(dominio, variable, distrito)})
    return anios or ANIOS_FALLBACK


def obtener_meses(dominio, variable, distrito, anio):
    """Meses con al menos un dato real para ese año, dentro de dominio/variable/distrito."""
    if not variable or not distrito or not anio:
        return MESES_FALLBACK
    meses = sorted(
        mes
        for a, mes in _periodos_disponibles(dominio, variable, distrito)
        if a == int(anio)
    )
    return meses or MESES_FALLBACK
