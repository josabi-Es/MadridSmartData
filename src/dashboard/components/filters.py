"""Opciones para los desplegables del dashboard -- sin lógica de negocio."""

from src.data.access.queries import (
    TRAFFIC_VARIABLES,
    get_air_variables_by_district,
    get_traffic_districts,
)

TRAFFIC_POINTS_PATH = "data/bronze/trafico_puntos_medida/*.parquet"
AIRQUALITY_PATH = "data/silver/aire/all.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire/latest.parquet"

AIR_VARIABLES = ["NO2", "PM10", "PM2.5", "O3", "NOx"]

UNITS = {
    "NO2": "µg/m³", "PM10": "µg/m³", "PM2.5": "µg/m³", "O3": "µg/m³", "NOx": "µg/m³",
    "intensidad": "veh/h", "ocupacion": "%", "carga": "%", "vmed": "km/h",
}  # fmt: skip


def obtener_distritos():
    """Los 21 distritos, vía los puntos de tráfico (los cubren todos)."""
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
