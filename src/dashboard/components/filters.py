"""Opciones para los desplegables del dashboard -- sin lógica de negocio."""

from src.data.access.queries import TRAFFIC_VARIABLES, get_traffic_districts

TRAFFIC_POINTS_PATH = "data/bronze/trafico_puntos_medida/*.parquet"

AIR_VARIABLES = ["NO2", "PM10", "PM2.5", "O3", "NOx"]


def obtener_distritos():
    """Los 21 distritos, vía los puntos de tráfico (los cubren todos)."""
    return get_traffic_districts(TRAFFIC_POINTS_PATH)


def obtener_variables(dominio):
    return AIR_VARIABLES if dominio == "Aire" else sorted(TRAFFIC_VARIABLES)
