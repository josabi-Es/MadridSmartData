"""Texto markdown de las tarjetas de indicadores del dashboard."""

from src.dashboard.components.map import valores_por_distrito
from src.data.access.queries import (
    count_stations_by_district,
    count_traffic_points_by_district,
)

TRAFFIC_POINTS_PATH = "data/bronze/trafico_puntos_medida/*.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire/latest.parquet"


def kpi_conteos_texto(distrito):
    if not distrito:
        return "Elige un distrito para ver el número de estaciones y puntos."
    # COD_DIS en silver/estaciones_aire va sin ceros a la izquierda ("1", no
    # "01") -- coincide con lo que ya devuelve obtener_distritos().
    n_estaciones = count_stations_by_district(ESTACIONES_DISTRITO_PATH, str(distrito))
    n_puntos = count_traffic_points_by_district(TRAFFIC_POINTS_PATH, str(distrito))
    return f"**Estaciones de aire:** {n_estaciones}  \n**Puntos de tráfico:** {n_puntos}"


def kpi_media_texto(dominio, variable, distrito, anio, mes):
    if not distrito:
        return "Elige un distrito para ver la media."
    valores = valores_por_distrito(dominio, variable, anio, mes)
    cod = str(distrito).zfill(2)
    fila = next((v for v in valores if str(v[0]).zfill(2) == cod), None)
    valor = round(fila[1], 2) if fila else None
    return f"**Media {variable} (distrito {distrito}):** {valor if valor is not None else 'N/A'}"
