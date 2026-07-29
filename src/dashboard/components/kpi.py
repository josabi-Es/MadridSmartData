"""Markdown text for dashboard KPI cards."""

from src.dashboard.components.filters import UNITS
from src.dashboard.components.map import valores_por_distrito
from src.data.access.queries import (
    count_stations_by_district,
    count_traffic_points_by_district,
)

TRAFFIC_POINTS_PATH = "data/gold/dim_punto_trafico.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire.parquet"


def kpi_conteos_texto(distrito):
    if not distrito:
        return "Select a district to see the number of stations and points."
    # COD_DIS in silver/estaciones_aire has no leading zero ("1", not "01") --
    # matches what obtener_distritos() already returns.
    n_estaciones = count_stations_by_district(ESTACIONES_DISTRITO_PATH, str(distrito))
    n_puntos = count_traffic_points_by_district(TRAFFIC_POINTS_PATH, str(distrito))
    return f"**Air stations:** {n_estaciones}  \n**Traffic points:** {n_puntos}"


def kpi_media_texto(dominio, variable, distrito, anio, mes):
    if not distrito:
        return "Select a district to see the average."
    valores = valores_por_distrito(dominio, variable, anio, mes)
    cod = str(distrito).zfill(2)
    fila = next((v for v in valores if str(v[0]).zfill(2) == cod), None)
    valor = round(fila[1], 2) if fila else None
    unidad = UNITS.get(variable, "")
    texto_valor = f"{valor} {unidad}" if valor is not None else "N/A"
    return f"**{variable} mean (district {distrito}):** {texto_valor}"
