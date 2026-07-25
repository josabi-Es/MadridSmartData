"""Pestaña Tabla: lecturas diarias de un gas, filtrable por estación."""

import pandas as pd

from src.dashboard.components.filters import UNITS
from src.data.access.queries import (
    daily_values_by_station,
    get_magnitudes,
    get_stations,
)

AIRQUALITY_PATH = "data/silver/aire/*.parquet"

COLUMNAS = ["Fecha", "Valor"]


def obtener_estaciones_tabla():
    return get_stations(AIRQUALITY_PATH)


def obtener_magnitudes_tabla(estacion_id):
    """Solo los gases que esa estación mide de verdad (ya filtrado en queries)."""
    return get_magnitudes(AIRQUALITY_PATH, estacion_id)


def unidad_texto(magnitud):
    if not magnitud:
        return ""
    return f"Unidad: {UNITS.get(magnitud, 'sin unidad')}"


def tabla_diaria(estacion_id, magnitud):
    if not estacion_id or not magnitud:
        return pd.DataFrame(columns=COLUMNAS)
    rows = daily_values_by_station(AIRQUALITY_PATH, estacion_id, magnitud)
    return pd.DataFrame(rows, columns=COLUMNAS)
