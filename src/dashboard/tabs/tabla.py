"""Table tab: daily readings of a gas, filterable by station."""

import pandas as pd

from src.dashboard.components.filters import UNITS
from src.data.access.queries import (
    daily_values_by_station,
    get_magnitudes,
    get_stations,
)

AIRQUALITY_PATH = "data/silver/aire.parquet"

COLUMNAS = ["Fecha", "Valor"]


def obtener_estaciones_tabla():
    return get_stations(AIRQUALITY_PATH)


def obtener_magnitudes_tabla(estacion_id):
    """Only gases that station actually measures (already filtered in queries)."""
    return get_magnitudes(AIRQUALITY_PATH, estacion_id)


def unidad_texto(magnitud):
    if not magnitud:
        return ""
    return f"Unit: {UNITS.get(magnitud, 'unitless')}"


def tabla_diaria(estacion_id, magnitud):
    if not estacion_id or not magnitud:
        return pd.DataFrame(columns=COLUMNAS)
    rows = daily_values_by_station(AIRQUALITY_PATH, estacion_id, magnitud)
    return pd.DataFrame(rows, columns=COLUMNAS)
