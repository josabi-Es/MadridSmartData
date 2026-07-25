"""Pestaña Tabla: lecturas diarias de un gas, filtrable por estación."""

import pandas as pd

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
    return get_magnitudes(AIRQUALITY_PATH, estacion_id)


def tabla_diaria(estacion_id, magnitud):
    if not estacion_id or not magnitud:
        return pd.DataFrame(columns=COLUMNAS)
    rows = daily_values_by_station(AIRQUALITY_PATH, estacion_id, magnitud)
    return pd.DataFrame(rows, columns=COLUMNAS)
