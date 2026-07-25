import os

import matplotlib.pyplot as plt
from dotenv import load_dotenv

from src.data.access.queries import get_magnitudes, get_stations, monthly_average

load_dotenv()

DATA_AIRQUALITY_PATH = os.getenv(
    "DATA_AIRQUALITY_PATH", "data/processed/aire/*.parquet"
)


def obtener_estaciones():
    return get_stations(DATA_AIRQUALITY_PATH)


def obtener_magnitudes(estacion_id=None):
    return get_magnitudes(DATA_AIRQUALITY_PATH, estacion_id)


def graficar_serie_temporal(estacion_id, magnitud):
    rows = monthly_average(DATA_AIRQUALITY_PATH, estacion_id, magnitud)
    meses = [r[0] for r in rows]
    medias = [r[1] for r in rows]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(meses, medias, marker="o", linestyle="-", color="b")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Valor medio")
    ax.set_title(f"Evolución mensual - Estación {estacion_id} - {magnitud}")
    ax.grid(True)
    fig.autofmt_xdate(rotation=45)

    return fig
