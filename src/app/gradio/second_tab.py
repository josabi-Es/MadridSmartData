import os

import matplotlib.pyplot as plt
from dotenv import load_dotenv

from src.data.access.queries import monthly_average_traffic

load_dotenv()
DATA_TRAFFIC_PATH = os.getenv("DATA_TRAFFIC_PATH", "data/processed/trafico/*.parquet")


def graficar_serie_trafico(id_trafico, variable):
    try:
        int(id_trafico)
    except ValueError:
        raise ValueError("El ID debe ser un número entero válido") from None

    rows = monthly_average_traffic(DATA_TRAFFIC_PATH, id_trafico, variable)
    meses = [r[0] for r in rows]
    medias = [r[1] for r in rows]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(meses, medias, marker="o", linestyle="-", color="g")
    ax.set_title(f"Evolución mensual de {variable} - ID {id_trafico}")
    ax.set_xlabel("Mes")
    ax.set_ylabel(f"Valor medio de {variable}")
    ax.grid(True)
    fig.autofmt_xdate(rotation=45)

    return fig
