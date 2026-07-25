import os

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

from src.data.access.queries import (
    daily_average_air_by_district,
    daily_average_traffic_by_district,
)

load_dotenv()

AIRQUALITY_PATH = os.getenv("DATA_AIRQUALITY_PATH", "data/processed/aire/*.parquet")
TRAFFIC_PATH = os.getenv("DATA_TRAFFIC_PATH", "data/processed/trafico/*.parquet")
ESTACIONES_DISTRITO_PATH = os.getenv(
    "ESTACIONES_DISTRITO_PATH", "data/processed/estaciones_aire/latest.parquet"
)
TRAFFIC_POINTS_PATH = os.getenv(
    "TRAFFIC_POINTS_PATH", "data/bronze/trafico_puntos_medida/*.parquet"
)


def plot_tendencia_temporal(gas, variable_trafico, distrito):
    poll_rows = daily_average_air_by_district(
        AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, gas, distrito
    )
    df_poll = pd.DataFrame(poll_rows, columns=["fecha_dia", "media_gas"])

    trafico_rows = daily_average_traffic_by_district(
        TRAFFIC_PATH, TRAFFIC_POINTS_PATH, variable_trafico, distrito
    )
    df_trafico = pd.DataFrame(trafico_rows, columns=["fecha_dia", "media_trafico"])

    df_joined = df_poll.merge(df_trafico, on="fecha_dia", how="inner")
    df_joined["fecha_dia"] = pd.to_datetime(df_joined["fecha_dia"])
    df_joined = df_joined.sort_values("fecha_dia")

    # --- Common range: according to the shorter dataset (traffic) ---
    min_fecha = pd.to_datetime("2019-01-01")
    max_fecha = pd.to_datetime("2025-03-31")
    df_joined = df_joined[(df_joined["fecha_dia"] >= min_fecha) & (df_joined["fecha_dia"] <= max_fecha)]

    # --- Plot ---
    fig, ax1 = plt.subplots(figsize=(15, 6))
    ax2 = ax1.twinx()

    ax1.plot(df_joined["fecha_dia"], df_joined["media_gas"], color="crimson", label=gas, linewidth=1.5)
    ax2.plot(df_joined["fecha_dia"], df_joined["media_trafico"], color="royalblue", label=variable_trafico, linewidth=1.5)

    ax1.set_xlabel("Fecha")
    ax1.set_ylabel(f"{gas} (µg/m³)", color="crimson")
    ax2.set_ylabel(f"{variable_trafico}", color="royalblue")

    ax1.tick_params(axis="y", labelcolor="crimson")
    ax2.tick_params(axis="y", labelcolor="royalblue")

    ax1.set_title(f"Evolución diaria de {gas} y {variable_trafico} en distrito {distrito}\n(2019-01-01 a 2025-03-31)")
    ax1.grid(True)
    fig.autofmt_xdate(rotation=45)

    return fig
