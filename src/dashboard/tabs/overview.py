"""Dashboard interactivo: filtros compartidos, KPIs, mapa, evolución, correlación."""

import matplotlib.pyplot as plt
import pandas as pd

from src.dashboard.components.filters import UNITS
from src.dashboard.components.kpi import kpi_conteos_texto, kpi_media_texto
from src.dashboard.components.map import (
    generar_leyenda_html,
    generar_mapa_colores_html,
    generar_mapa_posiciones_html,
)
from src.data.access.queries import (
    daily_average_air_by_district,
    daily_average_traffic_by_district,
)

AIRQUALITY_PATH = "data/silver/aire/all.parquet"
TRAFFIC_PATH = "data/silver/trafico/all.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire/latest.parquet"
TRAFFIC_POINTS_PATH = "data/bronze/trafico_puntos_medida/*.parquet"

ANIOS = [2020, 2021, 2022, 2023, 2024]
MESES = list(range(1, 13))


def _empty_fig(mensaje):
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.text(0.5, 0.5, mensaje, ha="center", va="center", transform=ax.transAxes)
    ax.axis("off")
    return fig


def graficar_evolucion(dominio, variable, distrito):
    """Serie diaria de `variable` para el distrito elegido, todo el histórico."""
    if not distrito:
        return _empty_fig("Elige un distrito para ver la evolución")

    if dominio == "Aire":
        rows = daily_average_air_by_district(
            AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, variable, str(distrito)
        )
    else:
        rows = daily_average_traffic_by_district(
            TRAFFIC_PATH, TRAFFIC_POINTS_PATH, variable, str(distrito)
        )
    if not rows:
        return _empty_fig("Sin datos para esta combinación")

    fechas = [r[0] for r in rows]
    medias = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(fechas, medias, linestyle="-", color="teal")
    ax.set_title(f"Evolución diaria — {variable} — distrito {distrito}")
    ax.set_xlabel("Fecha")
    ax.set_ylabel(f"{variable} ({UNITS.get(variable, 'sin unidad')})")
    ax.grid(True)
    fig.autofmt_xdate(rotation=45)
    return fig


def graficar_correlacion(gas, variable_trafico, distrito):
    """Gas vs. variable de tráfico superpuestos, mismo distrito, doble eje Y."""
    if not distrito:
        return _empty_fig("Elige un distrito para ver la correlación")

    poll_rows = daily_average_air_by_district(
        AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, gas, str(distrito)
    )
    trafico_rows = daily_average_traffic_by_district(
        TRAFFIC_PATH, TRAFFIC_POINTS_PATH, variable_trafico, str(distrito)
    )
    if not poll_rows or not trafico_rows:
        return _empty_fig("Sin datos suficientes para cruzar aire y tráfico")

    df_poll = pd.DataFrame(poll_rows, columns=["fecha_dia", "media_gas"])
    df_trafico = pd.DataFrame(trafico_rows, columns=["fecha_dia", "media_trafico"])
    df_joined = df_poll.merge(df_trafico, on="fecha_dia", how="inner")
    if df_joined.empty:
        return _empty_fig("Sin fechas en común entre aire y tráfico")

    df_joined["fecha_dia"] = pd.to_datetime(df_joined["fecha_dia"])
    df_joined = df_joined.sort_values("fecha_dia")

    fig, ax1 = plt.subplots(figsize=(14, 5))
    ax2 = ax1.twinx()
    ax1.plot(df_joined["fecha_dia"], df_joined["media_gas"], color="crimson", label=gas)
    ax2.plot(
        df_joined["fecha_dia"],
        df_joined["media_trafico"],
        color="royalblue",
        label=variable_trafico,
    )
    ax1.set_xlabel("Fecha")
    ax1.set_ylabel(f"{gas} ({UNITS.get(gas, 'sin unidad')})", color="crimson")
    ax2.set_ylabel(
        f"{variable_trafico} ({UNITS.get(variable_trafico, 'sin unidad')})",
        color="royalblue",
    )
    ax1.tick_params(axis="y", labelcolor="crimson")
    ax2.tick_params(axis="y", labelcolor="royalblue")
    ax1.set_title(f"{gas} vs. {variable_trafico} — distrito {distrito}")
    ax1.grid(True)
    fig.autofmt_xdate(rotation=45)
    return fig


def refrescar(dominio, variable, distrito, anio, mes):
    """Un único punto de refresco: leyenda, 2 mapas, KPIs y línea de evolución."""
    leyenda = generar_leyenda_html(variable)
    mapa_posiciones = generar_mapa_posiciones_html(distrito=distrito)
    mapa_colores = generar_mapa_colores_html(dominio, variable, anio, mes)
    conteos = kpi_conteos_texto(distrito)
    media = kpi_media_texto(dominio, variable, distrito, anio, mes)
    evolucion = graficar_evolucion(dominio, variable, distrito)
    return leyenda, mapa_posiciones, mapa_colores, conteos, media, evolucion
