"""Interactive dashboard: shared filters, KPIs, map, evolution, correlation."""

import matplotlib

matplotlib.use("Agg")  # headless: Gradio renders plots off the main thread, Tk isn't thread-safe

import matplotlib.pyplot as plt
import pandas as pd

from src.dashboard.components.filters import UNITS
from src.dashboard.components.kpi import kpi_conteos_texto, kpi_media_texto
from src.dashboard.components.map import generar_leyenda_html, generar_mapa_colores_html
from src.data.access.queries import (
    daily_average_air_by_district,
    daily_average_traffic_by_district,
)

AIRQUALITY_PATH = "data/silver/aire.parquet"
TRAFFIC_PATH = "data/silver/trafico.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire.parquet"
TRAFFIC_POINTS_PATH = "data/gold/dim_punto_trafico.parquet"


def _empty_fig(mensaje):
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.text(0.5, 0.5, mensaje, ha="center", va="center", transform=ax.transAxes)
    ax.axis("off")
    plt.close(fig)
    return fig


def graficar_evolucion(dominio, variable, distrito):
    """Daily series of `variable` for chosen district, all history."""
    if not distrito:
        return _empty_fig("Select a district to see the evolution")

    if dominio == "Aire":
        rows = daily_average_air_by_district(
            AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, variable, str(distrito)
        )
    else:
        rows = daily_average_traffic_by_district(
            TRAFFIC_PATH, TRAFFIC_POINTS_PATH, variable, str(distrito)
        )
    if not rows:
        return _empty_fig("No data for this combination")

    fechas = [r[0] for r in rows]
    medias = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(fechas, medias, linestyle="-", color="teal")
    ax.set_title(f"Daily evolution — {variable} — district {distrito}")
    ax.set_xlabel("Fecha")
    ax.set_ylabel(f"{variable} ({UNITS.get(variable, 'unitless')})")
    ax.grid(True)
    fig.autofmt_xdate(rotation=45)
    plt.close(fig)
    return fig


def graficar_correlacion(gas, variable_trafico, distrito):
    """Gas vs. traffic variable overlaid, same district, dual Y-axis."""
    if not distrito:
        return _empty_fig("Select a district to see the correlation")

    poll_rows = daily_average_air_by_district(
        AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, gas, str(distrito)
    )
    trafico_rows = daily_average_traffic_by_district(
        TRAFFIC_PATH, TRAFFIC_POINTS_PATH, variable_trafico, str(distrito)
    )
    if not poll_rows or not trafico_rows:
        return _empty_fig("Not enough data to cross air and traffic")

    df_poll = pd.DataFrame(poll_rows, columns=["fecha_dia", "media_gas"])
    df_trafico = pd.DataFrame(trafico_rows, columns=["fecha_dia", "media_trafico"])
    df_joined = df_poll.merge(df_trafico, on="fecha_dia", how="inner")
    if df_joined.empty:
        return _empty_fig("No common dates between air and traffic")

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
    ax1.set_ylabel(f"{gas} ({UNITS.get(gas, 'unitless')})", color="crimson")
    ax2.set_ylabel(
        f"{variable_trafico} ({UNITS.get(variable_trafico, 'unitless')})",
        color="royalblue",
    )
    ax1.tick_params(axis="y", labelcolor="crimson")
    ax2.tick_params(axis="y", labelcolor="royalblue")
    ax1.set_title(f"{gas} vs. {variable_trafico} — distrito {distrito}")
    ax1.grid(True)
    fig.autofmt_xdate(rotation=45)
    plt.close(fig)
    return fig


def refrescar(dominio, variable, distrito, anio, mes):
    """Single refresh point: legend, color map, KPIs and evolution line."""
    leyenda = generar_leyenda_html(variable)
    mapa_colores = generar_mapa_colores_html(dominio, variable, anio, mes)
    conteos = kpi_conteos_texto(distrito)
    media = kpi_media_texto(dominio, variable, distrito, anio, mes)
    evolucion = graficar_evolucion(dominio, variable, distrito)
    return leyenda, mapa_colores, conteos, media, evolucion
