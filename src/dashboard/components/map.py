"""Choropleth + individual markers, color scales for both domains (air/traffic)."""

import folium
import geopandas as gpd
import pandas as pd

from src.data.access.queries import (
    district_monthly_average,
    district_monthly_traffic_average,
    estaciones_aire_coords,
    traffic_points_by_district,
)

DISTRITOS_PATH = "data/bronze/distritos/latest.parquet"
AIRQUALITY_PATH = "data/silver/aire.parquet"
TRAFFIC_PATH = "data/silver/trafico.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire.parquet"
TRAFFIC_POINTS_PATH = "data/gold/dim_punto_trafico.parquet"
GOLD_DIM_DISTRITO_PATH = "data/gold/dim_distrito.parquet"

# Color bands by variable -- air quality in µg/m³, traffic in its own units.
#
# GAS_TRAMOS (NO2/PM10/PM2.5/O3): Official Air Quality Index (MITECO, Spain/EU),
# collapsing 6 categories into 4 dashboard colors (Good+Fairly good -> green,
# Fair -> yellow, Poor -> orange, Very poor+Extremely poor -> red). External
# source beyond the 4 project PDFs -- none of those define color thresholds
# (verified: only describe file format, station/gas codes, and point locations).
# Official index uses hourly reference periods (NO2) or max 8h (O3), while the
# dashboard plots daily/monthly averages -- taken as approximate visual scale,
# not literal regulatory classification. NOx has no public hourly index (only
# annual vegetation protection limit), so left with previous heuristic band.
GAS_TRAMOS = {
    "NO2": [(0, 90, "green"), (90, 120, "yellow"), (120, 230, "orange"), (230, float("inf"), "red")],
    "PM10": [(0, 40, "green"), (40, 50, "yellow"), (50, 100, "orange"), (100, float("inf"), "red")],
    "PM2.5": [(0, 20, "green"), (20, 25, "yellow"), (25, 50, "orange"), (50, float("inf"), "red")],
    "O3": [(0, 100, "green"), (100, 130, "yellow"), (130, 240, "orange"), (240, float("inf"), "red")],
    "NOx": [(0, 50, "green"), (50, 100, "yellow"), (100, 150, "orange"), (150, float("inf"), "red")],
}  # fmt: skip

# Official Madrid traffic PDF (Trafico_Estructura_DS_Contenido_Trafico_Historico.pdf)
# explicitly states "carga" is published as raw value without justifying coefficients --
# no official color scale for traffic exists anywhere. Bands below are heuristic
# (were before this revision), unchanged values.
TRAFFIC_TRAMOS = {
    "CARGA": [(0, 25, "green"), (25, 50, "yellow"), (50, 75, "orange"), (75, float("inf"), "red")],
    "OCUPACION": [(0, 20, "green"), (20, 40, "yellow"), (40, 60, "orange"), (60, float("inf"), "red")],
    "INTENSIDAD": [(0, 500, "green"), (500, 1000, "yellow"), (1000, 2000, "orange"), (2000, float("inf"), "red")],
    # VMED reversed: low speed = congestion
    "VMED": [(0, 20, "red"), (20, 40, "orange"), (40, 60, "yellow"), (60, float("inf"), "green")],
}  # fmt: skip

VARIABLE_TRAMOS = {**GAS_TRAMOS, **TRAFFIC_TRAMOS}


def obtener_color(valor, variable):
    for min_val, max_val, color in VARIABLE_TRAMOS.get(variable, []):
        if min_val <= valor < max_val:
            return color
    return "gray"


def generar_leyenda_html(variable):
    tramos = VARIABLE_TRAMOS.get(variable, [])
    html = "<div style='font-family:sans-serif; padding:10px;'>"
    html += f"<h4 style='margin-bottom:10px;'>Escala para {variable}</h4>"
    html += "<ul style='list-style:none;padding-left:0;'>"
    for min_val, max_val, color in tramos:
        label = f"{min_val}+" if max_val == float("inf") else f"{min_val} - {max_val}"
        html += "<li style='margin:4px 0; display:flex; align-items:center;'>"
        html += (
            f"<span style='display:inline-block;width:20px;height:20px;"
            f"background:{color};margin-right:10px;"
            f"border:1px solid black;'></span>{label}</li>"
        )
    html += "</ul></div>"
    return html


def valores_por_distrito(dominio, variable, anio, mes):
    """Mean of `variable` by district for given month/year."""
    if dominio == "Aire":
        return district_monthly_average(
            AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, variable, anio, mes
        )
    return district_monthly_traffic_average(
        TRAFFIC_PATH, TRAFFIC_POINTS_PATH, variable, anio, mes
    )


def _cargar_distritos():
    distritos = gpd.read_parquet(DISTRITOS_PATH).to_crs("EPSG:4326")
    distritos["COD_DIS"] = distritos["COD_DIS"].astype(str).str.zfill(2)
    return distritos


def generar_mapa_posiciones_html(distrito=None):
    """Position map: where stations/points are located, uncolored by value.

    24 air stations always visible (few of them). ~4,962 traffic points
    drawn only after choosing a district -- never all at once.
    District boundaries drawn in gray, reference only.
    """
    m = folium.Map(location=[40.4168, -3.7038], zoom_start=11)

    for _, row in _cargar_distritos().iterrows():
        geojson = folium.GeoJson(
            data=row["geometry"].__geo_interface__,
            style_function=lambda feature: {
                "fillColor": "white",
                "color": "gray",
                "weight": 1,
                "fillOpacity": 0.05,
            },
        )
        geojson.add_child(folium.Popup(f"<b>{row['NOMBRE']}</b>"))
        geojson.add_to(m)

    for id_aire, estacion, longitud, latitud, cod_dis in estaciones_aire_coords(
        ESTACIONES_DISTRITO_PATH
    ):
        folium.CircleMarker(
            location=[latitud, longitud],
            radius=5,
            color="blue",
            fill=True,
            fill_color="blue",
            popup=f"Air station {id_aire}: {estacion} (district {cod_dis})",
        ).add_to(m)

    if distrito:
        puntos = traffic_points_by_district(TRAFFIC_POINTS_PATH, str(distrito))
        for id_, nombre, longitud, latitud in puntos:
            folium.CircleMarker(
                location=[latitud, longitud],
                radius=3,
                color="orange",
                fill=True,
                fill_color="orange",
                popup=f"Traffic point {id_}: {nombre}",
            ).add_to(m)

    return m._repr_html_()


def generar_mapa_colores_html(dominio, variable, anio, mes):
    """Choropleth: each district colored by its mean `variable`."""
    distritos = _cargar_distritos()

    valores = valores_por_distrito(dominio, variable, anio, mes)
    df_valores = pd.DataFrame(valores, columns=["COD_DIS", "valor_medio"])
    df_valores["COD_DIS"] = df_valores["COD_DIS"].astype(str).str.zfill(2)

    distritos = distritos.merge(df_valores, on="COD_DIS", how="left")

    m = folium.Map(location=[40.4168, -3.7038], zoom_start=11)

    for _, row in distritos.iterrows():
        color = (
            obtener_color(row["valor_medio"], variable)
            if pd.notnull(row["valor_medio"])
            else "lightgray"
        )
        geojson = folium.GeoJson(
            data=row["geometry"].__geo_interface__,
            style_function=lambda feature, col=color: {
                "fillColor": col,
                "color": "black",
                "weight": 1,
                "fillOpacity": 0.5,
            },
        )
        valor_txt = (
            round(row["valor_medio"], 2) if pd.notnull(row["valor_medio"]) else "N/A"
        )
        popup = f"<b>{row['NOMBRE']}</b><br>{variable} en {mes}/{anio}: {valor_txt}"
        geojson.add_child(folium.Popup(popup))
        geojson.add_to(m)

        # Añadir número de distrito en el centroide
        centroide = row["geometry"].centroid
        folium.Marker(
            location=[centroide.y, centroide.x],
            icon=folium.DivIcon(
                html=f"<div style='font-weight:bold; font-size:22px; color:white; text-shadow:1px 1px 2px black; text-align:center;'>{row['COD_DIS']}</div>"
            ),
        ).add_to(m)

    return m._repr_html_()


def generar_mapa_cobertura_html():
    """Choropleth for Summary tab: green/red by `COBERTURA_AIRE` from
    `gold/dim_distrito.parquet` -- catalog only, no measurement data.
    """
    distritos = gpd.read_parquet(GOLD_DIM_DISTRITO_PATH).to_crs("EPSG:4326")

    m = folium.Map(location=[40.4168, -3.7038], zoom_start=11)

    for _, row in distritos.iterrows():
        color = "green" if row["COBERTURA_AIRE"] else "red"
        geojson = folium.GeoJson(
            data=row["GEOMETRY"].__geo_interface__,
            style_function=lambda feature, col=color: {
                "fillColor": col,
                "color": "black",
                "weight": 1,
                "fillOpacity": 0.5,
            },
        )
        popup = (
            f"<b>{row['NOMBRE']}</b><br>"
            f"Air stations: {row['N_ESTACIONES_AIRE']}<br>"
            f"Traffic points: {row['N_PUNTOS_TRAFICO']}"
        )
        geojson.add_child(folium.Popup(popup))
        geojson.add_to(m)

    return m._repr_html_()
