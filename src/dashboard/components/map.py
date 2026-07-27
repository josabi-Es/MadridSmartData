"""Choropleth + individual markers, color scales for both domains (aire/tráfico)."""

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
AIRQUALITY_PATH = "data/silver/aire/all.parquet"
TRAFFIC_PATH = "data/silver/trafico/all.parquet"
ESTACIONES_DISTRITO_PATH = "data/silver/estaciones_aire/latest.parquet"
TRAFFIC_POINTS_PATH = "data/bronze/trafico_puntos_medida/*.parquet"
GOLD_DIM_DISTRITO_PATH = "data/gold/dim_distrito.parquet"

# Tramos de color por variable -- aire en µg/m³, tráfico en sus propias unidades.
#
# GAS_TRAMOS (NO2/PM10/PM2.5/O3): Índice de Calidad del Aire oficial de
# España/UE (MITECO), colapsando sus 6 categorías a los 4 colores que usa
# el dashboard (Buena+Razonablemente buena -> verde, Regular -> amarillo,
# Desfavorable -> naranja, Muy desfavorable+Extremadamente desfavorable ->
# rojo). Fuente externa a los 4 PDF de estructura de datos del proyecto --
# ninguno de esos PDF define umbrales de color (verificado: solo describen
# formato de fichero, códigos de estación/magnitud y ubicación de puntos).
# El índice oficial usa periodos de referencia horario (NO2) o máx. 8h
# (O3), mientras el dashboard pinta medias diarias/mensuales -- se toma
# como escala visual aproximada, no como clasificación regulatoria literal.
# NOx no tiene índice horario público (solo un límite anual de protección
# a la vegetación), así que se deja con el tramo heurístico previo.
GAS_TRAMOS = {
    "NO2": [(0, 90, "green"), (90, 120, "yellow"), (120, 230, "orange"), (230, float("inf"), "red")],
    "PM10": [(0, 40, "green"), (40, 50, "yellow"), (50, 100, "orange"), (100, float("inf"), "red")],
    "PM2.5": [(0, 20, "green"), (20, 25, "yellow"), (25, 50, "orange"), (50, float("inf"), "red")],
    "O3": [(0, 100, "green"), (100, 130, "yellow"), (130, 240, "orange"), (240, float("inf"), "red")],
    "NOx": [(0, 50, "green"), (50, 100, "yellow"), (100, 150, "orange"), (150, float("inf"), "red")],
}  # fmt: skip

# El PDF oficial de tráfico de Madrid (Trafico_Estructura_DS_Contenido_
# Trafico_Historico.pdf) admite explícitamente que "carga" se publica como
# valor bruto sin justificar sus coeficientes -- no existe una escala de
# color oficial para tráfico en ningún sitio. Los tramos de abajo son
# heurísticos (ya lo eran antes de esta revisión), sin cambios de valores.
TRAFFIC_TRAMOS = {
    "carga": [(0, 25, "green"), (25, 50, "yellow"), (50, 75, "orange"), (75, float("inf"), "red")],
    "ocupacion": [(0, 20, "green"), (20, 40, "yellow"), (40, 60, "orange"), (60, float("inf"), "red")],
    "intensidad": [(0, 500, "green"), (500, 1000, "yellow"), (1000, 2000, "orange"), (2000, float("inf"), "red")],
    # vmed se lee al revés: velocidad baja = congestión
    "vmed": [(0, 20, "red"), (20, 40, "orange"), (40, 60, "yellow"), (60, float("inf"), "green")],
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
    """Media de `variable` por distrito, para el mes/año dados."""
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
    """Mapa de posiciones: dónde están las estaciones/puntos, sin colorear por valor.

    Las 24 estaciones de aire se ven siempre (son pocas). Los ~4.962 puntos
    de tráfico solo se dibujan tras elegir un distrito -- nunca de golpe.
    Los límites de distrito se dibujan en gris, solo de referencia.
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

    for codigo, estacion, longitud, latitud, cod_dis in estaciones_aire_coords(
        ESTACIONES_DISTRITO_PATH
    ):
        folium.CircleMarker(
            location=[latitud, longitud],
            radius=5,
            color="blue",
            fill=True,
            fill_color="blue",
            popup=f"Estación aire {codigo}: {estacion} (distrito {cod_dis})",
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
                popup=f"Punto tráfico {id_}: {nombre}",
            ).add_to(m)

    return m._repr_html_()


def generar_mapa_colores_html(dominio, variable, anio, mes):
    """Choropleth: cada distrito coloreado según su media de `variable`."""
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

    return m._repr_html_()


def generar_mapa_cobertura_html():
    """Choropleth de la pestaña Resumen: verde/rojo según `cobertura_aire`
    de `gold/dim_distrito.parquet` -- ningún dato de medición, solo catálogo.
    """
    distritos = gpd.read_parquet(GOLD_DIM_DISTRITO_PATH).to_crs("EPSG:4326")

    m = folium.Map(location=[40.4168, -3.7038], zoom_start=11)

    for _, row in distritos.iterrows():
        color = "green" if row["cobertura_aire"] else "red"
        geojson = folium.GeoJson(
            data=row["geometry"].__geo_interface__,
            style_function=lambda feature, col=color: {
                "fillColor": col,
                "color": "black",
                "weight": 1,
                "fillOpacity": 0.5,
            },
        )
        popup = (
            f"<b>{row['NOMBRE']}</b><br>"
            f"Estaciones aire: {row['n_estaciones_aire']}<br>"
            f"Puntos tráfico: {row['n_puntos_trafico']}"
        )
        geojson.add_child(folium.Popup(popup))
        geojson.add_to(m)

    return m._repr_html_()
