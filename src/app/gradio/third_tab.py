import os

import folium
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

from src.data.access.queries import district_monthly_average

load_dotenv()

# Tramos de color por gas
GAS_TRAMOS = {
    "NO2":   [(0, 10, "green"), (10, 25, "yellow"), (25, 40, "orange"), (40, float("inf"), "red")],
    "PM10":  [(0, 15, "green"), (15, 25, "yellow"), (25, 40, "orange"), (40, float("inf"), "red")],
    "PM2.5": [(0, 5, "green"), (5, 10, "yellow"), (10, 25, "orange"), (25, float("inf"), "red")],
    "O3":    [(0, 60, "green"), (60, 90, "yellow"), (90, 120, "orange"), (120, float("inf"), "red")],
    "NOx":   [(0, 50, "green"), (50, 100, "yellow"), (100, 150, "orange"), (150, float("inf"), "red")]
}

DISTRITOS_PATH = os.getenv("DISTRITOS_PATH", "data/bronze/distritos/latest.parquet")
AIRQUALITY_PATH = os.getenv("DATA_AIRQUALITY_PATH", "data/processed/aire/*.parquet")
ESTACIONES_DISTRITO_PATH = os.getenv(
    "ESTACIONES_DISTRITO_PATH", "data/processed/estaciones_aire/latest.parquet"
)


def obtener_color(valor, gas):
    for min_val, max_val, color in GAS_TRAMOS.get(gas, []):
        if min_val <= valor < max_val:
            return color
    return "gray"


def generar_mapa_html(gas, anio, mes):
    distritos = gpd.read_parquet(DISTRITOS_PATH).to_crs("EPSG:4326")
    distritos["COD_DIS"] = distritos["COD_DIS"].astype(str).str.zfill(2)

    valores = district_monthly_average(
        AIRQUALITY_PATH, ESTACIONES_DISTRITO_PATH, gas, anio, mes
    )
    df_valores = pd.DataFrame(valores, columns=["COD_DIS", "valor_medio"])
    df_valores["COD_DIS"] = df_valores["COD_DIS"].astype(str).str.zfill(2)

    distritos = distritos.merge(df_valores, on="COD_DIS", how="left")

    m = folium.Map(location=[40.4168, -3.7038], zoom_start=11)

    for _, row in distritos.iterrows():
        color = obtener_color(row["valor_medio"], gas) if pd.notnull(row["valor_medio"]) else "lightgray"
        geojson = folium.GeoJson(
            data=row["geometry"].__geo_interface__,
            style_function=lambda feature, col=color: {
                "fillColor": col,
                "color": "black",
                "weight": 1,
                "fillOpacity": 0.6
            }
        )
        popup = f"<b>{row['NOMBRE']}</b><br>{gas} en {mes}/{anio}: {round(row['valor_medio'], 2) if pd.notnull(row['valor_medio']) else 'N/A'} µg/m³"
        geojson.add_child(folium.Popup(popup))
        geojson.add_to(m)

    return m._repr_html_()

def generar_leyenda_html(gas):
    tramos = GAS_TRAMOS.get(gas, [])
    html = "<div style='font-family:sans-serif; padding:10px;'>"
    html += f"<h4 style='margin-bottom:10px;'>Escala para {gas}</h4>"
    html += "<ul style='list-style:none;padding-left:0;'>"
    for min_val, max_val, color in tramos:
        if max_val == float("inf"):
            label = f"{min_val}+"
        else:
            label = f"{min_val} - {max_val}"
        html += "<li style='margin:4px 0; display:flex; align-items:center;'>"
        html += f"<span style='display:inline-block;width:20px;height:20px;background:{color};margin-right:10px;border:1px solid black;'></span>{label}</li>"
    html += "</ul></div>"
    return html
