import geopandas as gpd
import pandas as pd
import folium
import os
from shapely.geometry import Point
from pyspark.sql.functions import year, month, avg
from dotenv import load_dotenv
from first_tab import iniciar_spark, cargar_datos

load_dotenv()

# Tramos de color por gas
GAS_TRAMOS = {
    "NO2":   [(0, 10, "green"), (10, 25, "yellow"), (25, 40, "orange"), (40, float("inf"), "red")],
    "PM10":  [(0, 15, "green"), (15, 25, "yellow"), (25, 40, "orange"), (40, float("inf"), "red")],
    "PM2.5": [(0, 5, "green"), (5, 10, "yellow"), (10, 25, "orange"), (25, float("inf"), "red")],
    "O3":    [(0, 60, "green"), (60, 90, "yellow"), (90, 120, "orange"), (120, float("inf"), "red")],
    "NOx":   [(0, 50, "green"), (50, 100, "yellow"), (100, 150, "orange"), (150, float("inf"), "red")]
}

DISTRITOS_PATH = os.getenv("DISTRITOS_PATH", "data/processed/districts/Distritos_geo.json")
ESTACIONES_CSV = os.getenv("ESTACIONES_CSV", "data/processed/districts/estaciones_distrito.csv")

def generar_estaciones_distrito(df_cont):
    if os.path.exists(ESTACIONES_CSV):
        df = pd.read_csv(ESTACIONES_CSV, dtype={"COD_DIS": str})
        df["COD_DIS"] = df["COD_DIS"].str.zfill(2)
        return df

    print("📍 Generando 'estaciones_distrito.csv'...")

    estaciones_df = df_cont.select("estacion", "latitud", "longitud").distinct().toPandas()
    estaciones_df["geometry"] = estaciones_df.apply(lambda row: Point(row["longitud"], row["latitud"]), axis=1)
    estaciones_gdf = gpd.GeoDataFrame(estaciones_df, geometry="geometry", crs="EPSG:4326")

    distritos = gpd.read_file(DISTRITOS_PATH)
    if distritos.crs != estaciones_gdf.crs:
        distritos = distritos.to_crs(estaciones_gdf.crs)

    distritos["COD_DIS"] = distritos["COD_DIS"].astype(str).str.zfill(2)

    estaciones_con_distrito = gpd.sjoin(estaciones_gdf, distritos, how="left", predicate="within")
    resultado = estaciones_con_distrito[["estacion", "COD_DIS"]].drop_duplicates()
    resultado["COD_DIS"] = resultado["COD_DIS"].astype(str).str.zfill(2)

    resultado.to_csv(ESTACIONES_CSV, index=False)
    return resultado


def obtener_color(valor, gas):
    for min_val, max_val, color in GAS_TRAMOS.get(gas, []):
        if min_val <= valor < max_val:
            return color
    return "gray"


def calcular_media_por_distrito(df_cont, estaciones_distrito_df, gas, anio, mes):
    # Asegurar formato de COD_DIS
    estaciones_distrito_df["COD_DIS"] = estaciones_distrito_df["COD_DIS"].astype(str).str.zfill(2)
    estaciones_spark = df_cont.sql_ctx.createDataFrame(estaciones_distrito_df)

    # Primero añadimos las columnas "anio" y "mes"
    df_temp = (
        df_cont
        .withColumn("anio", year(df_cont.fecha))
        .withColumn("mes", month(df_cont.fecha))
    )

    # Luego filtramos usando esas columnas
    df_filtrado = (
        df_temp
        .filter((df_temp.magnitud == gas) & (df_temp.anio == anio) & (df_temp.mes == mes))
    )

    # Hacemos join con las estaciones con distrito
    df_con_distrito = df_filtrado.join(estaciones_spark, on="estacion", how="inner")

    # Calculamos la media por distrito
    df_resultado = (
        df_con_distrito
        .groupBy("COD_DIS")
        .agg(avg("dato").alias("valor_medio"))
        .toPandas()
    )

    # Normalizamos el código de distrito a dos dígitos
    df_resultado["COD_DIS"] = df_resultado["COD_DIS"].astype(str).str.zfill(2)
    return df_resultado


def generar_mapa_html(gas, anio, mes):
    distritos = gpd.read_file(DISTRITOS_PATH)
    distritos["COD_DIS"] = distritos["COD_DIS"].astype(str).str.zfill(2)
    
    spark= iniciar_spark()
    df_cont = cargar_datos(spark)

    estaciones_distrito_df = generar_estaciones_distrito(df_cont)
    df_valores = calcular_media_por_distrito(df_cont, estaciones_distrito_df, gas, anio, mes)

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
        html += f"<li style='margin:4px 0; display:flex; align-items:center;'>"
        html += f"<span style='display:inline-block;width:20px;height:20px;background:{color};margin-right:10px;border:1px solid black;'></span>{label}</li>"
    html += "</ul></div>"
    return html
