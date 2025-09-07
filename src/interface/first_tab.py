from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, trunc
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

load_dotenv()

DATA_AIRQUALITY_PATH = os.getenv("DATA_AIRQUALITY_PATH", "data/processed/air_quality_processed")



def iniciar_spark():
    return SparkSession.builder \
        .appName("TFM Visualizador") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

def cargar_datos(spark):
    df_cont = spark.read.parquet(f"{DATA_AIRQUALITY_PATH}/*")
    return df_cont

def obtener_estaciones(df_cont):
    estaciones = df_cont.select("estacion").distinct().orderBy("estacion")
    return [str(row["estacion"]) for row in estaciones.collect()]

def obtener_magnitudes(df_cont, estacion_id=None):
    if estacion_id:
        magnitudes = df_cont.filter(col("estacion") == int(estacion_id)) \
                            .select("magnitud").distinct().orderBy("magnitud")
    else:
        magnitudes = df_cont.select("magnitud").distinct().orderBy("magnitud")
    return [row["magnitud"] for row in magnitudes.collect()]

def graficar_serie_temporal(df, estacion_id, magnitud):
    df_filtrado = df.filter(
        (col("estacion") == int(estacion_id)) &
        (col("magnitud") == magnitud) &
        (col("validez") == "V")
    )

    # Grouping by month (truncate the date to the month)
    df_mes = df_filtrado.withColumn("mes", trunc("fecha", "month"))

    # Group by station, month, and magnitude
    df_agrupado = df_mes.groupBy("estacion", "mes", "magnitud").agg(avg("dato").alias("media"))

    # Convert to Pandas
    pdf = df_agrupado.orderBy("mes").toPandas()

    # Plot
    fig, ax = plt.subplots(figsize=(14, 6))  
    ax.plot(pdf["mes"], pdf["media"], marker='o', linestyle='-', color='b')
    ax.set_xlabel("Mes")
    ax.set_ylabel("Valor medio")
    ax.set_title(f"Evolución mensual - Estación {estacion_id} - {magnitud}")
    ax.grid(True)
    fig.autofmt_xdate(rotation=45)

    return fig
