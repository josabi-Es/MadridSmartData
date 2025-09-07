from pyspark.sql.functions import col, trunc, avg
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()
DATA_TRAFFIC_PATH = os.getenv("DATA_TRAFFIC_PATH", "data/processed/traffic_processed")

def cargar_datos_trafico(spark):
    df_trafico = spark.read.parquet(f"{DATA_TRAFFIC_PATH}/*")
    return df_trafico

def graficar_serie_trafico(df, id_trafico, variable):
    try:
        id_trafico = int(id_trafico)
    except ValueError:
        raise ValueError("El ID debe ser un número entero válido")

    # Basic filtering
    df_filtrado = df.filter(
        (col("id") == id_trafico) &
        (col("error") == "N")
    )

    # Truncate date by month to reduce density (change to "day" for daily detail)
    df_mes = df_filtrado.withColumn("mes", trunc("fecha", "month"))

    # Monthly average of the variable
    df_agrupado = df_mes.groupBy("id", "mes").agg(avg(variable).alias("media"))

    # Convert to Pandas
    pdf = df_agrupado.orderBy("mes").toPandas()

    # Plot with matplotlib
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(pdf["mes"], pdf["media"], marker='o', linestyle='-', color='g')
    ax.set_title(f"Evolución mensual de {variable} - ID {id_trafico}")
    ax.set_xlabel("Mes")
    ax.set_ylabel(f"Valor medio de {variable}")
    ax.grid(True)
    fig.autofmt_xdate(rotation=45)

    return fig