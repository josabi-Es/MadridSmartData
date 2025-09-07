from pyspark.sql.functions import col, to_date, hour, avg
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, to_date, hour, avg
import pandas as pd
import matplotlib.pyplot as plt

def plot_tendencia_temporal(df_poll, df_trafico, estaciones_distrito_df, gas, variable_trafico, distrito):
    # --- Read CSV of stations with districts ---
    estaciones_distrito_df["COD_DIS"] = estaciones_distrito_df["COD_DIS"].astype(str).apply(lambda x: x.zfill(2))
    estaciones_spark = df_poll.sparkSession.createDataFrame(estaciones_distrito_df)

     # --- Pollution data ---
    df_poll = (
        df_poll
        .filter(col("validez") == "V")
        .join(estaciones_spark, on="estacion", how="inner")
        .filter((col("magnitud") == gas) & (col("COD_DIS") == distrito))
        .withColumn("fecha_dia", to_date("fecha"))
    )
    df_poll_agg = df_poll.groupBy("fecha_dia").agg(avg("dato").alias("media_gas"))

    # --- Traffic data ---
    df_trafico = (
        df_trafico
        .filter((col("error") == "N") & (col("distrito") == int(distrito)))
        .filter(hour("fecha") == 12)
        .withColumn("fecha_dia", to_date("fecha"))
    )
    df_trafico_agg = df_trafico.groupBy("fecha_dia").agg(avg(variable_trafico).alias("media_trafico"))

    # --- Join datasets ---
    df_joined = df_poll_agg.join(df_trafico_agg, on="fecha_dia", how="inner").toPandas()
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
