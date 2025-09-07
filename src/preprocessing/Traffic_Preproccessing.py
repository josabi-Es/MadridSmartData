from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, hour, to_date, month, dayofmonth, year, avg, minute, second
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
)
from pyspark.sql import Window
import logging
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Point

from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Paths from .env
TRAFFIC_RAW_PATH = os.getenv("TRAFFIC_RAW_PATH", "data/raw/traffic_data/")
POSITION_RAW_PATH = os.getenv("POSITION_RAW_PATH", "data/raw/traffic_sensors/")
TRAFFIC_PROCESSED_PATH = os.getenv("TRAFFIC_PROCESSED_PATH", "data/processed/traffic_data_processed/")



def schema_PosTraffic():
    return StructType([
        StructField("tipo_elem", StringType(), True),
        StructField( "distrito", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("cod_cent", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("utm_x", DoubleType(), True),
        StructField("utm_y", DoubleType(), True),
        StructField("longitud", DoubleType(), True),
        StructField("latitud", DoubleType(), True)
    ])


def schema_traffic():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("fecha", TimestampType(), True),
        StructField("tipo_elem", StringType(), True),
        StructField("intensidad", IntegerType(), True),
        StructField("ocupacion", IntegerType(), True),
        StructField("carga", IntegerType(), True),
        StructField("vmed", IntegerType(), True),
        StructField("error", StringType(), True),
        StructField("periodo_integracion", IntegerType(), True)
    ])


def cargar_datos(spark, paths_traffic, paths_position):
    df_traffic = spark.read.schema(schema_traffic()).parquet(*paths_traffic)
    df_position = spark.read.schema(schema_PosTraffic()).parquet(*paths_position)
    return df_traffic, df_position


def limpiar_error(df):
    df = df.filter(col("error").isNotNull())
    return df.withColumn("error", when(col("error") == "N", "N").otherwise(col("error")))


def eliminar_outliers(df):
    return df.filter(
        (col("intensidad") >= 0) & (col("intensidad") <= 2760) &
        (col("ocupacion") >= 0) & (col("ocupacion") <= 43) &
        (col("carga") >= 0) & (col("carga") <= 86) &
        (col("vmed") >= 0) & (col("vmed") <= 77) &
        (col("periodo_integracion") >= 1) & (col("periodo_integracion") <= 30)
    )


def eliminar_nulos_columnas_criticas(df):
    columnas_excluir = ["error"]
    columnas_objetivo = [f.name for f in df.schema.fields if f.name not in columnas_excluir]
    return df.dropna(subset=columnas_objetivo)


def filtrar_horas_en_punto(df):
    df = df.filter((minute(col("fecha")) == 0) & (second(col("fecha")) == 0))
    return df


def rellenar_vmed_nulos(df):
    df_con_fecha = df.withColumn("anio", year("fecha")) \
                     .withColumn("mes", month("fecha")) \
                     .withColumn("dia", dayofmonth("fecha")) \
                     .withColumn("hora", hour("fecha"))

    df_nulos = df_con_fecha.filter(col("vmed").isNull())
    df_validos = df_con_fecha.filter(col("vmed").isNotNull())

    window_spec = Window.partitionBy("id", "mes", "dia", "hora")
    df_validos_avg = df_validos.withColumn("vmed_avg", avg("vmed").over(window_spec))

    df_nulos_rellenados = df_nulos.join(
        df_validos_avg.select("id", "mes", "dia", "hora", "vmed_avg").distinct(),
        on=["id", "mes", "dia", "hora"],
        how="left"
    ).withColumn("vmed", col("vmed_avg")).drop("vmed_avg")

    df_final = df_validos.unionByName(df_nulos_rellenados)

    return df_final.drop("anio", "mes", "dia", "hora")


def seleccionar_columnas_posicion(df):
    df_limpio = df.select("id", "distrito", "latitud", "longitud") \
                  .filter(col("id").isNotNull()) \
                  .dropDuplicates(["id"])
    
    return df_limpio


def renombrar_y_join(df_traffic, df_position):
    df_pos_renamed = df_position.withColumnRenamed("id", "id_position")
    return df_traffic.join(
        df_pos_renamed,
        df_traffic.id == df_pos_renamed.id_position,
        how="inner"
    ).drop("id_position")

def eliminar_null_distritos(df):
    return df.filter(col("distrito").isNotNull())

def save_data(df, path, nombre_dataset, logger):
    df.write.mode("overwrite").parquet(path)
    logger.info(f"{nombre_dataset} guardado correctamente en {path}")


def pipeline_traffic_processing(spark, logger):
    
    YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
    paths_traffic = [f"{TRAFFIC_RAW_PATH}/{year}/*" for year in YEARS]
    paths_position = [f"{POSITION_RAW_PATH}/{year}/*" for year in YEARS]

    logger.info("Inicio del pipeline estructurado")

    start = datetime.now()
    print(f"[{start}] Cargando datos de tráfico y de posición...")
    df_traffic, df_position = cargar_datos(spark, paths_traffic, paths_position)
    count_traffic = df_traffic.count()
    count_position = df_position.count()
    end = datetime.now()
    print(f"[{end}] Carga completa. Tiempo: {(end - start).total_seconds()} s")
    print(f"Registros tráfico: {count_traffic}, posición: {count_position}")

    start = datetime.now()
    print(f"[{start}] Limpiando errores en columna 'error'...")
    df_traffic_clean = limpiar_error(df_traffic)
    count_clean = df_traffic_clean.count()
    end = datetime.now()
    print(f"[{end}] Limpieza error completa. Tiempo: {(end - start).total_seconds()} s")
    print(f"Registros tras limpiar errores: {count_clean}")

    start = datetime.now()
    print(f"[{start}] Eliminando outliers...")
    df_traffic_filtered = eliminar_outliers(df_traffic_clean)
    count_filtered = df_traffic_filtered.count()
    end = datetime.now()
    print(f"[{end}] Eliminación outliers completa. Tiempo: {(end - start).total_seconds()} s")
    print(f"Registros tras eliminar outliers: {count_filtered}")

    start = datetime.now()
    print(f"[{start}] Eliminando valores nulos en columnas críticas...")
    df_traffic_nonull = eliminar_nulos_columnas_criticas(df_traffic_filtered)
    count_nonull = df_traffic_nonull.count()
    end = datetime.now()
    print(f"[{end}] Eliminación nulos completa. Tiempo: {(end - start).total_seconds()} s")
    print(f"Registros tras eliminar nulos: {count_nonull}")

    start = datetime.now()
    print(f"[{start}] Filtrando registros a horas en punto...")
    df_traffic_hourly = filtrar_horas_en_punto(df_traffic_nonull)
    count_hourly = df_traffic_hourly.count()
    end = datetime.now()
    print(f"[{end}] Filtrado horas en punto completo. Tiempo: {(end - start).total_seconds()} s")
    print(f"Registros a horas exactas: {count_hourly}")

    start = datetime.now()
    print(f"[{start}] Rellenando 'vmed' nulos con promedios...")
    df_traffic_relleno = rellenar_vmed_nulos(df_traffic_hourly)
    count_relleno = df_traffic_relleno.count()
    end = datetime.now()
    print(f"[{end}] Relleno de vmed completo. Tiempo: {(end - start).total_seconds()} s")
    print(f"Registros tras rellenar 'vmed': {count_relleno}")

    start = datetime.now()
    print(f"[{start}] Seleccionando columnas limpias de posición...")
    df_position_clean = seleccionar_columnas_posicion(df_position)
    count_pos_clean = df_position_clean.count()
    end = datetime.now()
    print(f"[{end}] Selección de posición completa. Tiempo: {(end - start).total_seconds()} s")
    print(f"Registros únicos de posición: {count_pos_clean}")

    start = datetime.now()
    print(f"[{start}] Realizando join entre tráfico y posición...")
    df_final = renombrar_y_join(df_traffic_relleno, df_position_clean)
    count_final = df_final.count()
    end = datetime.now()
    print(f"[{end}] Join completo. Tiempo: {(end - start).total_seconds()} s")
    print(f"Registros tras join final: {count_final}")
    
    start = datetime.now()
    print(f"[{start}] Dando valor a los distritos nulls...")
    df_final= eliminar_null_distritos(df_final)
    end = datetime.now()

    start = datetime.now()
    print(f"[{start}] Guardando en HDFS...")
    save_data(df_final, "hdfs://localhost:9000/dataTraffic", "Dataset final de tráfico", logger)
    end = datetime.now()
    print(f"[{end}] Guardado completo. Tiempo: {(end - start).total_seconds()} s")

    print(f"[{datetime.now()}] Pipeline finalizado correctamente.")
    return df_final


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("PipelineTraffic")

    spark = SparkSession.builder \
        .appName("PreprocessTraffic") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

    try:
        logger.info("Iniciando pipeline de tráfico")
        pipeline_traffic_processing(spark, logger)
        logger.info("Pipeline finalizado con éxito")
    except Exception as e:
        logger.error(f"Error durante el pipeline: {e}")
    finally:
        spark.stop()
        logger.info("SparkSession cerrada")
