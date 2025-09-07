import logging
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, lpad, to_date, concat_ws, when, isnan,
    avg, median, first, expr
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DoubleType
from pyspark.sql import Window


# --- Configuración de paths (ajusta según tu entorno) ---
from dotenv import load_dotenv
import os

load_dotenv()

# Leer ruta de raw data desde .env
RAW_CALIDAD_AIRE_PATH = os.getenv("RAW_CALIDAD_AIRE_PATH", "data/raw/air_quality_data/")
STATIONS_RAW_PATH = os.getenv("STATIONS_RAW_PATH", "data/raw/air_quality_sensors/")
POLLUTION_PROCESSED_PATH = os.getenv("POLLUTION_PROCESSED_PATH", "data/processed/air_quality_processed/")

# Generar lista de paths de todos los años (suponiendo carpetas 2019, 2020...)
YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
paths_calidad_aire = [f"{RAW_CALIDAD_AIRE_PATH.rstrip('/')}/{year}/*" for year in YEARS]


# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Funciones auxiliares ---

def schema_Estaciones():
    return StructType([
        StructField("codigo", IntegerType(), True),
        StructField("codigo_corto", IntegerType(), True),
        StructField("estacion", StringType(), True),
        StructField("direccion", StringType(), True),
        StructField("longitud_etrs89", StringType(), True),
        StructField("latitud_etrs89", StringType(), True),
        StructField("altitud", IntegerType(), True),
        StructField("cod_tipo", StringType(), True),
        StructField("nom_tipo", StringType(), True),
        StructField("no2", StringType(), True),
        StructField("so2", StringType(), True),
        StructField("co", StringType(), True),
        StructField("pm10", StringType(), True),
        StructField("pm2_5", StringType(), True),
        StructField("o3", StringType(), True),
        StructField("btx", StringType(), True),
        StructField("cod_via", IntegerType(), True),
        StructField("via_clase", StringType(), True),
        StructField("via_par", StringType(), True),
        StructField("via_nombre", StringType(), True),
        StructField("fecha_alta", StringType(), True),
        StructField("coordenada_x_etrs89", StringType(), True),
        StructField("coordenada_y_etrs89", StringType(), True),
        StructField("longitud", DoubleType(), True),
        StructField("latitud", DoubleType(), True),
    ])

def schema_CalidadAireMes():
    fields = [
        StructField("provincia", IntegerType(), True),
        StructField("municipio", IntegerType(), True),
        StructField("estacion", IntegerType(), True),
        StructField("magnitud", IntegerType(), True),
        StructField("punto_muestreo", StringType(), True),
        StructField("ano", IntegerType(), True),
        StructField("mes", IntegerType(), True),
    ]

    for i in range(1, 32):
        fields.append(StructField(f"D{str(i).zfill(2)}", IntegerType(), True))
        fields.append(StructField(f"V{str(i).zfill(2)}", StringType(), True))

    return StructType(fields)


def save_data(df, path, name="dataset"):
    logger.info(f"Guardando dataset {name} en {path}")
    df.write.mode("overwrite").parquet(path)


def eliminar_columnas_irrelevantes(df):
    cols_to_drop = ["provincia", "municipio", "punto_muestreo"]
    cols_actuales = {c.lower(): c for c in df.columns}

    for c in cols_to_drop:
        if c in cols_actuales:
            df = df.drop(cols_actuales[c])
    return df

def seleccionar_posicion(df_pos):
    cols = ["codigo_corto", "latitud", "longitud"]
    cols_actuales = {c.lower(): c for c in df_pos.columns}

    cols_seleccionadas = [cols_actuales[c] for c in cols if c in cols_actuales]
    return df_pos.select(*cols_seleccionadas)

def unpivot_diarios(df):
    dias = [str(d).zfill(2) for d in range(1, 32)]
    df = df.toDF(*[c.lower() for c in df.columns])  # Convertir columnas a minúsculas
    dfs = []
    for d in dias:
        d_col = f"d{d}"  # columnas en minúscula
        v_col = f"v{d}"
        if d_col in df.columns and v_col in df.columns:
            df_d = df.select(
                "estacion",
                "magnitud",
                "ano",
                "mes",
                lit(int(d)).alias("dia"),
                col(d_col).alias("dato"),
                col(v_col).alias("validez")
            )
            dfs.append(df_d)
    return reduce(lambda dfa, dfb: dfa.unionByName(dfb), dfs)

def filtrar_fechas_invalidas_debug(df, logger):
    df = df.toDF(*[c.lower() for c in df.columns])

    df = df.withColumn("ano_int", col("ano").cast("int"))
    df = df.withColumn("mes_int", col("mes").cast("int"))
    df = df.withColumn("dia_int", col("dia").cast("int"))

    es_bisiesto = (
        ((col("ano_int") % 400) == 0) |
        (((col("ano_int") % 4) == 0) & ((col("ano_int") % 100) != 0))
    )

    cond_fecha_invalida = (
        (col("ano_int").isNull()) | (col("mes_int").isNull()) | (col("dia_int").isNull()) |  # nulos
        (col("ano_int") < 2019) | (col("ano_int") > 2025) |  
        (col("mes_int") < 1) | (col("mes_int") > 12) |  
        (col("dia_int") < 1) | (col("dia_int") > 31) |  
        (((col("mes_int").isin([4, 6, 9, 11])) & (col("dia_int") > 30))) |  
        ((col("mes_int") == 2) & (
            (~es_bisiesto & (col("dia_int") > 28)) |  
            (es_bisiesto & (col("dia_int") > 29))
        ))
    )

    # Logs para depurar
    logger.info(f"Filas totales: {df.count()}")
    logger.info(f"Filas con ano/mes/dia nulos: {df.filter(col('ano_int').isNull() | col('mes_int').isNull() | col('dia_int').isNull()).count()}")
    logger.info(f"Filas fuera rango años (2019-2025): {df.filter((col('ano_int') < 2019) | (col('ano_int') > 2025)).count()}")
    logger.info(f"Filas fuera rango meses (1-12): {df.filter((col('mes_int') < 1) | (col('mes_int') > 12)).count()}")
    logger.info(f"Filas fuera rango días (1-31): {df.filter((col('dia_int') < 1) | (col('dia_int') > 31)).count()}")
    logger.info(f"Filas con día inválido en meses con 30 días: {df.filter((col('mes_int').isin([4,6,9,11])) & (col('dia_int') > 30)).count()}")
    logger.info(f"Filas inválidas en febrero según bisiesto: {df.filter((col('mes_int') == 2) & ((~es_bisiesto & (col('dia_int') > 28)) | (es_bisiesto & (col('dia_int') > 29)))).count()}")

    df_filtrado = df.filter(~cond_fecha_invalida).drop("ano_int", "mes_int", "dia_int")
    logger.info(f"Filas después del filtro de fechas inválidas: {df_filtrado.count()}")

    return df_filtrado

def impute_invalid_values(df):
    # Definir ventana por grupo
    window_spec = Window.partitionBy("magnitud", "estacion", "mes", "dia")

    # Calcular la media solo usando valores válidos (validez == 'V')
    df = df.withColumn(
        "mean_valor",
        avg(when(col("validez") == "V", col("dato"))).over(window_spec)
    )

    # Imputar dato solo donde validez == 'N' y media no es nula
    df = df.withColumn(
        "dato",
        when((col("validez") == "N") & (col("mean_valor").isNotNull()), col("mean_valor"))
        .otherwise(col("dato"))
    )

    # Cambiar validez de 'N' a 'V' solo si se imputó dato (dato no nulo)
    df = df.withColumn(
        "validez",
        when((col("validez") == "N") & (col("dato").isNotNull()), lit("V"))
        .otherwise(col("validez"))
    )

    # Eliminar columna auxiliar
    df = df.drop("mean_valor")

    # Devolver todas las filas que tengan validez 'V' (originales o imputadas)
    df_filtrado = df.filter(col("validez") == "V")

    return df_filtrado

def imputar_valores_nulos_dato(df):
    window_spec = Window.partitionBy("estacion", "magnitud", "mes", "dia")
    
    # Calcular media solo de valores no nulos en la ventana
    df = df.withColumn(
        "mean_dato",
        avg(col("dato")).over(window_spec)
    )
    
    # Reemplazar dato nulo por la media calculada
    df = df.withColumn(
        "dato",
        when(col("dato").isNull(), col("mean_dato")).otherwise(col("dato"))
    )
    
    # Eliminar columna auxiliar
    df = df.drop("mean_dato")
    
    return df

def eliminar_filas_dato_null(df):
    df = df.filter(col("dato").isNotNull())
    return df

def transformar_fecha(df):
    # convertir año, mes y día a fecha y eliminar columnas originales
    df = df.withColumn("mes", lpad(col("mes").cast("string"), 2, "0"))
    df = df.withColumn("dia", lpad(col("dia").cast("string"), 2, "0"))
    df = df.withColumn("fecha", to_date(concat_ws("-", col("ano"), col("mes"), col("dia")), "yyyy-MM-dd"))
    df = df.drop("ano", "mes", "dia")
    return df

def renombrar_magnitud(df):
    mapping = {
        1: "SO2",
        6: "CO",
        7: "NO",
        8: "NO2",
        9: "PM2.5",
        10: "PM10",
        12: "NOx",
        14: "O3",
        20: "TOL",
        30: "BEN",
        35: "EBE",
        37: "MXY",
        38: "PXY",
        39: "OXY",
        42: "TCH",
        43: "CH4",
        44: "NMHC",
        431: "MPX",
    }

    from pyspark.sql.functions import create_map, lit
    mapping_expr = create_map([lit(x) for pair in mapping.items() for x in pair])

    df = df.withColumn("magnitud", mapping_expr[col("magnitud")])
    return df

def join_posicion(df, df_pos):
    df = df.join(
        df_pos.withColumnRenamed("codigo_corto", "estacion"),
        on="estacion",
        how="left"
    ).drop("codigo_corto")  # elimina columna duplicada
    return df

def eliminar_duplicados(df):
    return df.dropDuplicates()

# --- Pipeline principal ---
def process_contamination(spark):
    logger.info("Inicio procesamiento contaminación")

    # 1. Esquemas
    schema_calidad = schema_CalidadAireMes()
    schema_estaciones = schema_Estaciones()

    # 2. Cargar datasets
    df_cont = spark.read.schema(schema_calidad).parquet(*paths_calidad_aire)
    df_pos = spark.read.schema(schema_estaciones).parquet(STATIONS_RAW_PATH)
    logger.info(f"Carga datasets completada: cont={df_cont.count()} filas, pos={df_pos.count()} filas")

    # 3. Limpiar columnas irrelevantes
    df_cont = eliminar_columnas_irrelevantes(df_cont)
    logger.info("Columnas irrelevantes eliminadas en df_cont")

    # 4. Seleccionar columnas relevantes de posición
    df_pos = seleccionar_posicion(df_pos)
    logger.info("Seleccionadas columnas posición en df_pos")

    # 5. Unpivot columnas diarias D01...D31 a filas
    count_before_unpivot = df_cont.count()
    logger.info(f"Filas antes unpivot: {count_before_unpivot}")

    df_cont = unpivot_diarios(df_cont)
    count_after_unpivot = df_cont.count()
    logger.info(f"Unpivot completado: filas antes={count_before_unpivot}, después={count_after_unpivot}")

    # Aquí podrías mostrar una muestra para verificar formato y datos
    df_cont.show(5, truncate=False)

    # 6. Filtrar fechas inválidas 2019-2025
    df_cont = filtrar_fechas_invalidas_debug(df_cont,logger)
    logger.info(f"Filtrado fechas inválidas completado: {df_cont.count()} filas")
    df_cont.show(5, truncate=False)

    # 7. Imputar valores inválidos
    df_cont = impute_invalid_values(df_cont)
    logger.info(f"Imputación valores inválidos completada: {df_cont.count()} filas")
    df_cont.show(5, truncate=False)

    # 8. Imputar valores nulos en dato
    df_cont = imputar_valores_nulos_dato(df_cont)
    logger.info(f"Imputación valores nulos completada: {df_cont.count()} filas")
    df_cont.show(5, truncate=False)
    # 9. Imputar valores nulos en columna dato
    df_cont = eliminar_filas_dato_null(df_cont)
    logger.info(f"Filas con dato null eliminadas: quedan {df_cont.count()} filas")
    # 10. Transformar fecha y eliminar columnas año/mes/día
    df_cont = transformar_fecha(df_cont)
    logger.info("Transformación de fecha completada")

    # 11. Renombrar códigos de magnitud a nombres legibles
    df_cont = renombrar_magnitud(df_cont)
    logger.info("Renombrado magnitud completado")

    # 12. Join con posición para añadir coordenadas
    count_before_join = df_cont.count()
    df_cont = join_posicion(df_cont, df_pos)
    count_after_join = df_cont.count()
    logger.info(f"Join con posiciones completado: filas antes={count_before_join}, después={count_after_join}")

    # 13 Eliminar duplicados
    df_final = eliminar_duplicados(df_cont)
    logger.info(f"Duplicados eliminados: filas después={df_final.count()}")

    # Mostrar esquema y primeras filas para depuración
    logger.info("Esquema antes de guardar:")
    df_final.printSchema()

    logger.info("Primeras filas antes de guardar:")
    df_final.show(5, truncate=False)

    logger.info(f"Número de filas antes de guardar: {df_final.count()}")

    # 14. Guardar como parquet
    save_data(df_final, POLLUTION_PROCESSED_PATH, "contaminación procesada")
    logger.info("Guardado dataset procesado completado")

    logger.info("Fin procesamiento contaminación")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PreprocessContamination")\
                        .master("local[*]")\
                        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")\
                        .config("spark.driver.host", "localhost")\
                        .getOrCreate()

    try:
        process_contamination(spark)
    except Exception as e:
        logger.error(f"Error en pipeline principal: {e}")
    finally:
        spark.stop()
        logger.info("Spark session finalizada")