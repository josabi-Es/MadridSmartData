from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, DoubleType, TimestampType)

"""
This script reads a CSV file containing contamination data using a predefined schema,
loads it into a Spark DataFrame, caches it for faster processing, and prints basic info.

"""

def schema_PosTraffic():
    schema = StructType([
        StructField("tipo_elem", StringType(), True),
        StructField("distrito", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("cod_cent", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("utm_x", DoubleType(), True),
        StructField("utm_y", DoubleType(), True),
        StructField("longitud", DoubleType(), True),
        StructField("latitud", DoubleType(), True)
    ])
    return schema

def schema_traffic():
    schema = StructType([
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
    return schema

def schema_Contamination():
    schema = StructType([
        StructField("provincia", IntegerType(), True),
        StructField("municipio", IntegerType(), True),
        StructField("estacion", IntegerType(), True),
        StructField("magnitud", IntegerType(), True),
        StructField("punto_muestreo", StringType(), True),
        StructField("ano", IntegerType(), True),
        StructField("mes", IntegerType(), True),
        StructField("dia", IntegerType(), True),

        StructField("h01", IntegerType(), True),
        StructField("v01", StringType(), True),
        StructField("h02", IntegerType(), True),
        StructField("v02", StringType(), True),
        StructField("h03", IntegerType(), True),
        StructField("v03", StringType(), True),
        StructField("h04", IntegerType(), True),
        StructField("v04", StringType(), True),
        StructField("h05", IntegerType(), True),
        StructField("v05", StringType(), True),
        StructField("h06", IntegerType(), True),
        StructField("v06", StringType(), True),
        StructField("h07", IntegerType(), True),
        StructField("v07", StringType(), True),
        StructField("h08", IntegerType(), True),
        StructField("v08", StringType(), True),
        StructField("h09", IntegerType(), True),
        StructField("v09", StringType(), True),
        StructField("h10", IntegerType(), True),
        StructField("v10", StringType(), True),
        StructField("h11", IntegerType(), True),
        StructField("v11", StringType(), True),
        StructField("h12", IntegerType(), True),
        StructField("v12", StringType(), True),
        StructField("h13", IntegerType(), True),
        StructField("v13", StringType(), True),
        StructField("h14", IntegerType(), True),
        StructField("v14", StringType(), True),
        StructField("h15", IntegerType(), True),
        StructField("v15", StringType(), True),
        StructField("h16", IntegerType(), True),
        StructField("v16", StringType(), True),
        StructField("h17", IntegerType(), True),
        StructField("v17", StringType(), True),
        StructField("h18", IntegerType(), True),
        StructField("v18", StringType(), True),
        StructField("h19", IntegerType(), True),
        StructField("v19", StringType(), True),
        StructField("h20", IntegerType(), True),
        StructField("v20", StringType(), True),
        StructField("h21", IntegerType(), True),
        StructField("v21", StringType(), True),
        StructField("h22", IntegerType(), True),
        StructField("v22", StringType(), True),
        StructField("h23", IntegerType(), True),
        StructField("v23", StringType(), True),
        StructField("h24", IntegerType(), True),
        StructField("v24", StringType(), True)
    ])
    return schema

def schema_Contamination_v2():
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
    schema = StructType(fields)
    return schema

def schema_ContaminationStation():
    schema = StructType([
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
        StructField("latitud", DoubleType(), True)
    ])
    return schema



if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("PySpark CSV Example - Contamination Data") \
        .master("local[8]") \
        .config("spark.driver.host", "localhost")\
        .getOrCreate()

    # Define schema
    schema = schema_Contamination_v2()

    # Path to input CSV file
    input_path = "Your folder with CSV data/contamination_2024.csv"

    # Read CSV using predefined schema
    dataFrame = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("sep", ";")\
        .schema(schema) \
        .load(input_path) \
        .cache()

    # Show schema and first rows
    dataFrame.printSchema()
    dataFrame.show(5)

    # Print total row count
    print("Number of rows:", dataFrame.count())