from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

"""
This script reads a CSV file containing air quality station data,
applies a predefined schema, and converts it to Parquet format for faster processing.
"""

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
    spark = SparkSession.builder.appName("CSV to Parquet Converter") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

    # Input CSV file path
    input_path = "Your folder with CSV data/information_air_quality_stations.csv"

    # Output folder for Parquet files
    output_path = "Your folder for Parquet output"
    
    # Apply predefined schema
    schema = schema_ContaminationStation()

    # Read CSV and write to Parquet
    df = spark.read.csv(input_path, schema=schema, header=True, sep=";")
    df.write.mode("overwrite").parquet(output_path)

    # Notify user
    print("Conversion completed. Parquet file generated at:", output_path)

    spark.stop()