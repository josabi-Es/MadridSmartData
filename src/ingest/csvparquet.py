from pyspark.sql import SparkSession
import os
from ReadCSV import schema_PosTraffic, schema_traffic , schema_Contamination, schema_ContaminationStation, schema_Contamination_v2  # noqa: F401

"""
This script reads all CSV files in a directory organized by year,
converts them to Parquet format using a given schema, and saves
the output into a corresponding directory structure.

"""


def toparquet(input_dir: str, output_dir: str, schema):
    # 1. Create Spark session
    spark = SparkSession.builder.appName("CSV to Parquet Converter")\
                         .master("local[*]")\
                         .config("spark.driver.host", "localhost")\
                         .getOrCreate()

    # 2. Process files year by year
    for year in range(2019, 2026):
        input_dir_year = os.path.join(input_dir, str(year))
        output_dir_year = os.path.join(output_dir, str(year))

        os.makedirs(output_dir_year, exist_ok=True)

        if not os.path.exists(input_dir_year):
            print(f"Input directory {input_dir_year} not found, skipping...")
            continue

        # 3. Process each CSV file in the year's folder
        for file in os.listdir(input_dir_year):
            if file.endswith(".csv"):
                input_path = os.path.join(input_dir_year, file)
                output_path = os.path.join(output_dir_year, file.replace(".csv", ".parquet"))

                print(f"Processing {file} for year {year}...")

                df = spark.read.csv(
                    input_path,
                    header=True,
                    schema=schema,
                    sep=';',
                    timestampFormat="yyyy-MM-dd HH:mm:ss"
                )

                df.write.mode("overwrite").parquet(output_path)

    # 4. Stop Spark session
    spark.stop()

    return True

if __name__ == "__main__":
    input_dir = "Your folder with CSV data"
    output_dir = "Your folder for Parquet output"
    schema = schema_Contamination_v2()

    toparquet(input_dir, output_dir, schema)