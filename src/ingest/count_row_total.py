"""
This script quickly counts the total number of rows across multiple Parquet files
organized by year. Useful for a fast overview of the dataset size.
"""

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Read Parquet Files")\
                         .master("local[*]")\
                         .config("spark.driver.host", "localhost")\
                         .getOrCreate()

# Base folder containing yearly Parquet files
base_path = "Your folder with Parquet files"

# Read multiple Parquet files and count rows
total_rows = 0
try:
    for year in range(2019, 2026):
        try:
            parquet_file = spark.read.parquet(f"{base_path}/{year}/*.parquet")
            total_rows += parquet_file.count()
        except Exception as e:
            print(f"Error reading files for {year}: {e}")

    # Display total row count
    print(f"Total number of rows: {total_rows}")

finally:
    spark.stop()
    print("Spark session stopped.")
