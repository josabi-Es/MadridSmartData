from pyspark.sql import SparkSession
from pathlib import Path

"""
This script reads monthly parquet files from a base folder,
combines them by month, and saves them to an output directory.

"""

# Initialize Spark
spark = SparkSession.builder.appName("Combine by Month").master("local[*]").getOrCreate()

# Base folder with CSV data (or parquet input)
base_path = Path("Your folder with csv data")

# Output folder for combined parquet files
output_dir = Path("Your folder with parquet data")
output_dir.mkdir(parents=True, exist_ok=True)

# Iterate through years
for year_path in sorted(base_path.glob("20*")):
    if not year_path.is_dir():
        continue

    year = year_path.name

    # Iterate through months within the year
    for month_path in sorted(year_path.glob("*")):
        if not month_path.is_dir():
            continue

        # Get all valid .parquet files for the month
        parquet_files = [str(f) for f in month_path.glob("*.parquet")
                         if not f.name.startswith(('_', '.')) and f.suffix == ".parquet"]

        if not parquet_files:
            continue

        # Read and combine all parquet files of the month
        df = spark.read.parquet(*parquet_files)

        # Save combined parquet file by month
        output_path = output_dir / f"{year}_{month_path.name}.parquet"
        df.coalesce(1).write.mode("overwrite").parquet(str(output_path))

spark.stop()
