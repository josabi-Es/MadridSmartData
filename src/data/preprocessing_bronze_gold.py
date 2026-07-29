"""Single entry point: bronze (already downloaded) -> silver -> gold.

Ingestion itself stays manual/out-of-band (one `python -m
src.data.bronze.pipeline --dataset ... --years ...` per dataset/year, see
data/README.md) -- this only chains the steps that don't need a human
picking a year: cleaning already-downloaded bronze into silver, then
building the gold dimensions and facts. Idempotent: every step overwrites
its output, safe to re-run. This is the script fase 3 (Airflow) will split
into DAG tasks -- kept as one linear `main()` until then.
"""

import os

from dotenv import load_dotenv

from src.data.gold import dimensions as gold_dimensions
from src.data.gold import facts as gold_facts
from src.data.silver.aire import unpivot_air_quality
from src.data.silver.district_join import assign_district
from src.data.silver.trafico import clean_traffic
from src.utils.logger_config import logger

load_dotenv()

BRONZE_AIR_PATH = os.getenv("BRONZE_AIR_PATH", "data/bronze/aire/*.parquet")
BRONZE_TRAFFIC_PATH = os.getenv("BRONZE_TRAFFIC_PATH", "data/bronze/trafico/*.parquet")
BRONZE_ESTACIONES_PATH = os.getenv(
    "BRONZE_ESTACIONES_PATH", "data/bronze/estaciones_aire/latest.parquet"
)
DISTRITOS_PATH = os.getenv("DISTRITOS_PATH", "data/bronze/distritos/latest.parquet")

SILVER_AIR_PATH = os.getenv("SILVER_AIR_PATH", "data/silver/aire/all.parquet")
SILVER_TRAFFIC_PATH = os.getenv(
    "SILVER_TRAFFIC_PATH", "data/silver/trafico/all.parquet"
)
SILVER_ESTACIONES_PATH = os.getenv(
    "SILVER_ESTACIONES_PATH", "data/silver/estaciones_aire/latest.parquet"
)


def main() -> None:
    logger.info("Paso 1/3: silver -- limpieza de aire")
    unpivot_air_quality(BRONZE_AIR_PATH, SILVER_AIR_PATH)

    logger.info("Paso 1/3: silver -- limpieza de tráfico")
    clean_traffic(BRONZE_TRAFFIC_PATH, SILVER_TRAFFIC_PATH)

    logger.info("Paso 1/3: silver -- distrito de estaciones de aire")
    assign_district(BRONZE_ESTACIONES_PATH, DISTRITOS_PATH, SILVER_ESTACIONES_PATH)

    logger.info("Paso 2/3: gold -- dimensiones")
    gold_dimensions.main()

    logger.info("Paso 3/3: gold -- hechos")
    gold_facts.main()

    logger.info("Pipeline completo.")


if __name__ == "__main__":
    main()
