"""Single entry point: bronze (already downloaded) -> silver -> gold.

Ingestion stays manual/out-of-band (see `src.data.ingest_api_bronze`) --
this only chains silver cleaning + gold dimensions/facts. Idempotent,
safe to re-run. Airflow will split this into DAG tasks eventually; kept
as one linear `main()` until then.
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

SILVER_AIR_PATH = os.getenv("SILVER_AIR_PATH", "data/silver/aire.parquet")
SILVER_TRAFFIC_PATH = os.getenv("SILVER_TRAFFIC_PATH", "data/silver/trafico.parquet")
SILVER_ESTACIONES_PATH = os.getenv(
    "SILVER_ESTACIONES_PATH", "data/silver/estaciones_aire.parquet"
)


def main() -> None:
    logger.info("Step 1/3: silver -- air quality cleaning")
    unpivot_air_quality(BRONZE_AIR_PATH, SILVER_AIR_PATH)

    logger.info("Step 1/3: silver -- traffic cleaning")
    clean_traffic(BRONZE_TRAFFIC_PATH, SILVER_TRAFFIC_PATH)

    logger.info("Step 1/3: silver -- air station district assignment")
    assign_district(BRONZE_ESTACIONES_PATH, DISTRITOS_PATH, SILVER_ESTACIONES_PATH)

    logger.info("Step 2/3: gold -- dimensions")
    gold_dimensions.main()

    logger.info("Step 3/3: gold -- facts")
    gold_facts.main()

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
