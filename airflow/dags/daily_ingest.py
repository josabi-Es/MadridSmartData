import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}


def _bash(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd /opt/repo && {command}",
    )


YEAR_START = int(os.getenv("INGEST_YEAR_START", "2023"))
YEAR_END = int(os.getenv("INGEST_YEAR_END", "2025"))

with DAG(
    "daily_ingest",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    tags=["ingest", "gold"],
    doc_md=__doc__,
    params={"year": YEAR_END, "month": 12},
) as dag:
    # --- bronze: 5 datasets, independent (run in parallel) ---
    with TaskGroup("bronze") as bronze_tg:
        bronze_distritos = _bash(
            "ingest_distritos",
            "python -m src.data.ingest_api_bronze --dataset distritos",
        )
        bronze_estaciones_aire = _bash(
            "ingest_estaciones_aire",
            "python -m src.data.ingest_api_bronze --dataset estaciones_aire",
        )
        bronze_trafico_puntos_medida = _bash(
            "ingest_trafico_puntos_medida",
            "python -m src.data.ingest_api_bronze --dataset trafico_puntos_medida "
            "--years {{ params.year }}-{{ params.month }}",
        )
        bronze_aire = _bash(
            "ingest_aire",
            "python -m src.data.ingest_api_bronze --dataset aire "
            "--years {{ params.year }}",
        )
        bronze_trafico = _bash(
            "ingest_trafico",
            "python -m src.data.ingest_api_bronze --dataset trafico "
            "--years {{ params.year }}-{{ params.month }}",
        )

    # --- silver: each depends only on its own bronze ---
    with TaskGroup("silver") as silver_tg:
        silver_aire = _bash("silver_aire", "python -m src.data.silver.aire")
        silver_trafico = _bash(
            "silver_trafico", "python -m src.data.silver.trafico"
        )
        silver_estaciones = _bash(
            "silver_estaciones", "python -m src.data.silver.district_join"
        )

    # --- gold: dimensions + facts ---
    with TaskGroup("gold") as gold_tg:
        with TaskGroup("dimensions") as gold_dims:
            gold_dim_estacion_aire = _bash(
                "gold_dim_estacion_aire",
                "python -m src.data.gold.dimensions --target dim_estacion_aire",
            )
            gold_dim_punto_trafico = _bash(
                "gold_dim_punto_trafico",
                "python -m src.data.gold.dimensions --target dim_punto_trafico",
            )
            gold_dim_distrito = _bash(
                "gold_dim_distrito",
                "python -m src.data.gold.dimensions --target dim_distrito",
            )

        with TaskGroup("facts") as gold_facts:
            gold_fact_calidad_aire = _bash(
                "gold_fact_calidad_aire",
                "python -m src.data.gold.facts --target fact_calidad_aire",
            )
            gold_fact_trafico = _bash(
                "gold_fact_trafico",
                "python -m src.data.gold.facts --target fact_trafico",
            )

    # --- lineage: real dependencies only ---
    [bronze_estaciones_aire, bronze_distritos] >> silver_estaciones
    [bronze_estaciones_aire, bronze_distritos] >> gold_dim_estacion_aire
    bronze_trafico_puntos_medida >> gold_dim_punto_trafico
    dim_distrito_deps = [
        gold_dim_estacion_aire,
        gold_dim_punto_trafico,
        bronze_distritos,
    ]  # fmt: skip
    dim_distrito_deps >> gold_dim_distrito

    bronze_aire >> silver_aire
    bronze_trafico >> silver_trafico

    [silver_aire, gold_dim_estacion_aire] >> gold_fact_calidad_aire
    [silver_trafico, gold_dim_punto_trafico] >> gold_fact_trafico
