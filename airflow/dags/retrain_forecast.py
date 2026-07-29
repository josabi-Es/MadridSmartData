
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

with DAG(
    "retrain_forecast",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["ml"],
    doc_md=__doc__,
    params={"predict_months": 2, "gases": "NO2,O3"},
) as dag:
    BashOperator(
        task_id="train",
        cwd="/opt/repo",
        bash_command="python -m src.ml.main",
        append_env=True,  # si no, `env` reemplaza el entorno entero
        env={
            "PREDICT_MONTHS": "{{ params.predict_months }}",
            "GASES": "{{ params.gases }}",
        },
    )
