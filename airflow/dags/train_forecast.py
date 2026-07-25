"""Reentreno del forecast -- disparo independiente de daily_ingest.

No siempre hace falta reentrenar cuando se ingesta (spec.md), por eso es
un DAG aparte en vez de una task más al final de daily_ingest.
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    "retrain_forecast",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    tags=["ml"],
    doc_md=__doc__,
) as dag:
    train_ml = BashOperator(
        task_id="train_ml",
        cwd="/opt/repo",
        bash_command="python -m src.ml.train",
    )
