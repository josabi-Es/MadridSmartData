"""Reentreno del forecast -- disparo independiente de daily_ingest.

No siempre hace falta reentrenar cuando se ingesta (spec.md), por eso es
un DAG aparte en vez de una task más al final de daily_ingest.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "retrain_forecast",
    schedule=None,
    catchup=False,
    start_date=datetime(2024, 1, 1),
) as dag:
    train_ml = BashOperator(
        task_id="train_ml",
        cwd="/opt/repo",
        bash_command="python -m src.ml.train",
    )
