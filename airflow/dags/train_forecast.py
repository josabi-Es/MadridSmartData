"""Reentreno del forecast -- disparo independiente de daily_ingest.

No siempre hace falta reentrenar cuando se ingesta (spec.md), por eso es
un DAG aparte en vez de una task más al final de daily_ingest.

Antes: una única task `train_ml` llamando a `python -m src.ml.train`
(monolítico, un fallo de un modelo tumbaba el reentreno de todas las
variables). Ahora: una task por combinación (modelo, variable) -- 5 modelos
x 4 variables = 20 tasks en paralelo, cada una ejecuta su notebook via
papermill (`src.ml.notebooks.run_one`) y escribe solo su fila de
comparación en `gold/ml_runs/`. Fallo aislado: una task cayéndose no afecta
a las demás. La task final `promote_all` (trigger_rule="all_done") corre
igual aunque falten algunas de las 20 -- promueve el mejor modelo
disponible por variable con `src.ml.promote.promote_winner`, la misma
función que usa `python -m src.ml.train` en local, para que el criterio de
selección del ganador no viva en dos sitios.
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

MODELS = ["naive", "decision_tree", "random_forest", "xgboost", "mlp"]
VARIABLES = ["NO2", "PM10", "PM2.5", "intensidad"]

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # 20 ejecuciones de notebook tardan más que el CLI monolítico de antes
    # (20 min bastaban para 1 sola llamada a train.py) -- medir con datos
    # reales y ajustar; de momento se dobla el margen anterior.
    "execution_timeout": timedelta(minutes=40),
}


def _promote_all(ano: str, **_) -> None:
    from src.ml.promote import promote_winner

    # PythonOperator runs in-process in the scheduler, whose cwd is
    # /opt/airflow (not /opt/repo like the BashOperator tasks above, which
    # set cwd explicitly) -- relative paths here would silently miss the
    # run files the notebooks just wrote.
    for variable in VARIABLES:
        promote_winner(
            variable,
            int(ano),
            runs_dir="/opt/repo/data/gold/ml_runs",
            models_dir="/opt/repo/data/gold",
            air_path="/opt/repo/data/silver/aire/all.parquet",
            traffic_path="/opt/repo/data/silver/trafico/all.parquet",
        )


with DAG(
    "retrain_forecast",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    tags=["ml"],
    doc_md=__doc__,
    params={"ano": datetime.now().year},
) as dag:
    run_tasks = [
        BashOperator(
            task_id=f"run_{model}_{variable}",
            cwd="/opt/repo",
            bash_command=(
                f"python -m src.ml.notebooks.run_one --model {model} "
                f"--variable {variable!r} --ano {{{{ params.ano }}}}"
            ),
        )
        for model in MODELS
        for variable in VARIABLES
    ]

    promote_all = PythonOperator(
        task_id="promote_all",
        python_callable=_promote_all,
        op_kwargs={"ano": "{{ params.ano }}"},
        trigger_rule="all_done",
    )

    run_tasks >> promote_all
