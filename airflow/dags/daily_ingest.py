"""ingest (bronze, año/mes vía params year/month) -> run_pipeline.

2 tasks, no una por script: menos nodos que mantener, a costa de ver el
fallo por bloque (ingest vs pipeline) en vez de por script individual.
`run_pipeline` ya encadena silver/{aire,trafico,district_join} ->
gold.dimensions -> gold.facts (src/data/run_pipeline.py), así que aquí
solo se invoca una vez.

`year`/`month` son params del DAG (default 2024/12) -- override desde
"Trigger DAG w/ config" en la UI para pedir otro mes sin tocar `.env` ni
reconstruir el contenedor. `aire` usa solo año, `trafico`/
`trafico_puntos_medida` necesitan año-mes. Sigue siendo un único año/mes
por ejecución, no un bucle histórico -- eso queda pendiente (ver
CLAUDE.md, fase 2).
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
    "daily_ingest",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    tags=["ingest", "gold"],
    doc_md=__doc__,
    params={"year": 2024, "month": 12},
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        cwd="/opt/repo",
        bash_command=(
            "python -m src.data.bronze.pipeline --dataset distritos && "
            "python -m src.data.bronze.pipeline --dataset estaciones_aire && "
            "python -m src.data.bronze.pipeline --dataset trafico_puntos_medida "
            "--years {{ params.year }}-{{ params.month }} && "
            "python -m src.data.bronze.pipeline --dataset aire "
            "--years {{ params.year }} && "
            "python -m src.data.bronze.pipeline --dataset trafico "
            "--years {{ params.year }}-{{ params.month }}"
        ),
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        cwd="/opt/repo",
        bash_command="python -m src.data.run_pipeline",
    )

    ingest >> run_pipeline
