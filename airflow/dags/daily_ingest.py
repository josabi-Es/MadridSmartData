"""ingest (bronze, año/mes desde INGEST_YEAR/INGEST_MONTH) -> run_pipeline.

2 tasks, no una por script: menos nodos que mantener, a costa de ver el
fallo por bloque (ingest vs pipeline) en vez de por script individual.
`run_pipeline` ya encadena silver/{aire,trafico,district_join} ->
gold.dimensions -> gold.facts (src/data/run_pipeline.py), así que aquí
solo se invoca una vez.

INGEST_YEAR/INGEST_MONTH vienen del .env del repo (ver .env.template),
pasados al contenedor en docker-compose.yml. `aire` usa solo año,
`trafico`/`trafico_puntos_medida` necesitan año-mes -- por eso se
construye `$INGEST_YEAR-$INGEST_MONTH` para esos dos. Sigue siendo un
único año/mes por ejecución, no un bucle histórico -- eso queda pendiente
(ver CLAUDE.md, fase 2).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "daily_ingest",
    schedule=None,
    catchup=False,
    start_date=datetime(2024, 1, 1),
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        cwd="/opt/repo",
        bash_command=(
            "python -m src.data.bronze.pipeline --dataset distritos && "
            "python -m src.data.bronze.pipeline --dataset estaciones_aire && "
            "python -m src.data.bronze.pipeline --dataset trafico_puntos_medida "
            "--years $INGEST_YEAR-$INGEST_MONTH && "
            "python -m src.data.bronze.pipeline --dataset aire --years $INGEST_YEAR && "
            "python -m src.data.bronze.pipeline --dataset trafico "
            "--years $INGEST_YEAR-$INGEST_MONTH"
        ),
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        cwd="/opt/repo",
        bash_command="python -m src.data.run_pipeline",
    )

    ingest >> run_pipeline
