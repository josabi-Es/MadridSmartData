"""ingest (bronze) -> silver -> gold, como grafo real, no como 2 cajas negras.

Antes: 2 tasks, `ingest` (los 5 datasets bronze encadenados a ciegas con
`&&` aunque son independientes) y `run_pipeline` (todo silver+gold en un
solo `python -m src.data.run_pipeline`, invisible desde Airflow). Ahora: una
task por paso real, conectadas solo donde hay una dependencia de datos de
verdad -- los 5 bronze no dependen entre sí (van en paralelo), cada
silver/gold espera solo a su propio upstream. Ni 2 bloques opacos ni "todo
en paralelo sin sentido" -- el grafo de la UI es la lineage real:

    ingest_distritos ────────────┬──────────────────────────┐
    ingest_estaciones_aire ──────┼─> silver_estaciones        │
                                  └─> gold_dim_estacion_aire ──┼─> gold_dim_distrito
    ingest_trafico_puntos_medida ───> gold_dim_punto_trafico ──┘
    gold_dim_magnitud (catálogo estático, sin dependencias)
    ingest_aire ─> silver_aire ───────────────────────┐
    ingest_trafico ─> silver_trafico ───────────────┐  │
    gold_dim_estacion_aire + silver_aire ─> gold_fact_calidad_aire
    gold_dim_punto_trafico + silver_trafico ─> gold_fact_trafico

`year`/`month` son params del DAG (default 2024/12) -- override desde
"Trigger DAG w/ config" en la UI para pedir otro mes sin tocar `.env` ni
reconstruir el contenedor. `aire` usa solo año, `trafico`/
`trafico_puntos_medida` necesitan año-mes. Sigue siendo un único año/mes
por ejecución, no un bucle histórico -- eso queda pendiente (ver
CLAUDE.md, fase 2).

`src/data/run_pipeline.py` sigue existiendo tal cual para `make pipeline`
-- este DAG no lo llama, invoca cada paso por separado vía su propio
`python -m src.data.<capa>.<módulo>`.
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}


def _bash(task_id: str, command: str) -> BashOperator:
    return BashOperator(task_id=task_id, cwd="/opt/repo", bash_command=command)


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
    # --- bronze: 5 datasets, independientes entre sí ---
    ingest_distritos = _bash(
        "ingest_distritos",
        "python -m src.data.bronze.pipeline --dataset distritos",
    )
    ingest_estaciones_aire = _bash(
        "ingest_estaciones_aire",
        "python -m src.data.bronze.pipeline --dataset estaciones_aire",
    )
    ingest_trafico_puntos_medida = _bash(
        "ingest_trafico_puntos_medida",
        "python -m src.data.bronze.pipeline --dataset trafico_puntos_medida "
        "--years {{ params.year }}-{{ params.month }}",
    )
    ingest_aire = _bash(
        "ingest_aire",
        "python -m src.data.bronze.pipeline --dataset aire --years {{ params.year }}",
    )
    ingest_trafico = _bash(
        "ingest_trafico",
        "python -m src.data.bronze.pipeline --dataset trafico "
        "--years {{ params.year }}-{{ params.month }}",
    )

    # --- silver: cada uno depende solo de su propio bronze ---
    silver_aire = _bash("silver_aire", "python -m src.data.silver.aire")
    silver_trafico = _bash("silver_trafico", "python -m src.data.silver.trafico")
    silver_estaciones = _bash(
        "silver_estaciones", "python -m src.data.silver.district_join"
    )

    # --- gold dimensiones ---
    gold_dim_estacion_aire = _bash(
        "gold_dim_estacion_aire",
        "python -m src.data.gold.dimensions --target dim_estacion_aire",
    )
    gold_dim_punto_trafico = _bash(
        "gold_dim_punto_trafico",
        "python -m src.data.gold.dimensions --target dim_punto_trafico",
    )
    gold_dim_magnitud = _bash(
        "gold_dim_magnitud",
        "python -m src.data.gold.dimensions --target dim_magnitud",
    )
    gold_dim_distrito = _bash(
        "gold_dim_distrito",
        "python -m src.data.gold.dimensions --target dim_distrito",
    )

    # --- gold hechos ---
    gold_fact_calidad_aire = _bash(
        "gold_fact_calidad_aire",
        "python -m src.data.gold.facts --target fact_calidad_aire",
    )
    gold_fact_trafico = _bash(
        "gold_fact_trafico",
        "python -m src.data.gold.facts --target fact_trafico",
    )

    # --- lineage real ---
    [ingest_estaciones_aire, ingest_distritos] >> silver_estaciones
    [ingest_estaciones_aire, ingest_distritos] >> gold_dim_estacion_aire
    ingest_trafico_puntos_medida >> gold_dim_punto_trafico
    dim_distrito_deps = [
        gold_dim_estacion_aire, gold_dim_punto_trafico, ingest_distritos
    ]  # fmt: skip
    dim_distrito_deps >> gold_dim_distrito

    ingest_aire >> silver_aire
    ingest_trafico >> silver_trafico

    [silver_aire, gold_dim_estacion_aire] >> gold_fact_calidad_aire
    [silver_trafico, gold_dim_punto_trafico] >> gold_fact_trafico
    # gold_dim_magnitud no depende de nada -- corre suelto.
