# airflow/

Orquestación del proyecto — se enciende cuando hace falta, no corre nada
por defecto.

## Levantarla

```bash
cp .env.template .env
docker compose up -d
```

La primera vez construye la imagen (`airflow/Dockerfile`). `standalone`
inicializa la base de datos sola.

## Credenciales

Fijas por env var en el `.env` de la raíz (`AIRFLOW_PORT`,
`AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`). Las crea un `airflow users
create` explícito dentro del `command:` de `docker-compose.yml` — sin eso,
`standalone` genera una contraseña aleatoria distinta en cada arranque
limpio.

El estado (DB, runs, logs) vive en el volumen con nombre `airflow_home`, así
que **sobrevive a `docker compose down`**. Para empezar de cero:
`docker compose down -v`.

UI en <http://localhost:8081> (o el puerto de `AIRFLOW_PORT`).

## DAGs

- **`daily_ingest`** (`dags/daily_ingest.py`) — 13 tasks: 5 de ingesta a
  bronze en paralelo, 3 de silver, 4 de dimensiones gold y 2 de facts gold,
  encadenadas por dependencia real de datos. Params `year`/`month` (default
  2024/12), override desde "Trigger DAG w/ config".
- **`retrain_forecast`** (`dags/retrain_forecast.py`) — 1 task,
  `python -m src.ml.main`. Params `predict_months`/`gases`, que se pasan como
  env vars a la task. Disparo independiente: no hace falta reentrenar cada
  vez que se ingesta.

Ambos con `schedule=None` — se disparan a mano desde la UI. La ingesta hace
upsert por year/month en `manifest.json` (`append_manifest` en
`src/data/ingest_api_bronze.py`), así que reingestar no duplica.

## Añadir DAGs nuevos

Cualquier `.py` en `airflow/dags/` aparece en la UI: es un montaje del repo,
no algo dentro del contenedor.

Puedes importar el código del proyecto tal cual — el repo está montado en
`/opt/repo` y `PYTHONPATH` apunta ahí. Las tasks usan `BashOperator` con
`python -m src.<módulo>` (no `.venv/bin/python`): el `Dockerfile` instala las
dependencias dentro de la imagen, porque un `.venv` creado en Windows nativo
no sirve montado en el contenedor Linux.

```python
BashOperator(task_id="x", cwd="/opt/repo", bash_command="python -m src.data.ingest_api_bronze ...")
```

## Decisiones del Dockerfile

El fichero va sin comentarios a propósito; el porqué vive aquí.

- **`--constraint`**: Airflow fija versiones de sus dependencias, e instalar
  paquetes nuevos sin su fichero de constraints es la forma más común de
  romper la imagen. No lo quites aunque parezca redundante.
- **`duckdb` aparte y sin constraint**: el constraints lo pinea a 1.1.3, que
  no soporta el kwarg `encoding=` de `read_csv_auto` que usa
  `src/data/ingest_api_bronze.py`.
- **Lista corta a mano, no el `pyproject.toml` entero**: `gradio`/`fastapi`
  (solo los usa el dashboard, ningún DAG los importa) fijan un `anyio` que
  choca con el de Airflow. Se instala solo lo que los DAGs tocan.
- **`scikit-learn` aunque nada en `src/` la importe**: `XGBRegressor` hereda
  de `sklearn.base.RegressorMixin`, es dependencia de runtime.

## Apagarla

```bash
docker compose down      # conserva el estado
docker compose down -v   # lo borra
```
