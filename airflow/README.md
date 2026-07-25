# airflow/

Base para orquestar el proyecto con Airflow — pensada para encenderla
cuando haga falta, no corre nada por defecto.

## Levantarla

Primera vez: copia el `.env.template` de la raíz del repo (puerto +
credenciales, ver abajo):

```bash
cp .env.template .env
docker compose up -d
```

La primera vez construye la imagen (`airflow/Dockerfile`, deps mínimas
para `src/data/*` y `src/ml/*` encima de la imagen oficial de Airflow).
`standalone` inicializa la base de datos sola, sin pasos manuales.

## Credenciales

Fijas por env var (`.env` en la raíz del repo, ver `.env.template`):

```
AIRFLOW_PORT=8081
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

El propio entrypoint de la imagen oficial de Airflow crea ese usuario al
arrancar (`_AIRFLOW_WWW_USER_CREATE`/`_AIRFLOW_WWW_USER_USERNAME`/
`_AIRFLOW_WWW_USER_PASSWORD` en `docker-compose.yml`) — sin esto,
`standalone` genera un admin con contraseña aleatoria distinta cada vez
que el contenedor arranca de cero, molesto para un demo en el que solo
quieres entrar. Sin volumen con nombre para `AIRFLOW_HOME`: el estado no
sobrevive a `docker compose down`, y no hace falta que sobreviva —
`_AIRFLOW_WWW_USER_CREATE` vuelve a crear el mismo admin en cada arranque
limpio.

UI en <http://localhost:8081> (o el puerto que pongas en `AIRFLOW_PORT`).

## DAGs ya incluidos

- **`daily_ingest`** (`airflow/dags/daily_ingest.py`) — 2 tasks:
  `ingest` (bronze, un `--dataset`/`--years` fijo por dataset, datos de
  demo) `>>` `run_pipeline` (una sola llamada a `python -m
  src.data.run_pipeline`, que ya encadena silver → gold.dimensions →
  gold.facts). Deliberadamente 2 nodos, no uno por script — menos
  mantenimiento, a cambio de ver el fallo por bloque en vez de por script
  individual en el árbol de Airflow.
- **`retrain_forecast`** (`airflow/dags/retrain_forecast.py`) — 1 task,
  `python -m src.ml.train`. Disparo independiente: no siempre hace falta
  reentrenar cada vez que se ingesta.

Ambos con `schedule=None` — se disparan a mano desde la UI (`Trigger
DAG`), no hay cron. `ingest` reingesta siempre los mismos
dataset/año-mes fijos, sobrescribiendo (ver `append_manifest` en
`src/data/bronze/pipeline.py`, hace upsert por year/month, no acumula
duplicados en `manifest.json` al reingestar).

## Añadir DAGs nuevos

Cualquier `.py` que dejes en `airflow/dags/` aparece en la UI — es un
montaje del repo (no algo dentro del contenedor), así que lo que crees
ahí se queda guardado en el propio proyecto aunque pares o borres el
contenedor.

Dentro de un DAG puedes importar el código del proyecto tal cual, porque
el repo entero está montado en `/opt/repo` y `PYTHONPATH` apunta ahí. Las
tasks de este proyecto usan `BashOperator` con `python -m src.<módulo>`
directamente (no `.venv/bin/python`) porque el `Dockerfile` ya instala
las dependencias del proyecto dentro de la propia imagen de Airflow —
evita el problema de que un `.venv` creado en PowerShell nativo en
Windows no sirva montado en el contenedor Linux:

```python
BashOperator(task_id="x", cwd="/opt/repo", bash_command="python -m src.data.bronze.pipeline ...")
```

## Por qué el Dockerfile instala con `--constraint`

Airflow fija versiones concretas de sus propias dependencias. Instalar
paquetes nuevos (los de este proyecto) sin el fichero de constraints
oficial de Airflow es la forma más común de romper la imagen — el
`Dockerfile` lo usa a propósito, no lo quites aunque parezca redundante.

## Por qué el Dockerfile no instala `pyproject.toml` entero

Se probó exportar todas las deps del proyecto (`uv export`) e instalarlas
con el constraints de Airflow — falla: `gradio`/`fastapi` (solo los usa el
dashboard, ningún DAG los importa) fijan una versión de `anyio` que choca
con la que pide Airflow. Como los DAGs solo tocan `src/data/*` y
`src/ml/*`, el `Dockerfile` instala esa lista corta a mano en vez de
pelear con el resolver por dependencias que aquí no hacen falta.

## Apagarla

```bash
docker compose down
```

Sin volumen con nombre, `down` ya se lleva todo el estado (DB de Airflow,
runs, logs) — el próximo `up` empieza de cero, con el mismo admin fijo.
