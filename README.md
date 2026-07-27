# Madrid Smart Data

Traffic & air quality analytics for Madrid open data: ingest → clean →
forecast → dashboard, orchestrated by Airflow.

<div align="center">
<img src="https://skillicons.dev/icons?i=python,docker,git,githubactions&theme=light" />
<br/><br/>
<img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" />
<img src="https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" />
<img src="https://img.shields.io/badge/Parquet-50ABF1?style=for-the-badge" />
<img src="https://img.shields.io/badge/Gradio-FF7C00?style=for-the-badge" />
<img src="https://img.shields.io/badge/scikit--learn-F7931E?style=for-the-badge&logo=scikitlearn&logoColor=white" />
</div>

## Workflow

![orchestration](docs/workflow.png)

## Where the data comes from

[Madrid Open Data](https://datos.madrid.es) is the city council's public
data portal, free datasets (traffic, air quality, districts...) anyone
can query, no login needed. It's served through **CKAN**, a standard
open-data API: one call (`package_show?id=<dataset>`) returns the
resource list for a dataset, then each resource is just a downloadable
CSV/file. `src/data/bronze/ckan.py` does exactly that call, one dataset
id per source (air, stations, traffic...).

That raw pull lands in **DuckDB + Parquet**, no database server, just
files queried directly with SQL, following the **Medallion**
architecture: `bronze/` (raw, as CKAN gave it), `silver/` (cleaned),
`gold/` (final tables + trained models), each stage only reading the one
before it.

## Run it

```bash
cp .env.template .env
docker compose up --build
```

Two services, two ports:

| Port | Service | What it's for |
|---|---|---|
| **7860** | `dashboard` (Gradio) | See the result: the visualizer, already reading whatever data is in `data/` |
| **8081** | `airflow` (`admin`/`admin`, from `.env`) | Trigger ingestion and retraining |

Trigger sequence in the Airflow UI (`Trigger DAG w/ config`):

1. **`daily_ingest`** first: pulls a year/month from the CKAN API into
   `bronze/`, then runs `src/data/run_pipeline.py` (silver → gold dims/facts).
2. **`train_forecast`** after, only once there's data: retrains the
   forecasting models onto `gold/`.

![airflow](docs/airflow.png)

## Dashboard cli

```bash
cp .env.template .env
uv venv
uv sync
uv run python -m src.dashboard.interface
```

## CI/CD

**CI**: en cada `push`/merge a `main` (nunca en ramas o PRs sueltos) se
corre `ruff check` + `pytest` vía GitHub Actions (`.github/workflows/ci-cd.yml`).

**CD**: no hay despliegue real todavía — se simula. Un commit *breaking*
(Conventional Commits: `feat!: ...` o footer `BREAKING CHANGE: ...`) crea
automáticamente una rama `release-v{N}` a partir de `main`, numerada de
forma secuencial (`release-v1`, `release-v2`, ...). Esa sería la rama que
un servidor de producción leería siempre — la más alta es la más
actualizada. Un `feat:`/`fix:` normal solo deja `main` en verde, sin crear
rama nueva.

Para dispararlo al hacer *squash merge* de un PR: el mensaje del commit de
squash es por defecto el título del PR (editable en la caja de merge) —
ahí va el `!` (`feat!: cambia el esquema de X`), o si se prefiere no tocar
el título, una línea `BREAKING CHANGE: ...` en el cuerpo de esa misma caja.

```mermaid
flowchart LR
    A[push / squash-merge a main] --> B[CI: ruff check + pytest]
    B -->|falla| X((main queda rojo))
    B -->|ok| C{commit breaking?}
    C -->|no: feat/fix normal| D((main queda verde))
    C -->|sí: feat!/BREAKING CHANGE| E[crear release-v N+1]
    E --> F[(producción lee siempre\nla release-v más alta)]
```

`release-please-config.json` / `.release-please-manifest.json` declaran la
política de versionado (semver por Conventional Commits); en esta
simulación no se invoca la action real de `release-please` — el paso de
arriba reproduce directamente su efecto (detectar breaking → nueva
versión).

---

More structure and convention detail lives in `CLAUDE.md` (gitignored,
local project memory — not needed to run anything above).
