.PHONY: install test ingest pipeline train dashboard airflow-up airflow-down

install:
	@test -d .venv || uv venv
	uv sync

test:
	uv run pytest

# Un dataset/año de ejemplo -- ver data/README.md para el resto de comandos.
ingest:
	uv run python -m src.data.bronze.pipeline --dataset distritos
	uv run python -m src.data.bronze.pipeline --dataset estaciones_aire
	uv run python -m src.data.bronze.pipeline --dataset trafico_puntos_medida --years 2024-12
	uv run python -m src.data.bronze.pipeline --dataset aire --years 2024
	uv run python -m src.data.bronze.pipeline --dataset trafico --years 2024-01

pipeline:
	uv run python -m src.data.run_pipeline

train:
	uv run python -m src.ml.train

dashboard:
	uv run python -m src.dashboard.interface

airflow-up:
	docker compose up -d --build

airflow-down:
	docker compose down
