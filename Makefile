.PHONY: install test ingest pipeline train dashboard airflow-up airflow-down train-dag clean

install:
	@test -d .venv || uv venv
	uv sync --extra pipeline

test:
	uv run pytest

ingest:
	uv run python -m src.data.ingest_api_bronze

pipeline:
	uv run python -m src.data.preprocessing_bronze_gold

train:
	uv run python -m src.ml.main

dashboard:
g
	uv run python -m src.dashboard.interface

airflow-up:
	docker compose up -d --build

airflow-down:
	docker compose down

train-dag: 
	docker compose exec airflow airflow dags trigger retrain_forecast

clean: 
	uv run python -m src.ml.notebooks.clean_cache
