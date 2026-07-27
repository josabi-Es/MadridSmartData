# Airflow image with just what the DAGs actually import (src/data/*,
# src/ml/*) -- NOT the full pyproject.toml. gradio/fastapi (dashboard-only
# deps, never imported by a DAG) drag in an anyio pin that conflicts with
# Airflow's own constraints file and breaks the build; the DAGs don't need
# them, so they're left out here instead of fighting the resolver.

FROM apache/airflow:2.10.4-python3.11

# Airflow ships a curated dependency set; installing extra packages without
# its constraints file is the #1 way to break the image (see Airflow docs
# "Installing extra packages"). Keep --constraint even if it looks
# redundant. Exception: duckdb -- the constraints file pins it to 1.1.3,
# which doesn't support the `encoding=` kwarg on read_csv_auto that
# src/data/bronze/pipeline.py relies on (added in a later duckdb release).
# Installed separately, unconstrained, before the rest.
RUN pip install --no-cache-dir "duckdb>=1.5.0"
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt" \
    geopandas>=1.0.1 shapely requests>=2.31.0 \
    scikit-learn>=1.5.0 xgboost>=3.2.0 joblib>=1.5.3 dotenv>=0.9.9 pyarrow>=24.0.0

# papermill/ipykernel: needed by src/ml/notebooks/run_one.py to execute the
# 5 per-model notebooks from the retrain_forecast DAG. matplotlib: the
# notebooks plot EDA/holdout charts (it's a base dep in pyproject.toml for
# the dashboard, but this image only installs from a hand-picked list, so
# it needs to be listed here too). Same constraint URL as above -- resolved
# cleanly against it in local `uv sync` (no gradio/fastapi in this image to
# collide with, see comment above).
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt" \
    papermill>=2.6.0 ipykernel>=6.29.0 matplotlib==3.9.4
RUN python -m ipykernel install --sys-prefix --name python3

ENV PYTHONPATH=/opt/repo
