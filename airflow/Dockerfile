FROM apache/airflow:2.10.4-python3.11

RUN pip install --no-cache-dir "duckdb>=1.5.0"
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt" \
    geopandas>=1.0.1 shapely requests>=2.31.0 \
    scikit-learn>=1.5.0 xgboost>=3.2.0 statsmodels>=0.14.0 \
    dotenv>=0.9.9 pyarrow>=24.0.0

ENV PYTHONPATH=/opt/repo
