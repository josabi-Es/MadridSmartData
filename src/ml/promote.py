"""Shared winner-selection/persist logic between train.py and the Airflow DAG.

Both the local CLI (in-memory comparison, `df` already built) and the DAG's
final task (on-disk per-model comparison rows written by 5 notebooks, `df`
rebuilt from scratch) must agree on which model wins -- this module is the
single place that decides, so the criterion can't drift between the two.
"""

import json
import os
from pathlib import Path

import joblib
import pandas as pd

from src.ml.evaluate import last_fold_predictions
from src.ml.features import build_air_features, build_traffic_features
from src.ml.forecast import recursive_forecast
from src.ml.models.sklearn_models import (
    DecisionTreeModel,
    MLPModel,
    NaiveModel,
    RandomForestModel,
    XGBoostModel,
)

FORECAST_HORIZON_DEFAULT = 365

MODEL_REGISTRY = {
    cls().name: cls
    for cls in [
        NaiveModel,
        DecisionTreeModel,
        RandomForestModel,
        XGBoostModel,
        MLPModel,
    ]
}

CALENDAR_COLS = ["lag_1", "lag_7", "lag_30", "roll_mean_7", "dow", "mes", "is_weekend"]
AIR_FEATURE_COLS = ["estacion", *CALENDAR_COLS]
TRAFFIC_FEATURE_COLS = ["id", *CALENDAR_COLS]

AIR_VARIABLES = ["NO2", "PM10", "PM2.5"]
TRAFFIC_VARIABLES = ["intensidad"]


def _variable_config(
    variable: str, air_path: str, traffic_path: str
) -> tuple[pd.DataFrame, str, list[str], str]:
    """variable -> (df, target_col, feature_cols, partition_col)."""
    if variable in AIR_VARIABLES:
        return (
            build_air_features(air_path, magnitud=variable),
            "dato",
            AIR_FEATURE_COLS,
            "estacion",
        )
    return (
        build_traffic_features(traffic_path, variable=variable),
        variable,
        TRAFFIC_FEATURE_COLS,
        "id",
    )


def refit_and_persist(
    variable: str,
    df: pd.DataFrame,
    target_col: str,
    feature_cols: list[str],
    partition_col: str,
    comparison: pd.DataFrame,
    models_dir: str,
    horizon: int = FORECAST_HORIZON_DEFAULT,
) -> tuple[pd.DataFrame, str, Path]:
    """Refit the comparison's winner on all of `df`, persist the 4 artifacts."""
    winner_name = comparison.iloc[0]["model"]
    winner_cls = MODEL_REGISTRY[winner_name]

    winner = winner_cls()
    winner.fit(df[feature_cols], df[target_col])

    year = int(df["fecha"].max().year)
    stem = f"ml_{variable}_{year}"

    Path(models_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(models_dir) / f"{stem}.joblib"
    joblib.dump(winner, out_path)

    metrics_path = Path(models_dir) / f"{stem}_metrics.json"
    metrics_path.write_text(
        json.dumps({"winner": winner_name, "comparison": comparison.to_dict("records")})
    )

    holdout = last_fold_predictions(
        winner_cls(), df, target_col, feature_cols, partition_col
    )
    holdout.to_parquet(Path(models_dir) / f"{stem}_holdout.parquet")

    future = recursive_forecast(
        winner, df, target_col, feature_cols, partition_col, horizon=horizon
    )
    future.to_parquet(Path(models_dir) / f"{stem}_future.parquet")

    return comparison, winner_name, out_path


def promote_winner(
    variable: str,
    ano: int,
    runs_dir: str,
    models_dir: str,
    air_path: str | None = None,
    traffic_path: str | None = None,
    df_builder=_variable_config,
) -> tuple[pd.DataFrame, str, Path]:
    """Read gold/ml_runs/<variable>_*_<ano>.parquet, pick the lowest-rmse model.

    Tolerates a partial set of run files (some notebooks may have failed
    upstream) -- promotes among whatever is present.
    """
    air_path = air_path or os.getenv(
        "DATA_AIRQUALITY_PATH", "data/silver/aire/all.parquet"
    )
    traffic_path = traffic_path or os.getenv(
        "DATA_TRAFFIC_PATH", "data/silver/trafico/all.parquet"
    )

    run_files = sorted(Path(runs_dir).glob(f"{variable}_*_{ano}.parquet"))
    if not run_files:
        raise FileNotFoundError(f"no run files for {variable} {ano} in {runs_dir}")

    comparison = (
        pd.concat([pd.read_parquet(f) for f in run_files], ignore_index=True)
        .sort_values("rmse")
        .reset_index(drop=True)
    )

    df, target_col, feature_cols, partition_col = df_builder(
        variable, air_path, traffic_path
    )
    return refit_and_persist(
        variable, df, target_col, feature_cols, partition_col, comparison, models_dir
    )
