"""CLI: compare the 5 v1 models per variable, save the winner (spec 004).

Usage: python -m src.ml.train
"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.ml.evaluate import walk_forward_evaluate
from src.ml.features import build_air_features, build_traffic_features
from src.ml.models.sklearn_models import (
    DecisionTreeModel,
    MLPModel,
    NaiveModel,
    RandomForestModel,
    XGBoostModel,
)
from src.ml.promote import refit_and_persist

load_dotenv()

AIRQUALITY_PATH = os.getenv("DATA_AIRQUALITY_PATH", "data/silver/aire/all.parquet")
TRAFFIC_PATH = os.getenv("DATA_TRAFFIC_PATH", "data/silver/trafico/all.parquet")
MODELS_DIR = os.getenv("ML_MODELS_DIR", "data/gold")

MODEL_CLASSES = [
    NaiveModel, DecisionTreeModel, RandomForestModel, XGBoostModel, MLPModel
]  # fmt: skip

CALENDAR_COLS = ["lag_1", "lag_7", "lag_30", "roll_mean_7", "dow", "mes", "is_weekend"]
AIR_FEATURE_COLS = ["estacion", *CALENDAR_COLS]
TRAFFIC_FEATURE_COLS = ["id", *CALENDAR_COLS]

AIR_VARIABLES = ["NO2", "PM10", "PM2.5"]
TRAFFIC_VARIABLES = ["intensidad"]

FORECAST_HORIZON = 365


def _train_and_save(
    variable: str,
    df: pd.DataFrame,
    target_col: str,
    feature_cols: list[str],
    partition_col: str,
) -> tuple[pd.DataFrame, str, Path]:
    """Compare the 5 models on `df`, refit the winner on all of it, save with joblib.

    Also persists the comparison table and a held-out real-vs-predicted table
    (spec 005 reads these instead of recomputing them from the tab). The
    refit/persist tail is shared with the Airflow DAG's promotion task via
    `src.ml.promote.refit_and_persist` -- single source of truth for the
    winner-selection criterion.
    """
    comparison = walk_forward_evaluate(
        [cls() for cls in MODEL_CLASSES], df, target_col, feature_cols
    )
    return refit_and_persist(
        variable,
        df,
        target_col,
        feature_cols,
        partition_col,
        comparison,
        MODELS_DIR,
        horizon=FORECAST_HORIZON,
    )


def main() -> dict[str, tuple[pd.DataFrame, str, Path]]:
    results = {}
    for gas in AIR_VARIABLES:
        df = build_air_features(AIRQUALITY_PATH, magnitud=gas)
        results[gas] = _train_and_save(gas, df, "dato", AIR_FEATURE_COLS, "estacion")

    for variable in TRAFFIC_VARIABLES:
        df = build_traffic_features(TRAFFIC_PATH, variable=variable)
        results[variable] = _train_and_save(
            variable, df, variable, TRAFFIC_FEATURE_COLS, "id"
        )

    for variable, (comparison, winner_name, out_path) in results.items():
        print(f"\n{variable}\n{comparison}\nGanador: {winner_name} -> {out_path}")

    return results


if __name__ == "__main__":
    main()
