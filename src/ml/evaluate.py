"""Walk-forward validation: MAE/RMSE/MAPE comparison table across models (spec 004)."""

import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)
from sklearn.model_selection import TimeSeriesSplit

from src.ml.models.base import ForecastModel


def _n_splits_for(n_rows: int, requested: int) -> int:
    # ponytail: caps splits so short series (e.g. 2 months of traffic) still
    # get >=2 folds instead of crashing TimeSeriesSplit; revisit once more
    # history is ingested.
    return max(2, min(requested, n_rows // 10, n_rows - 1))


def walk_forward_evaluate(
    models: list[ForecastModel],
    df: pd.DataFrame,
    target_col: str,
    feature_cols: list[str],
    n_splits: int = 3,
) -> pd.DataFrame:
    """Expanding-window evaluation: never trains on data that is temporally after
    what it's tested on. Returns one row per model, sorted best (lowest RMSE) first.
    """
    df = df.sort_values("fecha").reset_index(drop=True)
    features = df[feature_cols]
    y = df[target_col]

    splitter = TimeSeriesSplit(n_splits=_n_splits_for(len(df), n_splits))

    scores = {model.name: {"mae": [], "rmse": [], "mape": []} for model in models}
    for train_idx, test_idx in splitter.split(features):
        x_train, x_test = features.iloc[train_idx], features.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        for model in models:
            model.fit(x_train, y_train)
            preds = model.predict(x_test)
            scores[model.name]["mae"].append(mean_absolute_error(y_test, preds))
            scores[model.name]["rmse"].append(root_mean_squared_error(y_test, preds))
            scores[model.name]["mape"].append(
                mean_absolute_percentage_error(y_test, preds)
            )

    rows = [
        {"model": name, **{metric: np.mean(values) for metric, values in m.items()}}
        for name, m in scores.items()
    ]
    return pd.DataFrame(rows).sort_values("rmse").reset_index(drop=True)


def last_fold_predictions(
    model: ForecastModel,
    df: pd.DataFrame,
    target_col: str,
    feature_cols: list[str],
    partition_col: str,
    n_splits: int = 3,
) -> pd.DataFrame:
    """Real vs. predicted for the most recent walk-forward fold only.

    Fits `model` on everything before that fold (never on the fold itself),
    for an honest "held-out" chart — not the version refit on all data.
    """
    df = df.sort_values("fecha").reset_index(drop=True)
    features = df[feature_cols]
    y = df[target_col]

    splitter = TimeSeriesSplit(n_splits=_n_splits_for(len(df), n_splits))
    train_idx, test_idx = list(splitter.split(features))[-1]

    model.fit(features.iloc[train_idx], y.iloc[train_idx])
    predicted = model.predict(features.iloc[test_idx])

    return pd.DataFrame(
        {
            "fecha": df["fecha"].iloc[test_idx].to_numpy(),
            partition_col: df[partition_col].iloc[test_idx].to_numpy(),
            "actual": y.iloc[test_idx].to_numpy(),
            "predicted": predicted,
        }
    )
