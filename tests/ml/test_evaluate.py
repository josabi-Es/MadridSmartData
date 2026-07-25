import numpy as np
import pandas as pd

from src.ml.evaluate import last_fold_predictions, walk_forward_evaluate
from src.ml.models.base import ForecastModel

FEATURE_COLS = ["lag_1", "dow"]


def _toy_df(n=60):
    return pd.DataFrame(
        {
            "fecha": pd.date_range("2024-01-01", periods=n),
            "estacion": [4] * n,
            "lag_1": np.arange(n, dtype=float),
            "dow": [i % 7 for i in range(n)],
            "dato": np.arange(n, dtype=float) + 1,
        }
    )


class _PerfectModel(ForecastModel):
    """Cheats by memorising y_test — used to sanity-check metric plumbing."""

    def fit(self, X_train, y_train):  # noqa: N803
        pass

    def predict(self, X_test):  # noqa: N803
        return X_test["lag_1"].to_numpy() + 1

    @property
    def name(self):
        return "perfect"


class _ConstantModel(ForecastModel):
    def fit(self, X_train, y_train):  # noqa: N803
        self._mean = y_train.mean()

    def predict(self, X_test):  # noqa: N803
        return np.full(len(X_test), self._mean)

    @property
    def name(self):
        return "constant"


class _SpyModel(ForecastModel):
    """Records the index range seen at fit/predict time, for leakage checks."""

    def __init__(self):
        self.folds = []

    def fit(self, X_train, y_train):  # noqa: N803
        self._train_max = X_train.index.max()

    def predict(self, X_test):  # noqa: N803
        self.folds.append((self._train_max, X_test.index.min()))
        return np.zeros(len(X_test))

    @property
    def name(self):
        return "spy"


def test_walk_forward_evaluate_returns_one_row_per_model():
    df = _toy_df()
    models = [_PerfectModel(), _ConstantModel()]

    result = walk_forward_evaluate(
        models, df, target_col="dato", feature_cols=FEATURE_COLS
    )

    assert set(result["model"]) == {"perfect", "constant"}
    assert list(result.columns) == ["model", "mae", "rmse", "mape"]


def test_walk_forward_evaluate_ranks_perfect_model_first():
    df = _toy_df()
    models = [_ConstantModel(), _PerfectModel()]

    result = walk_forward_evaluate(
        models, df, target_col="dato", feature_cols=FEATURE_COLS
    )

    assert result.iloc[0]["model"] == "perfect"
    assert result.iloc[0]["rmse"] < 1e-9


def test_walk_forward_evaluate_never_trains_on_future_data():
    df = _toy_df()
    spy = _SpyModel()

    walk_forward_evaluate([spy], df, target_col="dato", feature_cols=FEATURE_COLS)

    assert len(spy.folds) >= 2
    for train_max_idx, test_min_idx in spy.folds:
        assert train_max_idx < test_min_idx


def test_last_fold_predictions_returns_expected_columns():
    df = _toy_df()

    result = last_fold_predictions(
        _ConstantModel(), df, "dato", FEATURE_COLS, partition_col="estacion"
    )

    assert list(result.columns) == ["fecha", "estacion", "actual", "predicted"]


def test_last_fold_predictions_matches_perfect_model_exactly():
    df = _toy_df()

    result = last_fold_predictions(
        _PerfectModel(), df, "dato", FEATURE_COLS, partition_col="estacion"
    )

    assert (result["actual"] == result["predicted"]).all()


def test_last_fold_predictions_never_trains_on_its_own_test_rows():
    df = _toy_df()
    spy = _SpyModel()

    last_fold_predictions(spy, df, "dato", FEATURE_COLS, partition_col="estacion")

    assert len(spy.folds) == 1
    train_max_idx, test_min_idx = spy.folds[0]
    assert train_max_idx < test_min_idx
