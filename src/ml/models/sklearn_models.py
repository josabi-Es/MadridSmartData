"""ForecastModel implementations for v1: baseline, tree, two ensembles, one NN."""

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor

from src.ml.models.base import ForecastModel

RANDOM_STATE = 42


class NaiveModel(ForecastModel):
    """Predicts yesterday's value (`lag_1`) unchanged. No training needed."""

    def fit(self, X_train: pd.DataFrame, y_train: pd.Series) -> None:  # noqa: N803
        pass

    def predict(self, X_test: pd.DataFrame) -> np.ndarray:  # noqa: N803
        return X_test["lag_1"].to_numpy()

    @property
    def name(self) -> str:
        return "naive"


class _SklearnWrapper(ForecastModel):
    """Thin adapter: any sklearn-style regressor with .fit/.predict."""

    def __init__(self, estimator) -> None:
        self._estimator = estimator

    def fit(self, X_train: pd.DataFrame, y_train: pd.Series) -> None:  # noqa: N803
        self._estimator.fit(X_train, y_train)

    def predict(self, X_test: pd.DataFrame) -> np.ndarray:  # noqa: N803
        return self._estimator.predict(X_test)


class DecisionTreeModel(_SklearnWrapper):
    # ponytail: max_depth=8 keeps this cheap for notebook/CI runs; the
    # heavy-grid cell in src/ml/notebooks/decision_tree.ipynb sweeps wider.
    def __init__(self, max_depth: int = 8) -> None:
        super().__init__(
            DecisionTreeRegressor(random_state=RANDOM_STATE, max_depth=max_depth)
        )

    @property
    def name(self) -> str:
        return "decision_tree"


class RandomForestModel(_SklearnWrapper):
    # ponytail: n_estimators/max_depth trimmed from sklearn's 100/unbounded
    # defaults so a full run stays fast on modest hardware; see the
    # commented grid-search cell in src/ml/notebooks/random_forest.ipynb.
    def __init__(self, n_estimators: int = 50, max_depth: int = 10) -> None:
        super().__init__(
            RandomForestRegressor(
                random_state=RANDOM_STATE,
                n_estimators=n_estimators,
                max_depth=max_depth,
            )
        )

    @property
    def name(self) -> str:
        return "random_forest"


class XGBoostModel(_SklearnWrapper):
    # ponytail: trimmed from xgboost's 100 rounds/depth 6 defaults; see the
    # commented grid-search cell in src/ml/notebooks/xgboost.ipynb.
    def __init__(self, n_estimators: int = 50, max_depth: int = 4) -> None:
        super().__init__(
            XGBRegressor(
                random_state=RANDOM_STATE,
                n_estimators=n_estimators,
                max_depth=max_depth,
            )
        )

    @property
    def name(self) -> str:
        return "xgboost"


class MLPModel(_SklearnWrapper):
    # ponytail: one small hidden layer instead of (100,)/1000 iters; see the
    # commented grid-search cell in src/ml/notebooks/mlp.ipynb.
    def __init__(
        self, hidden_layer_sizes: tuple[int, ...] = (16,), max_iter: int = 300
    ) -> None:
        super().__init__(
            MLPRegressor(
                random_state=RANDOM_STATE,
                hidden_layer_sizes=hidden_layer_sizes,
                max_iter=max_iter,
            )
        )

    @property
    def name(self) -> str:
        return "mlp"
