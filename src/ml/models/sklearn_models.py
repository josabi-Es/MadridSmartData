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
    def __init__(self) -> None:
        super().__init__(DecisionTreeRegressor(random_state=RANDOM_STATE))

    @property
    def name(self) -> str:
        return "decision_tree"


class RandomForestModel(_SklearnWrapper):
    def __init__(self) -> None:
        super().__init__(RandomForestRegressor(random_state=RANDOM_STATE))

    @property
    def name(self) -> str:
        return "random_forest"


class XGBoostModel(_SklearnWrapper):
    def __init__(self) -> None:
        super().__init__(XGBRegressor(random_state=RANDOM_STATE))

    @property
    def name(self) -> str:
        return "xgboost"


class MLPModel(_SklearnWrapper):
    def __init__(self) -> None:
        super().__init__(MLPRegressor(random_state=RANDOM_STATE, max_iter=1000))

    @property
    def name(self) -> str:
        return "mlp"
