"""Gradient boosting over the legal features, one global fit per gas.

`estacion` goes in as a plain numeric column: the trees split on it, which
lets one model hold 24 station-specific levels without 24 separate fits.
"""

import numpy as np
import pandas as pd
from xgboost import XGBRegressor

from src.ml.core.features import FEATURE_COLS
from src.ml.models.base import Modelo

# ponytail: sane defaults, no grid search. Tuning these is editing this dict;
# add a search only once the backtest says boosting is actually competitive.
PARAMS = {
    "n_estimators": 300,
    "max_depth": 5,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "random_state": 42,
}


class XGBoost(Modelo):
    name = "xgboost"

    def fit(self, historico: pd.DataFrame) -> None:
        self._model = XGBRegressor(**PARAMS)
        self._model.fit(historico[FEATURE_COLS], historico["dato"])

    def predict(self, futuro: pd.DataFrame) -> np.ndarray:
        return self._model.predict(futuro[FEATURE_COLS])
