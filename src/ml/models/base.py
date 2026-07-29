"""The contract every model implements.

Deliberately tiny: `fit` receives the feature table as-is and each model
reshapes it however it needs. That's the whole point of one file per model --
XGBoost wants the wide table with all 24 stations at once, SARIMAX wants a
univariate series per station. Keeping that inside the model file means adding
a fourth model never touches `main.py`.
"""

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class Modelo(ABC):
    name: str

    @abstractmethod
    def fit(self, historico: pd.DataFrame) -> None:
        """`historico`: station, cod_dis, fecha, dato + the columns in FEATURE_COLS."""

    @abstractmethod
    def predict(self, futuro: pd.DataFrame) -> np.ndarray:
        """`futuro`: same columns minus `dato`. One value per row, in row order."""
