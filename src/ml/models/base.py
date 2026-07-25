"""Common interface every forecasting model implements (spec 004)."""

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class ForecastModel(ABC):
    @abstractmethod
    def fit(self, X_train: pd.DataFrame, y_train: pd.Series) -> None: ...  # noqa: N803

    @abstractmethod
    def predict(self, X_test: pd.DataFrame) -> np.ndarray: ...  # noqa: N803

    @property
    @abstractmethod
    def name(self) -> str: ...
