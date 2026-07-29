"""Baseline: what this station usually reads on this weekday.

Not here to win -- here to be the yardstick. A model that can't beat "the mean
of the last 4 Tuesdays" is not learning anything worth the compute.
"""

import numpy as np
import pandas as pd

from src.ml.models.base import Modelo

VENTANA = 4  # ultimas N apariciones del mismo dia de la semana


class SeasonalNaive(Modelo):
    name = "seasonal_naive"

    def fit(self, historico: pd.DataFrame) -> None:
        ultimos = (
            historico.sort_values("fecha")
            .groupby(["estacion", "dow"])["dato"]
            .apply(lambda s: s.tail(VENTANA).mean())
        )
        self._tabla = ultimos
        self._global = historico["dato"].mean()

    def predict(self, futuro: pd.DataFrame) -> np.ndarray:
        claves = pd.MultiIndex.from_arrays([futuro["estacion"], futuro["dow"]])
        return self._tabla.reindex(claves).fillna(self._global).to_numpy()
