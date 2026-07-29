"""SARIMAX, one univariate fit per station.

Different shape of input from the other two: this one ignores the feature
columns entirely and eats the raw daily series, because the seasonality and
autocorrelation it models live in the series itself.

Expectation check: with s=7 it captures the weekly cycle but not the annual
one (s=365 is not estimable, and there are barely two annual cycles in the
data anyway). Past ~3 weeks its forecast decays towards a trend line with a
weekly ripple. It's here as a contrast, not as the favourite.
"""

import warnings

import numpy as np
import pandas as pd
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from statsmodels.tsa.statespace.sarimax import SARIMAX

from src.ml.models.base import Modelo

PARAMS = {
    "order": (1, 1, 1),
    "seasonal_order": (1, 1, 1, 7),
    "enforce_stationarity": False,
    "enforce_invertibility": False,
}

# ponytail: one fit per station per backtest origin (~24 x 4 per gas). Fine on
# a laptop at a few seconds each; if the station count grows, cache fits or
# drop to a single pooled model.
MIN_OBS = 60

# With seasonal differencing on a series that also trends annually, SARIMAX
# sometimes diverges outright -- O3 produced forecasts of 1e17 ug/m3. A gas
# concentration is non-negative and bounded by physics, so clamp to the range
# the station has actually seen. Better a flat wrong answer than a wrong answer
# that wins the backtest by being absurd in the other direction.
TECHO_FACTOR = 2.0


class Sarimax(Modelo):
    name = "sarimax"

    def fit(self, historico: pd.DataFrame) -> None:
        self._fits: dict = {}
        self._medias: dict = {}
        self._fin: dict = {}
        self._techo: dict = {}

        for estacion, grupo in historico.groupby("estacion"):
            serie = (
                grupo.set_index("fecha")["dato"].sort_index().asfreq("D")
            )  # gaps stay NaN; the Kalman filter handles them
            self._medias[estacion] = serie.mean()
            self._fin[estacion] = serie.index.max()
            self._techo[estacion] = serie.max() * TECHO_FACTOR

            if serie.notna().sum() < MIN_OBS:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", ConvergenceWarning)
                warnings.simplefilter("ignore", UserWarning)
                try:
                    self._fits[estacion] = SARIMAX(serie, **PARAMS).fit(disp=False)
                except (ValueError, np.linalg.LinAlgError):
                    pass  # falls back to the station mean in predict()

    def predict(self, futuro: pd.DataFrame) -> np.ndarray:
        salida = pd.Series(np.nan, index=futuro.index)

        for estacion, grupo in futuro.groupby("estacion"):
            media = self._medias.get(estacion, np.nan)
            ajuste = self._fits.get(estacion)
            if ajuste is None:
                salida.loc[grupo.index] = media
                continue

            pasos = (grupo["fecha"].max() - self._fin[estacion]).days
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pronostico = ajuste.forecast(steps=pasos)
            salida.loc[grupo.index] = np.clip(
                pronostico.reindex(grupo["fecha"]).to_numpy(),
                0,
                self._techo[estacion],
            )

        return salida.fillna(np.nanmean(list(self._medias.values()))).to_numpy()
