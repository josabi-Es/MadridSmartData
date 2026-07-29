"""Rolling-origin backtest: rank the models under the conditions they'll run in.

Not a generic k-fold: every fold trains up to a cutoff and predicts exactly the
next H days, which is literally what `main.py` asks of the winner afterwards.
Averaging over a few origins stops one lucky (or freakish) two-month window
from picking the model.
"""

import numpy as np
import pandas as pd

from src.ml.models import REGISTRY


def _metricas(real: np.ndarray, pred: np.ndarray) -> tuple[float, float]:
    error = real - pred
    return float(np.abs(error).mean()), float(np.sqrt((error**2).mean()))


def rankear(
    tabla_train: pd.DataFrame, horizonte_dias: int, n_origenes: int = 3
) -> pd.DataFrame:
    """One row per model with mean MAE/RMSE across origins, best RMSE first."""
    fin = tabla_train["fecha"].max()
    paso = pd.Timedelta(days=horizonte_dias)

    scores: dict[str, list[tuple[float, float]]] = {n: [] for n in REGISTRY}

    for k in range(n_origenes):
        fin_test = fin - k * paso
        ini_test = fin_test - paso + pd.Timedelta(days=1)

        train = tabla_train[tabla_train["fecha"] < ini_test]
        test = tabla_train[
            tabla_train["fecha"].between(ini_test, fin_test)
        ]
        if test.empty or train["fecha"].nunique() < horizonte_dias:
            break  # not enough history left for another origin

        for nombre, cls in REGISTRY.items():
            modelo = cls()
            modelo.fit(train)
            scores[nombre].append(
                _metricas(test["dato"].to_numpy(), modelo.predict(test))
            )

    filas = [
        {
            "modelo": nombre,
            "mae": float(np.mean([m for m, _ in vals])),
            "rmse": float(np.mean([r for _, r in vals])),
            "origenes": len(vals),
        }
        for nombre, vals in scores.items()
        if vals
    ]
    if not filas:
        raise ValueError("no hay histórico suficiente para ni un solo origen")

    return pd.DataFrame(filas).sort_values("rmse").reset_index(drop=True)
