import numpy as np
import pandas as pd

from src.ml.forecast import recursive_forecast
from src.ml.models.sklearn_models import NaiveModel

FEATURE_COLS = [
    "estacion", "lag_1", "lag_7", "lag_30", "roll_mean_7", "dow", "mes", "is_weekend"
]  # fmt: skip


def _toy_df(partitions=("A", "B"), n=40, start="2024-01-01"):
    frames = [
        pd.DataFrame(
            {
                "fecha": pd.date_range(start, periods=n),
                "estacion": p,
                "lag_1": np.arange(n, dtype=float),
                "lag_7": np.arange(n, dtype=float),
                "lag_30": np.arange(n, dtype=float),
                "roll_mean_7": np.arange(n, dtype=float),
                "dow": [i % 7 for i in range(n)],
                "mes": [1] * n,
                "is_weekend": [0] * n,
                "dato": np.arange(n, dtype=float) + 1,
            }
        )
        for p in partitions
    ]
    return pd.concat(frames, ignore_index=True)


class _WeekendModel:
    """Returns 100 on weekend features, 0 otherwise -- probes the dow conversion."""

    def predict(self, X_test):  # noqa: N803
        return np.where(X_test["is_weekend"] == 1, 100.0, 0.0)


class _Lag30Model:
    """Echoes lag_30 -- probes that the buffer is seeded from real history."""

    def predict(self, X_test):  # noqa: N803
        return X_test["lag_30"].to_numpy()


def test_recursive_forecast_shape_and_consecutive_dates():
    df = _toy_df()

    result = recursive_forecast(
        NaiveModel(), df, "dato", FEATURE_COLS, "estacion", horizon=10
    )

    assert len(result) == 10 * 2
    assert list(result.columns) == ["fecha", "estacion", "predicted"]
    last_date = df["fecha"].max()
    expected_dates = set(pd.date_range(last_date + pd.Timedelta(days=1), periods=10))
    assert set(result["fecha"].unique()) == expected_dates


def test_recursive_forecast_naive_model_holds_last_value_constant():
    df = _toy_df(partitions=("A",))

    result = recursive_forecast(
        NaiveModel(), df, "dato", FEATURE_COLS, "estacion", horizon=5
    )

    last_actual = df.sort_values("fecha")["dato"].iloc[-1]
    assert (result["predicted"] == last_actual).all()


def test_recursive_forecast_matches_calendar_weekends():
    df = _toy_df(partitions=("A",))

    result = recursive_forecast(
        _WeekendModel(), df, "dato", FEATURE_COLS, "estacion", horizon=14
    ).sort_values("fecha")

    es_finde = result["fecha"].dt.dayofweek.isin([5, 6])
    assert (result.loc[es_finde, "predicted"] == 100.0).all()
    assert (result.loc[~es_finde, "predicted"] == 0.0).all()


def test_recursive_forecast_seeds_lag_30_from_real_history():
    df = _toy_df(partitions=("A",)).sort_values("fecha").reset_index(drop=True)

    result = recursive_forecast(
        _Lag30Model(), df, "dato", FEATURE_COLS, "estacion", horizon=1
    )

    assert result["predicted"].iloc[0] == df["dato"].iloc[-30]
