import json

import joblib
import numpy as np
import pandas as pd

from src.ml.train import _train_and_save


def _toy_df(n=60):
    return pd.DataFrame(
        {
            "fecha": pd.date_range("2024-01-01", periods=n),
            "estacion": [4] * n,
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


FEATURE_COLS = [
    "estacion", "lag_1", "lag_7", "lag_30", "roll_mean_7", "dow", "mes", "is_weekend"
]  # fmt: skip


def test_train_and_save_writes_a_loadable_model(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ml.train.MODELS_DIR", str(tmp_path))

    comparison, winner_name, out_path = _train_and_save(
        "NO2", _toy_df(), "dato", FEATURE_COLS, partition_col="estacion"
    )

    assert winner_name in comparison["model"].to_numpy()
    assert out_path.exists()
    loaded = joblib.load(out_path)
    assert loaded.name == winner_name


def test_train_and_save_winner_is_refit_on_full_data(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ml.train.MODELS_DIR", str(tmp_path))
    df = _toy_df()

    _, _, out_path = _train_and_save(
        "NO2", df, "dato", FEATURE_COLS, partition_col="estacion"
    )

    winner = joblib.load(out_path)
    preds = winner.predict(df[FEATURE_COLS])
    assert len(preds) == len(df)


def test_train_and_save_writes_metrics_json(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ml.train.MODELS_DIR", str(tmp_path))

    comparison, winner_name, _ = _train_and_save(
        "NO2", _toy_df(), "dato", FEATURE_COLS, partition_col="estacion"
    )

    metrics_path = tmp_path / "NO2_metrics.json"
    assert metrics_path.exists()
    saved = json.loads(metrics_path.read_text())
    assert saved["winner"] == winner_name
    assert len(saved["comparison"]) == len(comparison)


def test_train_and_save_writes_holdout_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ml.train.MODELS_DIR", str(tmp_path))

    _train_and_save(
        "NO2", _toy_df(), "dato", FEATURE_COLS, partition_col="estacion"
    )

    holdout_path = tmp_path / "NO2_holdout.parquet"
    assert holdout_path.exists()
    holdout = pd.read_parquet(holdout_path)
    assert list(holdout.columns) == ["fecha", "estacion", "actual", "predicted"]
    assert len(holdout) > 0
