"""Real vs. predicted (held-out fold) for the fase-4 winner model. No retraining here."""

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

MODELS_DIR = "data/gold"


def _latest_year(variable):
    """Newest year available for `variable`, so a retrain with more history
    never gets shadowed by an older path hardcoded somewhere."""
    years = [
        int(p.stem.removeprefix(f"ml_{variable}_").removesuffix("_holdout"))
        for p in Path(MODELS_DIR).glob(f"ml_{variable}_*_holdout.parquet")
    ]
    return max(years)


def _holdout(variable):
    stem = f"ml_{variable}_{_latest_year(variable)}"
    df = pd.read_parquet(Path(MODELS_DIR) / f"{stem}_holdout.parquet")
    partition_col = next(
        c for c in df.columns if c not in ("fecha", "actual", "predicted")
    )
    return df, partition_col


def obtener_estaciones_prediccion(variable):
    df, partition_col = _holdout(variable)
    return sorted(df[partition_col].unique().tolist())


def graficar_prediccion(variable, estacion_id):
    df, partition_col = _holdout(variable)
    df = df[df[partition_col] == estacion_id].sort_values("fecha")

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(df["fecha"], df["actual"], linestyle="-", color="b", label="Real")
    ax.plot(df["fecha"], df["predicted"], linestyle="--", color="r", label="Predicho")
    ax.set_title(f"Real vs. predicho — {variable} — {partition_col} {estacion_id}")
    ax.set_xlabel("Fecha")
    ax.set_ylabel(variable)
    ax.legend()
    ax.grid(True)
    fig.autofmt_xdate(rotation=45)

    return fig


def metricas_texto(variable):
    stem = f"ml_{variable}_{_latest_year(variable)}"
    with open(Path(MODELS_DIR) / f"{stem}_metrics.json", encoding="utf-8") as f:
        data = json.load(f)
    winner = data["winner"]
    row = next(r for r in data["comparison"] if r["model"] == winner)
    return (
        f"**Ganador:** {winner} — MAE {row['mae']:.2f}, "
        f"RMSE {row['rmse']:.2f}, MAPE {row['mape']:.2%}"
    )
