"""Real vs. predicted (held-out fold) for the fase-4 winner model. No retraining here."""

import json
import os

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MODELS_DIR = os.getenv("ML_MODELS_DIR", "data/models")


def _holdout(variable):
    df = pd.read_parquet(f"{MODELS_DIR}/{variable}_holdout.parquet")
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
    with open(f"{MODELS_DIR}/{variable}_metrics.json", encoding="utf-8") as f:
        data = json.load(f)
    winner = data["winner"]
    row = next(r for r in data["comparison"] if r["model"] == winner)
    return (
        f"**Ganador:** {winner} — MAE {row['mae']:.2f}, "
        f"RMSE {row['rmse']:.2f}, MAPE {row['mape']:.2%}"
    )
