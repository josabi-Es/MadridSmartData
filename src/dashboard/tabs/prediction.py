"""Forecast of the next N months per station. Reads gold, never trains.

`python -m src.ml.main` writes data/gold/ml/pred_<gas>_<N>m.parquet; this tab
only plots it next to the real history it continues.
"""

import json
import re
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd

ML_DIR = Path("data/gold/ml")
FACT_AIR_PATH = "data/gold/fact_calidad_aire.parquet"
DIM_MAGNITUD_PATH = "data/gold/dim_magnitud.parquet"

# ponytail: show the longest horizon on disk. A 1-month and a 2-month run
# coexist as separate files; add a horizon selector only if you want both at once.
_PATRON = re.compile(r"^pred_(?P<gas>.+)_(?P<meses>\d+)m$")


def _runs() -> dict[str, tuple[int, Path]]:
    """gas -> (meses, ruta) for the longest horizon available per gas."""
    encontrados: dict[str, tuple[int, Path]] = {}
    for ruta in ML_DIR.glob("pred_*m.parquet"):
        casa = _PATRON.match(ruta.stem)
        if not casa:
            continue
        gas, meses = casa["gas"], int(casa["meses"])
        if meses > encontrados.get(gas, (0, None))[0]:
            encontrados[gas] = (meses, ruta)
    return encontrados


def gases_disponibles() -> list[str]:
    return sorted(_runs())


def obtener_estaciones_prediccion(gas: str) -> list[int]:
    meses, ruta = _runs()[gas]
    return sorted(pd.read_parquet(ruta)["ID_AIRE"].unique().tolist())


def graficar_prediccion(gas: str, estacion_id):
    meses, ruta = _runs()[gas]
    pred = pd.read_parquet(ruta)
    pred = pred[pred["ID_AIRE"] == int(estacion_id)].sort_values("FECHA")

    real = duckdb.sql(
        f"""
        SELECT a.FECHA AS fecha, a.DATO AS dato
        FROM '{FACT_AIR_PATH}' a
        JOIN '{DIM_MAGNITUD_PATH}' m ON a.ID_MAGNITUD = m.ID_MAGNITUD
        WHERE m.MAGNITUD = ? AND a.ID_AIRE = ? ORDER BY fecha
        """,
        params=[gas, int(estacion_id)],
    ).df()

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(real["fecha"], real["dato"], color="b", linewidth=0.8, label="Real")

    if not real.empty:
        ultima_fecha_real = pd.to_datetime(real["fecha"]).max()
        ultima_dato_real = real[pd.to_datetime(real["fecha"]) == ultima_fecha_real]["dato"].iloc[0]
        pred_continuo_x = pd.concat([
            pd.Series([ultima_fecha_real]),
            pd.to_datetime(pred["FECHA"])
        ]).reset_index(drop=True)
        pred_continuo_y = pd.concat([
            pd.Series([ultima_dato_real]),
            pd.Series(pred["VALOR_PREDICHO"].values)
        ]).reset_index(drop=True)
        ax.plot(
            pred_continuo_x,
            pred_continuo_y,
            color="r",
            linestyle="--",
            label=f"Predicho ({meses} mes/es)",
        )
        ax.axvline(ultima_fecha_real, color="grey", linestyle=":")
    else:
        ax.plot(
            pd.to_datetime(pred["FECHA"]),
            pred["VALOR_PREDICHO"],
            color="r",
            linestyle="--",
            label=f"Predicho ({meses} mes/es)",
        )

    ax.set_title(f"{gas} — estación {estacion_id} — histórico y predicción")
    ax.set_xlabel("Fecha")
    ax.set_ylabel(gas)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate(rotation=45)
    return fig


def metricas_texto(gas: str) -> str:
    meses, _ = _runs()[gas]
    with open(ML_DIR / f"metrics_{gas}_{meses}m.json", encoding="utf-8") as f:
        datos = json.load(f)

    tabla = "\n".join(
        f"| {fila['modelo']} | {fila['mae']:.2f} | {fila['rmse']:.2f} |"
        for fila in datos["comparativa"]
    )
    return (
        f"**Ganador:** `{datos['ganador']}` — horizonte {meses} mes(es) "
        f"desde {datos['ultima_fecha_real']}\n\n"
        f"| modelo | MAE | RMSE |\n|---|---|---|\n{tabla}"
    )
