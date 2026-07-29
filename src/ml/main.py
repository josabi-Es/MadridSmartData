"""Forecast the next N months of air quality per station, into gold.

The only executable in src/ml/:  python -m src.ml.main

Reads PREDICT_MONTHS and GASES from .env; everything else (hyperparameters,
which models exist) lives next to the code that uses it.
"""

import os

import pandas as pd
from dotenv import load_dotenv

from src.ml.core.backtest import rankear
from src.ml.core.features import build
from src.ml.core.gold import escribir_prediccion, leer_historico
from src.ml.models import REGISTRY

load_dotenv()

# Beyond a year the lag_>=H features would need more history than exists and
# the model would fall back to bare calendar -- a straight face on a guess.
MAX_MESES = 12


def _horizonte_dias(ultima_fecha: pd.Timestamp, meses: int) -> int:
    """Calendar months, not 30-day blocks: 2 months from 30-Nov is 31-Jan."""
    return (ultima_fecha + pd.DateOffset(months=meses) - ultima_fecha).days


def predecir(gas: str, meses: int) -> pd.DataFrame:
    historico = leer_historico(gas)
    if historico.empty:
        raise ValueError(f"sin datos para {gas!r} en fact_calidad_aire")

    ultima_real = pd.to_datetime(historico["fecha"]).max()
    horizonte = _horizonte_dias(ultima_real, meses)

    train, futuro = build(historico, horizonte)
    ranking = rankear(train, horizonte)

    nombre_ganador = ranking.iloc[0]["modelo"]
    ganador = REGISTRY[nombre_ganador]()
    ganador.fit(train)

    prediccion = pd.DataFrame(
        {
            "fecha": futuro["fecha"].dt.date,
            "estacion": futuro["estacion"],
            "cod_dis": futuro["cod_dis"],
            "magnitud": gas,
            "valor_predicho": ganador.predict(futuro),
            "modelo": nombre_ganador,
            "horizonte_meses": meses,
        }
    )

    salida = escribir_prediccion(gas, meses, prediccion, ranking, ultima_real.date())
    print(f"\n=== {gas} | {meses} mes(es) | ultimo real {ultima_real.date()} ===")
    print(ranking.to_string(index=False))
    print(f"ganador: {nombre_ganador} -> {salida}")
    return prediccion


def main() -> None:
    meses = int(os.getenv("PREDICT_MONTHS", "2"))
    if not 1 <= meses <= MAX_MESES:
        raise ValueError(f"PREDICT_MONTHS debe estar entre 1 y {MAX_MESES}, no {meses}")

    gases = [g.strip() for g in os.getenv("GASES", "NO2,O3").split(",") if g.strip()]
    for gas in gases:
        predecir(gas, meses)


if __name__ == "__main__":
    main()
