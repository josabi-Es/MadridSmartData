"""Read the gold fact, write the prediction back into gold.

The only module that knows where data lives on disk. `fact_calidad_aire`
already carries `cod_dis` (the station->district join is materialised there),
so nothing here needs to touch silver or a dimension table.
"""

import json
import os
from pathlib import Path

import duckdb
import pandas as pd

FACT_AIR_PATH = os.getenv("GOLD_FACT_AIR_PATH", "data/gold/fact_calidad_aire.parquet")
ML_DIR = os.getenv("GOLD_ML_DIR", "data/gold/ml")


def leer_historico(gas: str, path: str = FACT_AIR_PATH) -> pd.DataFrame:
    """Daily readings of one gas, one row per (estacion, fecha).

    `validez` is not filtered here: silver already drops everything but 'V'.
    """
    return duckdb.sql(
        f"""
        SELECT estacion, cod_dis, fecha, dato
        FROM '{path}'
        WHERE magnitud = ?
        ORDER BY estacion, fecha
        """,
        params=[gas],
    ).df()


def _stem(gas: str, meses: int) -> str:
    return f"pred_{gas}_{meses}m"


def escribir_prediccion(
    gas: str,
    meses: int,
    prediccion: pd.DataFrame,
    ranking: pd.DataFrame,
    ultima_fecha_real,
    ml_dir: str = ML_DIR,
) -> Path:
    """Persist the winner's forecast plus the model comparison that chose it."""
    Path(ml_dir).mkdir(parents=True, exist_ok=True)

    out_path = Path(ml_dir) / f"{_stem(gas, meses)}.parquet"
    prediccion.to_parquet(out_path, index=False)

    metrics_path = Path(ml_dir) / f"metrics_{gas}_{meses}m.json"
    metrics_path.write_text(
        json.dumps(
            {
                "gas": gas,
                "horizonte_meses": meses,
                "ultima_fecha_real": str(ultima_fecha_real),
                "ganador": ranking.iloc[0]["modelo"],
                "comparativa": ranking.to_dict("records"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return out_path
