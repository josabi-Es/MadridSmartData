"""Read the gold fact, write the prediction back into gold.

The only module that knows where data lives on disk, and the only one that
translates between gold's on-disk column names (uppercase, homogenized:
ID_AIRE, COD_DIS, ID_MAGNITUD, FECHA, DATO) and the lowercase Spanish names
the rest of `src/ml/` works with internally (`estacion`, `cod_dis`, `fecha`,
`dato`...). Nothing else in `src/ml/` needs to know gold's schema changed.
"""

import json
import os
from pathlib import Path

import duckdb
import pandas as pd

FACT_AIR_PATH = os.getenv("GOLD_FACT_AIR_PATH", "data/gold/fact_calidad_aire.parquet")
DIM_MAGNITUD_PATH = os.getenv(
    "GOLD_DIM_MAGNITUD_PATH", "data/gold/dim_magnitud.parquet"
)
ML_DIR = os.getenv("GOLD_ML_DIR", "data/gold/ml")


def leer_historico(
    gas: str, path: str = FACT_AIR_PATH, dim_magnitud_path: str = DIM_MAGNITUD_PATH
) -> pd.DataFrame:
    """Daily readings of one gas, one row per (estacion, fecha).

    FACT_CALIDAD_AIRE already comes filtered to valid readings and joins
    on ID_MAGNITUD (numeric FK) rather than the gas name, so this resolves
    `gas` to its ID_MAGNITUD via DIM_MAGNITUD first.
    """
    return duckdb.sql(
        f"""
        SELECT a.ID_AIRE AS estacion, a.COD_DIS AS cod_dis,
               a.FECHA AS fecha, a.DATO AS dato
        FROM '{path}' a
        JOIN '{dim_magnitud_path}' m ON a.ID_MAGNITUD = m.ID_MAGNITUD
        WHERE m.MAGNITUD = ?
        ORDER BY estacion, fecha
        """,
        params=[gas],
    ).df()


def _stem(gas: str, meses: int) -> str:
    return f"pred_{gas}_{meses}m"


def _id_magnitud(gas: str, dim_magnitud_path: str = DIM_MAGNITUD_PATH) -> int:
    return int(
        duckdb.sql(
            f"SELECT ID_MAGNITUD FROM '{dim_magnitud_path}' WHERE MAGNITUD = ?",
            params=[gas],
        ).fetchone()[0]
    )


def escribir_prediccion(
    gas: str,
    meses: int,
    prediccion: pd.DataFrame,
    ranking: pd.DataFrame,
    ultima_fecha_real,
    ml_dir: str = ML_DIR,
) -> Path:
    """Persist the winner's forecast plus the model comparison that chose it.

    `prediccion` comes in with `src/ml/main.py`'s internal lowercase columns
    (fecha, estacion, cod_dis, magnitud, valor_predicho, modelo,
    horizonte_meses) -- this is the only place that translates them to
    gold's on-disk schema: uppercase, ID_MAGNITUD (numeric FK) instead of
    the magnitud text, and HORIZONTE_MESES dropped (it's already in the
    filename, PRED_<gas>_<N>m).
    """
    Path(ml_dir).mkdir(parents=True, exist_ok=True)

    salida = pd.DataFrame(
        {
            "FECHA": prediccion["fecha"],
            "ID_AIRE": prediccion["estacion"],
            "COD_DIS": prediccion["cod_dis"],
            "ID_MAGNITUD": _id_magnitud(gas),
            "VALOR_PREDICHO": prediccion["valor_predicho"],
            "MODELO": prediccion["modelo"],
        }
    )

    out_path = Path(ml_dir) / f"{_stem(gas, meses)}.parquet"
    salida.to_parquet(out_path, index=False)

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
