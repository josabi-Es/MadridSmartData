"""Feature tables for direct (non-recursive) multi-step forecasting.

The whole design turns on one rule: a feature is *legal* only if it can be
computed at prediction time. For a horizon of H days that leaves exactly two
families:

* calendar (dow/month/day-of-year) -- deterministic for any future date;
* lags of >= H days -- `lag_H` of `hoy + H` is the value of `hoy`, already
  observed. A `lag_30` at H=60 would fall on `hoy + 30`, which has not
  happened yet, so it is banned.

With only these, the whole horizon is predicted in one `predict()` call
instead of feeding the model its own output day by day -- no compounding
error, no flattening towards the mean.
"""

import duckdb
import numpy as np
import pandas as pd

# ponytail: offsets added to H, so every lag stays >= H and therefore legal.
# lag_365 (same day last year) would model the annual cycle far better, but it
# needs a full year of history before each row and there are only 23 months --
# it would halve the training set. Add it once 3+ years are ingested.
LAG_OFFSETS = [0, 7, 30]

_ROLLING_DAYS = 30

FEATURE_COLS = [
    "estacion",
    *[f"lag_{off}" for off in LAG_OFFSETS],
    "roll_mean",
    "dow",
    "mes",
    "doy_sin",
    "doy_cos",
    "is_weekend",
]


def _sql(horizonte_dias: int) -> str:
    """Densified per-station calendar + lag/rolling/calendar columns.

    The grid is densified (one row per station per day, gaps included as NULL)
    on purpose: `LAG(dato, n)` counts *rows*, not days, so on a station with
    missing readings a raw row-offset lag would silently mean the wrong date.
    """
    lags = ",\n            ".join(
        f"LAG(dato, {horizonte_dias + off}) OVER w AS lag_{off}" for off in LAG_OFFSETS
    )
    return f"""
        WITH bounds AS (
            SELECT min(fecha) AS f0, max(fecha) AS f1 FROM hist
        ),
        estaciones AS (
            SELECT DISTINCT estacion, cod_dis FROM hist
        ),
        dias AS (
            SELECT unnest(generate_series(
                f0, f1 + INTERVAL {horizonte_dias} DAY, INTERVAL 1 DAY
            ))::DATE AS fecha
            FROM bounds
        ),
        serie AS (
            SELECT e.estacion, e.cod_dis, d.fecha, h.dato
            FROM estaciones e
            CROSS JOIN dias d
            LEFT JOIN hist h ON h.estacion = e.estacion AND h.fecha = d.fecha
        )
        SELECT
            estacion,
            cod_dis,
            fecha,
            dato,
            {lags},
            AVG(dato) OVER (
                PARTITION BY estacion ORDER BY fecha
                ROWS BETWEEN {horizonte_dias + _ROLLING_DAYS} PRECEDING
                         AND {horizonte_dias} PRECEDING
            ) AS roll_mean,
            dayofweek(fecha) AS dow,
            month(fecha) AS mes,
            dayofyear(fecha) AS doy,
            CASE WHEN dayofweek(fecha) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend
        FROM serie
        WINDOW w AS (PARTITION BY estacion ORDER BY fecha)
        ORDER BY estacion, fecha
    """


def build(
    historico: pd.DataFrame, horizonte_dias: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """`historico` (estacion, cod_dis, fecha, dato) -> (tabla_train, tabla_futuro).

    Both tables carry the same feature columns; only `tabla_train` has `dato`.
    """
    hist = historico  # noqa: F841 -- duckdb resolves it from the local scope
    df = duckdb.sql(_sql(horizonte_dias)).df()

    df["fecha"] = pd.to_datetime(df["fecha"])
    # Annual seasonality as a smooth cycle, so 31-dec and 1-jan sit next to
    # each other instead of at opposite ends of a 1..365 ramp.
    angle = 2 * np.pi * df["doy"] / 365.25
    df["doy_sin"], df["doy_cos"] = np.sin(angle), np.cos(angle)

    ultima_real = pd.to_datetime(historico["fecha"]).max()
    listo = df.dropna(subset=FEATURE_COLS[1:])

    train = listo[(listo["fecha"] <= ultima_real) & listo["dato"].notna()]
    futuro = listo[listo["fecha"] > ultima_real].drop(columns=["dato"])

    # The shortest lag (H days) of the furthest future date lands exactly on
    # `ultima_real`. If this trips, the model is reading data that doesn't exist yet.
    assert futuro["fecha"].max() - pd.Timedelta(days=horizonte_dias) <= ultima_real

    return train.reset_index(drop=True), futuro.reset_index(drop=True)
