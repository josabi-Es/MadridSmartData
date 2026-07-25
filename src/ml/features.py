"""DuckDB SQL feature engineering: lags, rolling mean, calendar (spec 004)."""

import duckdb
import pandas as pd

from src.data.access.queries import TRAFFIC_VARIABLES

# ponytail: rolling window uses PRECEDING rows only (not CURRENT ROW like the
# literal spec snippet) — including today's own value in its feature would
# leak the target into X. Revisit if the spec's window is needed verbatim.
_ROLLING_WINDOW = "ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING"


def _feature_columns(order_col: str, partition_col: str, value_col: str) -> str:
    return f"""
        LAG({value_col}, 1) OVER (PARTITION BY {partition_col} ORDER BY {order_col})
            AS lag_1,
        LAG({value_col}, 7) OVER (PARTITION BY {partition_col} ORDER BY {order_col})
            AS lag_7,
        LAG({value_col}, 30) OVER (PARTITION BY {partition_col} ORDER BY {order_col})
            AS lag_30,
        AVG({value_col}) OVER (
            PARTITION BY {partition_col} ORDER BY {order_col} {_ROLLING_WINDOW}
        ) AS roll_mean_7,
        dayofweek({order_col}) AS dow,
        month({order_col}) AS mes,
        CASE WHEN dayofweek({order_col}) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend
    """


def build_air_features(path: str, magnitud: str) -> pd.DataFrame:
    """Feature table for one gas: lags/rolling/calendar, station as a plain column."""
    query = f"""
        SELECT estacion, fecha, dato, {_feature_columns("fecha", "estacion", "dato")}
        FROM '{path}'
        WHERE magnitud = '{magnitud}' AND validez = 'V'
        QUALIFY lag_30 IS NOT NULL
        ORDER BY estacion, fecha
    """
    return duckdb.sql(query).df()


def build_traffic_features(path: str, variable: str) -> pd.DataFrame:
    """Feature table for one traffic variable: lags/rolling/calendar per sensor id.

    Raw readings are every 15 minutes; aggregated to one row per (id, day)
    first so lags mean "days ago" like air's, not "15-minutes-ago" — and so
    the ~4700 sensors don't explode into tens of millions of feature rows.
    """
    if variable not in TRAFFIC_VARIABLES:
        raise ValueError(f"unknown traffic variable: {variable!r}")
    query = f"""
        WITH daily AS (
            SELECT id, CAST(fecha AS DATE) AS fecha, avg({variable}) AS {variable}
            FROM '{path}'
            WHERE error = 'N'
            GROUP BY id, CAST(fecha AS DATE)
        )
        SELECT id, fecha, {variable}, {_feature_columns("fecha", "id", variable)}
        FROM daily
        QUALIFY lag_30 IS NOT NULL
        ORDER BY id, fecha
    """
    return duckdb.sql(query).df()
