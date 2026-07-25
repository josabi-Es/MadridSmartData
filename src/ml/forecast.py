"""Recursive multi-step forecast past the last known date (spec 006, fase 2)."""

import numpy as np
import pandas as pd


def recursive_forecast(
    model, df: pd.DataFrame, target_col: str, feature_cols: list[str],
    partition_col: str, horizon: int = 365,
) -> pd.DataFrame:  # fmt: skip
    """Roll `model` forward day-by-day past `df["fecha"].max()`.

    Lag/rolling features don't exist for future dates -- each step's
    prediction becomes the next step's lag_1 (and eventually lag_7/lag_30);
    calendar features are date-mechanical, always computable. DuckDB's
    dayofweek() is Sunday=0..Saturday=6 (see src/ml/features.py), so
    Python's weekday() (Monday=0) needs a +1 shift to match.
    """
    df = df.sort_values("fecha")
    last_date = df["fecha"].max()

    partitions = df[partition_col].unique()
    tail_by_partition = df.groupby(partition_col)[target_col].apply(
        lambda s: s.to_numpy()[-30:]
    )
    buffer = np.stack([tail_by_partition[p] for p in partitions]).astype(float)

    steps = []
    for step in range(1, horizon + 1):
        date = last_date + pd.Timedelta(days=step)
        dow = (date.weekday() + 1) % 7

        features = pd.DataFrame(
            {
                partition_col: partitions,
                "lag_1": buffer[:, -1],
                "lag_7": buffer[:, -7],
                "lag_30": buffer[:, -30],
                "roll_mean_7": buffer[:, -7:].mean(axis=1),
                "dow": dow,
                "mes": date.month,
                "is_weekend": int(dow in (0, 6)),
            }
        )[feature_cols]

        predicted = model.predict(features)
        step_df = pd.DataFrame(
            {"fecha": date, partition_col: partitions, "predicted": predicted}
        )
        steps.append(step_df)
        buffer = np.column_stack([buffer[:, 1:], predicted])

    return pd.concat(steps, ignore_index=True)
