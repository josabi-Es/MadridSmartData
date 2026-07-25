import duckdb

from src.ml.features import build_air_features, build_traffic_features


def _write_air_fixture(path, n_days=35):
    rows = ",\n".join(
        f"(4, 'NO2', DATE '2024-01-01' + {i}, {float(i)}, 'V')" for i in range(n_days)
    )
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            {rows},
            (4, 'PM10', DATE '2024-01-01', 999.0, 'V'),
            (4, 'NO2', DATE '2024-01-05', 999.0, 'N')
        ) AS t(estacion, magnitud, fecha, dato, validez))
        TO '{path}' (FORMAT PARQUET)
    """)


def test_build_air_features_has_expected_columns(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    df = build_air_features(str(path), magnitud="NO2")

    expected = {
        "estacion",
        "fecha",
        "dato",
        "lag_1",
        "lag_7",
        "lag_30",
        "roll_mean_7",
        "dow",
        "mes",
        "is_weekend",
    }
    assert expected.issubset(df.columns)


def test_build_air_features_drops_rows_with_missing_lags(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    df = build_air_features(str(path), magnitud="NO2")

    assert df["lag_1"].notna().all()
    assert df["lag_7"].notna().all()
    assert df["lag_30"].notna().all()


def test_build_air_features_filters_by_magnitud_and_validez(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    df = build_air_features(str(path), magnitud="NO2")

    assert (df["dato"] != 999.0).all()


def test_build_air_features_lag_1_matches_previous_day(tmp_path):
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    df = build_air_features(str(path), magnitud="NO2").sort_values("fecha")

    row = df.iloc[0]
    assert row["lag_1"] == row["dato"] - 1


def test_build_air_features_roll_mean_excludes_current_row(tmp_path):
    # roll_mean_7 must only look at PRECEDING days, otherwise the target
    # leaks into its own feature.
    path = tmp_path / "aire.parquet"
    _write_air_fixture(path)

    df = build_air_features(str(path), magnitud="NO2").sort_values("fecha")

    row = df.iloc[0]
    assert row["roll_mean_7"] != row["dato"]


def _write_traffic_fixture(path, n_days=35):
    rows = ",\n".join(
        f"(100, TIMESTAMP '2024-01-01' + INTERVAL {i} DAY, {float(i)}, 'N')"
        for i in range(n_days)
    )
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES
            {rows},
            (100, TIMESTAMP '2024-01-10', 999.0, 'E')
        ) AS t(id, fecha, intensidad, error))
        TO '{path}' (FORMAT PARQUET)
    """)


def test_build_traffic_features_has_expected_columns(tmp_path):
    path = tmp_path / "trafico.parquet"
    _write_traffic_fixture(path)

    df = build_traffic_features(str(path), variable="intensidad")

    expected = {
        "id",
        "fecha",
        "intensidad",
        "lag_1",
        "lag_7",
        "lag_30",
        "roll_mean_7",
        "dow",
        "mes",
        "is_weekend",
    }
    assert expected.issubset(df.columns)


def test_build_traffic_features_filters_errored_readings(tmp_path):
    path = tmp_path / "trafico.parquet"
    _write_traffic_fixture(path)

    df = build_traffic_features(str(path), variable="intensidad")

    assert (df["intensidad"] != 999.0).all()


def test_build_traffic_features_rejects_unknown_variable(tmp_path):
    path = tmp_path / "trafico.parquet"
    _write_traffic_fixture(path)

    try:
        build_traffic_features(str(path), variable="not_a_variable")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for unknown traffic variable")


def test_build_traffic_features_aggregates_intraday_readings_to_daily(tmp_path):
    # Traffic is recorded every 15 minutes; lags must be day-based like air's,
    # not 15-minutes-based, and one row per (id, day) must reach the model.
    path = tmp_path / "trafico.parquet"
    rows = ",\n".join(
        f"(100, TIMESTAMP '2024-01-01' + INTERVAL {i} DAY, {float(i)}, 'N'),\n"
        f"(100, TIMESTAMP '2024-01-01' + INTERVAL {i} DAY + INTERVAL 15 MINUTE, "
        f"{float(i) + 10}, 'N')"
        for i in range(35)
    )
    duckdb.sql(f"""
        COPY (SELECT * FROM (VALUES {rows})
              AS t(id, fecha, intensidad, error))
        TO '{path}' (FORMAT PARQUET)
    """)

    df = build_traffic_features(str(path), variable="intensidad").sort_values("fecha")

    assert df["fecha"].duplicated().sum() == 0
    # first surviving row is day 30 (2024-01-31): avg(30.0, 40.0) == 35.0
    assert df.iloc[0]["intensidad"] == 35.0
