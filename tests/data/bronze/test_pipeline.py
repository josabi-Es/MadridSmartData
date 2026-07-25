import json
import zipfile
from pathlib import Path

import duckdb
import geopandas as gpd
import pytest
import requests
from shapely.geometry import Point

from src.data.bronze.pipeline import (
    append_manifest,
    csv_to_parquet,
    extract_first_file,
    fetch_with_retry,
    month_to_description,
    select_by_format,
    select_resource,
    shapefile_to_parquet,
)


def test_select_resource_picks_matching_year_and_format():
    resources = [
        {"description": "Calidad del aire. Datos diarios. 2023", "format": "XML"},
        {"description": "Calidad del aire. Datos diarios. 2024", "format": "CSV"},
        {"description": "Calidad del aire. Datos diarios. 2024", "format": "XML"},
    ]

    result = select_resource(resources, year="2024", fmt="CSV")

    assert result["format"] == "CSV"
    assert "2024" in result["description"]


def test_select_resource_raises_when_no_match():
    resources = [{"description": "Calidad del aire. 2023", "format": "CSV"}]

    with pytest.raises(ValueError, match="2024"):
        select_resource(resources, year="2024", fmt="CSV")


def test_append_manifest_replaces_entry_with_same_year(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    append_manifest(str(manifest_path), {"year": "2024", "resource": "r1", "rows": 10})

    append_manifest(str(manifest_path), {"year": "2024", "resource": "r2", "rows": 20})

    entries = json.loads(manifest_path.read_text())
    assert len(entries) == 1
    assert entries[0] == {"year": "2024", "resource": "r2", "rows": 20}


def test_append_manifest_keeps_entries_for_different_years(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    append_manifest(str(manifest_path), {"year": "2024", "resource": "r1", "rows": 10})

    append_manifest(str(manifest_path), {"year": "2025", "resource": "r2", "rows": 20})

    entries = json.loads(manifest_path.read_text())
    assert len(entries) == 2
    assert {e["year"] for e in entries} == {"2024", "2025"}


def test_append_manifest_replaces_entry_with_same_month(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    append_manifest(
        str(manifest_path), {"month": "2024-01", "resource": "r1", "rows": 10}
    )

    append_manifest(
        str(manifest_path), {"month": "2024-01", "resource": "r2", "rows": 20}
    )

    entries = json.loads(manifest_path.read_text())
    assert len(entries) == 1
    assert entries[0]["resource"] == "r2"


def test_append_manifest_replaces_snapshot_entry_with_no_year_or_month(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    append_manifest(str(manifest_path), {"resource": "r1", "rows": 10})

    append_manifest(str(manifest_path), {"resource": "r2", "rows": 20})

    entries = json.loads(manifest_path.read_text())
    assert len(entries) == 1
    assert entries[0]["resource"] == "r2"


def test_csv_to_parquet_forces_types_over_nan_literal(tmp_path):
    csv_path = tmp_path / "trafico.csv"
    csv_path.write_text("id;vmed\n3489;NaN\n3490;42\n")
    out_path = tmp_path / "trafico.parquet"

    row_count = csv_to_parquet(str(csv_path), str(out_path), types={"vmed": "DOUBLE"})

    assert row_count == 2
    schema = duckdb.sql(f"DESCRIBE SELECT * FROM '{out_path}'").fetchall()
    vmed_type = next(col[1] for col in schema if col[0] == "vmed")
    assert vmed_type == "DOUBLE"


def test_csv_to_parquet_reads_latin1_encoded_text(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_bytes("nombre\nLeganés\n".encode("latin-1"))
    out_path = tmp_path / "sample.parquet"

    csv_to_parquet(str(csv_path), str(out_path), encoding="latin-1")

    result = duckdb.sql(f"SELECT nombre FROM '{out_path}'").fetchone()
    assert result == ("Leganés",)


def test_csv_to_parquet_converts_and_returns_row_count(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("estacion;magnitud;dato\n4;NO2;12.5\n8;NO2;7.3\n")
    out_path = tmp_path / "sample.parquet"

    row_count = csv_to_parquet(str(csv_path), str(out_path))

    assert row_count == 2
    assert out_path.exists()
    result = duckdb.sql(f"SELECT dato FROM '{out_path}' ORDER BY dato").fetchall()
    assert result == [(7.3,), (12.5,)]


def test_append_manifest_creates_file_with_one_entry(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    append_manifest(str(manifest_path), {"year": "2024", "hash": "abc"})

    entries = json.loads(manifest_path.read_text())
    assert entries == [{"year": "2024", "hash": "abc"}]


def test_append_manifest_appends_to_existing_entries(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps([{"year": "2023", "hash": "old"}]))

    append_manifest(str(manifest_path), {"year": "2024", "hash": "abc"})

    entries = json.loads(manifest_path.read_text())
    assert entries == [{"year": "2023", "hash": "old"}, {"year": "2024", "hash": "abc"}]


def test_select_by_format_picks_single_matching_resource():
    resources = [
        {"format": "PDF", "name": "structure-doc"},
        {"format": "CSV", "name": "stations-csv"},
        {"format": "XLS", "name": "stations-xls"},
    ]

    result = select_by_format(resources, "CSV")

    assert result["name"] == "stations-csv"


def test_select_by_format_raises_when_no_match():
    resources = [{"format": "PDF", "name": "structure-doc"}]

    with pytest.raises(ValueError, match="CSV"):
        select_by_format(resources, "CSV")


def test_month_to_description_converts_january():
    assert month_to_description("2024-01") == "Enero 2024"


def test_month_to_description_converts_december():
    assert month_to_description("2024-12") == "Diciembre 2024"


def test_shapefile_to_parquet_converts_and_returns_row_count(tmp_path):
    shp_path = tmp_path / "districts.shp"
    gdf = gpd.GeoDataFrame(
        {"NOMBRE": ["Centro", "Salamanca"]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )
    gdf.to_file(shp_path)
    out_path = tmp_path / "districts.parquet"

    row_count = shapefile_to_parquet(str(shp_path), str(out_path))

    assert row_count == 2
    result = gpd.read_parquet(out_path)
    assert sorted(result["NOMBRE"]) == ["Centro", "Salamanca"]


def test_extract_first_file_returns_path_to_extracted_csv(tmp_path):
    zip_path = tmp_path / "trafico_2024_01.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("01-2024.csv", "id;vmed\n1;10\n")

    extracted = extract_first_file(str(zip_path), str(tmp_path))

    assert extracted == str(tmp_path / "01-2024.csv")
    assert Path(extracted).read_text() == "id;vmed\n1;10\n"


def _fake_zip_bytes() -> bytes:
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(
            "01-2024.csv",
            "id;intensidad;ocupacion;carga;vmed\n1;100;5;20;NaN\n",
        )
    return buf.getvalue()


def test_ingest_traffic_month_cleans_up_work_dir(tmp_path, monkeypatch):
    from src.data.bronze import pipeline

    resource = {
        "name": "208627-x-zip",
        "format": "ZIP",
        "description": "Histórico de datos del tráfico. Enero 2024",
        "url": "https://example.invalid/trafico.zip",
    }
    monkeypatch.setattr(pipeline, "fetch_resources", lambda dataset: [resource])
    monkeypatch.setattr(pipeline, "fetch_with_retry", lambda fn, **kwargs: fn())

    class FakeResponse:
        content = _fake_zip_bytes()

        def raise_for_status(self):
            pass

    monkeypatch.setattr(pipeline.requests, "get", lambda url, timeout: FakeResponse())

    work_dir = tmp_path / "work"
    out_dir = tmp_path / "out"
    pipeline.ingest_traffic_month("2024-01", str(out_dir), str(work_dir))

    assert not work_dir.exists()
    assert (out_dir / "2024-01.parquet").exists()


def test_fetch_with_retry_succeeds_after_transient_failures():
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise requests.RequestException("network blip")
        return "ok"

    sleeps = []
    result = fetch_with_retry(flaky, attempts=3, base_delay=1, sleep=sleeps.append)

    assert result == "ok"
    assert len(attempts) == 3
    assert sleeps == [1, 2]


def test_fetch_with_retry_raises_after_exhausting_attempts():
    def always_fails():
        raise requests.RequestException("down")

    with pytest.raises(requests.RequestException):
        fetch_with_retry(always_fails, attempts=2, base_delay=1, sleep=lambda _: None)
