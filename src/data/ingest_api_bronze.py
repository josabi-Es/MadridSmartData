"""CKAN -> bronze Parquet ingestion for Madrid Open Data."""

import argparse
import json
import os
import shutil
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import Callable, TypeVar

import duckdb
import geopandas as gpd
import requests
from dotenv import load_dotenv

from src.data.bronze.ckan import fetch_resources
from src.utils.logger_config import logger

load_dotenv()

T = TypeVar("T")

TRAFFIC_TYPES = {
    "intensidad": "DOUBLE",
    "ocupacion": "DOUBLE",
    "carga": "DOUBLE",
    "vmed": "DOUBLE",
}

# datos.madrid.es has used both latin-1 and UTF-8. csv_to_parquet tries
# multiple encodings automatically, so this is now None (let it detect).
_MADRID_OPEN_DATA_ENCODING = None

SPANISH_MONTHS = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]  # fmt: skip


def month_to_description(month: str) -> str:
    """Convert 'YYYY-MM' into the Spanish 'Mes YYYY' the CKAN description uses."""
    year, month_num = month.split("-")
    return f"{SPANISH_MONTHS[int(month_num) - 1]} {year}"


def select_resource(resources: list[dict], year: str, fmt: str = "CSV") -> dict:
    """Pick the resource matching a year (in its description) and format."""
    for resource in resources:
        if year in resource["description"] and resource["format"] == fmt:
            return resource
    raise ValueError(f"no {fmt} resource found for year {year!r}")


def select_by_format(resources: list[dict], fmt: str) -> dict:
    """Pick the resource matching a format, for datasets with no year/month split."""
    for resource in resources:
        if resource["format"] == fmt:
            return resource
    raise ValueError(f"no {fmt} resource found")


def csv_to_parquet(
    csv_source: str,
    out_path: str,
    types: dict[str, str] | None = None,
    encoding: str | None = None,
) -> int:
    """Stream a CSV (local path or URL) into a Parquet file via DuckDB.

    Returns the row count written. Tries multiple encodings if the first fails.
    """
    con = duckdb.connect()
    con.install_extension("httpfs")
    con.load_extension("httpfs")

    encodings = [encoding] if encoding else ["utf-8", "latin-1"]
    last_error = None

    for enc in encodings:
        try:
            read_args = (
                f"'{csv_source}', sep=';', encoding={enc!r}, "
                "allow_quoted_nulls=true"
            )
            if types:
                read_args += f", types={types!r}"
            select = f"SELECT * FROM read_csv_auto({read_args})"
            con.execute(f"COPY ({select}) TO '{out_path}' (FORMAT PARQUET)")
            return con.execute(f"SELECT count(*) FROM '{out_path}'").fetchone()[0]
        except Exception as e:
            last_error = e
            continue

    raise last_error


def shapefile_to_parquet(source: str, out_path: str) -> int:
    """Read a shapefile (local path or 'zip+<url>') via geopandas, write GeoParquet."""
    gdf = gpd.read_file(source)
    gdf.to_parquet(out_path)
    return len(gdf)


def extract_first_file(zip_path: str, dest_dir: str) -> str:
    """Extract a ZIP's first member into dest_dir, return its full path."""
    with zipfile.ZipFile(zip_path) as z:
        name = z.namelist()[0]
        z.extractall(dest_dir)
    return str(Path(dest_dir) / name)


def append_manifest(manifest_path: str, entry: dict) -> None:
    """Upsert one entry into the JSON manifest, creating it if missing.

    Re-ingesting the same year/month overwrites its manifest entry instead
    of piling up duplicate log lines -- the parquet itself already gets
    overwritten per year/month file, the manifest should match. Snapshot
    entries (no year/month, e.g. distritos/estaciones_aire) have no key to
    dedupe on, so a re-run just replaces the single existing entry.
    """
    path = Path(manifest_path)
    entries = json.loads(path.read_text()) if path.exists() else []

    key = "year" if "year" in entry else "month" if "month" in entry else None
    if key is not None:
        entries = [e for e in entries if e.get(key) != entry[key]]
    else:
        entries = []

    entries.append(entry)
    path.write_text(json.dumps(entries, indent=2))


def fetch_with_retry(
    fn: Callable[[], T],
    attempts: int = 3,
    base_delay: float = 2,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call fn, retrying on requests.RequestException with exponential backoff."""
    for attempt in range(attempts):
        try:
            return fn()
        except requests.RequestException:
            if attempt == attempts - 1:
                raise
            sleep(base_delay * 2**attempt)
    raise AssertionError("unreachable")


def ingest_year(dataset: str, year: str, out_dir: str) -> int:
    """Ingest one year (streaming CSV -> bronze Parquet) for a direct-CSV dataset."""
    resources = fetch_with_retry(lambda: fetch_resources(dataset))
    resource = select_resource(resources, year)

    out_path = Path(out_dir) / f"{year}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = csv_to_parquet(
        resource["url"], str(out_path), encoding=_MADRID_OPEN_DATA_ENCODING
    )

    append_manifest(
        str(Path(out_dir) / "manifest.json"),
        {"year": year, "resource": resource["name"], "rows": row_count},
    )
    return row_count


def ingest_snapshot(dataset: str, out_dir: str) -> int:
    """Ingest a non-partitioned dataset (single current CSV snapshot)."""
    resources = fetch_with_retry(lambda: fetch_resources(dataset))
    resource = select_by_format(resources, "CSV")

    out_path = Path(out_dir) / "latest.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = csv_to_parquet(
        resource["url"], str(out_path), encoding=_MADRID_OPEN_DATA_ENCODING
    )

    append_manifest(
        str(Path(out_dir) / "manifest.json"),
        {"resource": resource["name"], "rows": row_count},
    )
    return row_count


def ingest_month_snapshot(dataset: str, month: str, out_dir: str) -> int:
    """Ingest one month of a monthly-snapshot, direct-CSV dataset (sensor locations)."""
    resources = fetch_with_retry(lambda: fetch_resources(dataset))
    resource = select_resource(resources, month_to_description(month))

    out_path = Path(out_dir) / f"{month}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = csv_to_parquet(
        resource["url"], str(out_path), encoding=_MADRID_OPEN_DATA_ENCODING
    )

    append_manifest(
        str(Path(out_dir) / "manifest.json"),
        {"month": month, "resource": resource["name"], "rows": row_count},
    )
    return row_count


def ingest_districts(out_dir: str) -> int:
    """Ingest the districts shapefile straight from its remote ZIP."""
    resources = fetch_with_retry(lambda: fetch_resources("distritos"))
    resource = select_by_format(resources, "ZIP")

    out_path = Path(out_dir) / "latest.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = shapefile_to_parquet(f"zip+{resource['url']}", str(out_path))

    append_manifest(
        str(Path(out_dir) / "manifest.json"),
        {"resource": resource["name"], "rows": row_count},
    )
    return row_count


def ingest_traffic_month(month: str, out_dir: str, work_dir: str) -> int:
    """Download one traffic ZIP, extract, force types, write bronze Parquet."""
    resources = fetch_with_retry(lambda: fetch_resources("trafico"))
    resource = select_resource(resources, month_to_description(month), fmt="ZIP")

    Path(work_dir).mkdir(parents=True, exist_ok=True)
    zip_path = Path(work_dir) / f"{month}.zip"

    def download():
        response = requests.get(resource["url"], timeout=60)
        response.raise_for_status()
        zip_path.write_bytes(response.content)

    fetch_with_retry(download)
    csv_path = extract_first_file(str(zip_path), work_dir)

    out_path = Path(out_dir) / f"{month}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = csv_to_parquet(
        csv_path,
        str(out_path),
        types=TRAFFIC_TYPES,
        encoding=_MADRID_OPEN_DATA_ENCODING,
    )
    shutil.rmtree(work_dir)

    append_manifest(
        str(Path(out_dir) / "manifest.json"),
        {"month": month, "resource": resource["name"], "rows": row_count},
    )
    return row_count


DATASET_CHOICES = [
    "aire", "trafico", "estaciones_aire", "trafico_puntos_medida", "distritos"
]  # fmt: skip


def run_dataset(dataset: str, years: str | None) -> tuple[int, str]:
    """Ingest one dataset (single year/month, or snapshot). Returns (rows, out_file)."""
    out_dir = f"data/bronze/{dataset}"
    if dataset == "distritos":
        return ingest_districts(out_dir), "latest.parquet"
    if dataset == "estaciones_aire":
        return ingest_snapshot(dataset, out_dir), "latest.parquet"
    if dataset == "trafico":
        n = ingest_traffic_month(years, out_dir, work_dir=f"{out_dir}/_tmp")
        return n, f"{years}.parquet"
    if dataset == "trafico_puntos_medida":
        return ingest_month_snapshot(dataset, years, out_dir), f"{years}.parquet"
    return ingest_year(dataset, years, out_dir), f"{years}.parquet"


def _run_dataset_safe(dataset: str, years: str | None) -> None:
    """Same as run_dataset, but logs and continues instead of aborting the batch.

    A historical backfill spans many year/month combos -- one month with no
    published resource yet (or a transient API hiccup after retries) should
    not kill the other 80+ calls in the same run.
    """
    try:
        n, out_file = run_dataset(dataset, years)
        logger.info(
            f"{dataset} {years or ''}: {n} filas -> data/bronze/{dataset}/{out_file}"
        )
    except Exception as e:
        logger.warning(f"{dataset} {years or ''}: fallo, se salta -- {e}")


def ingest_all_from_env() -> None:
    """Ingest every dataset for the year range configured in .env.

    Driven by INGEST_YEAR_START/INGEST_YEAR_END (default 2019 -> current
    year) so a single `python -m src.data.ingest_api_bronze` (no args)
    does the full historical backfill instead of one command per
    dataset/year/month. Snapshots (distritos/estaciones_aire) run once;
    aire loops per year; trafico/trafico_puntos_medida loop per year-month
    (12 calls/year each).
    """
    start = int(os.getenv("INGEST_YEAR_START", "2019"))
    end = int(os.getenv("INGEST_YEAR_END", str(date.today().year)))
    logger.info(f"Ingesta completa {start}-{end}")

    _run_dataset_safe("distritos", None)
    _run_dataset_safe("estaciones_aire", None)

    for year in range(start, end + 1):
        _run_dataset_safe("aire", str(year))
        for month in range(1, 13):
            year_month = f"{year}-{month:02d}"
            _run_dataset_safe("trafico", year_month)
            _run_dataset_safe("trafico_puntos_medida", year_month)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        choices=DATASET_CHOICES,
        help="omit with --years to ingest everything per .env (INGEST_YEAR_START/END)",
    )
    parser.add_argument("--years", help="2024 / 2024-01, omit for a snapshot dataset")
    args = parser.parse_args()

    if args.dataset is None:
        ingest_all_from_env()
    else:
        rows, written_file = run_dataset(args.dataset, args.years)
        out_dir = f"data/bronze/{args.dataset}"
        print(f"{args.dataset}: {rows} rows -> {out_dir}/{written_file}")
