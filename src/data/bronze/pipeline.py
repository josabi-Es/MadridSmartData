"""CKAN -> bronze Parquet ingestion for Madrid Open Data."""

import argparse
import json
import shutil
import time
import zipfile
from pathlib import Path
from typing import Callable, TypeVar

import duckdb
import geopandas as gpd
import requests

from src.data.bronze.ckan import fetch_resources

T = TypeVar("T")

TRAFFIC_TYPES = {
    "intensidad": "DOUBLE",
    "ocupacion": "DOUBLE",
    "carga": "DOUBLE",
    "vmed": "DOUBLE",
}

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

    Returns the row count written.
    """
    con = duckdb.connect()
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    read_args = f"'{csv_source}', sep=';', encoding='UTF-8', allow_quoted_nulls=true"
    if types:
        read_args += f", types={types!r}"
    select = f"SELECT * FROM read_csv_auto({read_args})"
    con.execute(f"COPY ({select}) TO '{out_path}' (FORMAT PARQUET)")
    return con.execute(f"SELECT count(*) FROM '{out_path}'").fetchone()[0]


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
    row_count = csv_to_parquet(resource["url"], str(out_path))

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
    row_count = csv_to_parquet(resource["url"], str(out_path))

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
    row_count = csv_to_parquet(resource["url"], str(out_path))

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
    row_count = csv_to_parquet(csv_path, str(out_path), types=TRAFFIC_TYPES)
    shutil.rmtree(work_dir)

    append_manifest(
        str(Path(out_dir) / "manifest.json"),
        {"month": month, "resource": resource["name"], "rows": row_count},
    )
    return row_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    choices = [
        "aire", "trafico", "estaciones_aire", "trafico_puntos_medida", "distritos"
    ]  # fmt: skip
    parser.add_argument("--dataset", required=True, choices=choices)
    parser.add_argument("--years", help="2024 / 2024-01, omit for a snapshot dataset")
    args = parser.parse_args()

    out_dir = f"data/bronze/{args.dataset}"
    if args.dataset == "distritos":
        n, out_file = ingest_districts(out_dir), "latest.parquet"
    elif args.dataset == "estaciones_aire":
        n, out_file = ingest_snapshot(args.dataset, out_dir), "latest.parquet"
    elif args.dataset == "trafico":
        n = ingest_traffic_month(args.years, out_dir, work_dir=f"{out_dir}/_tmp")
        out_file = f"{args.years}.parquet"
    elif args.dataset == "trafico_puntos_medida":
        n = ingest_month_snapshot(args.dataset, args.years, out_dir)
        out_file = f"{args.years}.parquet"
    else:
        n = ingest_year(args.dataset, args.years, out_dir)
        out_file = f"{args.years}.parquet"
    print(f"{args.dataset}: {n} rows -> {out_dir}/{out_file}")
