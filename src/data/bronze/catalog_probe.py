"""Read-only probe against the Madrid Open Data CKAN API.

Lists resources (URL, format, datastore availability) per dataset_id, so we
can decide the download strategy before writing the real ingestor.

Examples:
    uv run python src/data/bronze/catalog_probe.py
    uv run python src/data/bronze/catalog_probe.py --dataset aire_diario --year 2024
    uv run python src/data/bronze/catalog_probe.py \
        --dataset aire_diario --year 2024 --preview
"""

import argparse

import pandas as pd
import requests

CKAN_BASE = "https://datos.madrid.es/api/3/action"

DATASETS = {
    "aire_diario": "201410-0-calidad-aire-diario",
    "estaciones_aire": "212629-0-estaciones-control-aire",
    "trafico_historico": "208627-0-transporte-ptomedida-historico",
    "trafico_puntos_medida": "202468-0-intensidad-trafico",
}


def fetch_package(dataset_id: str) -> dict:
    response = requests.get(f"{CKAN_BASE}/package_show", params={"id": dataset_id})
    response.raise_for_status()
    return response.json()["result"]


def filter_by_year(resources: list[dict], year: str) -> list[dict]:
    return [r for r in resources if year in r["description"]]


def print_resources(dataset_id: str, resources: list[dict]) -> None:
    print(f"\n=== {dataset_id} ({len(resources)} resources) ===")
    for r in resources:
        print(
            f"- {r['name']} | format={r['format']} | "
            f"datastore_active={r['datastore_active']} | "
            f"description={r['description']!r}"
        )


def preview_csv(resource: dict, rows: int = 5) -> None:
    if resource["format"] != "CSV":
        print(f"[preview] skipped: format={resource['format']} is not CSV")
        return
    print(f"\n[preview] first {rows} rows of {resource['url']}")
    print(pd.read_csv(resource["url"], sep=";", nrows=rows))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=DATASETS, help="limit to one dataset")
    parser.add_argument("--year", help="limit to resources mentioning this year")
    parser.add_argument(
        "--preview", action="store_true", help="print first rows of first matching CSV"
    )
    args = parser.parse_args()

    labels = [args.dataset] if args.dataset else list(DATASETS)
    for label in labels:
        package = fetch_package(DATASETS[label])
        resources = package["resources"]
        if args.year:
            resources = filter_by_year(resources, args.year)
        print_resources(label, resources)

        if args.preview:
            csv_resources = [r for r in resources if r["format"] == "CSV"]
            if csv_resources:
                preview_csv(csv_resources[0])
            else:
                print("[preview] skipped: no CSV resource in this selection")
