"""Minimal CKAN client: dataset_id confirmed in 001-descubrimiento-catalago."""

import requests

CKAN_BASE = "https://datos.madrid.es/api/3/action"

DATASETS = {
    "aire": "201410-0-calidad-aire-diario",
    "estaciones_aire": "212629-0-estaciones-control-aire",
    "trafico": "208627-0-transporte-ptomedida-historico",
    "trafico_puntos_medida": "202468-0-intensidad-trafico",
    "distritos": "300497-0-distritos-municipales-madrid",
}


def fetch_resources(dataset: str) -> list[dict]:
    """Return the resource list for one of the known datasets."""
    response = requests.get(
        f"{CKAN_BASE}/package_show", params={"id": DATASETS[dataset]}
    )
    response.raise_for_status()
    return response.json()["result"]["resources"]
