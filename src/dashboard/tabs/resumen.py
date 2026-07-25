"""Pestaña Resumen: catálogo (nº distritos/estaciones/puntos, cobertura).

Sin datos de medición -- lee solo de `gold/dim_*`, no de `silver`/`bronze`.
"""

from src.dashboard.components.map import (
    generar_mapa_cobertura_html,
    generar_mapa_posiciones_html,
)
from src.dashboard.components.resumen import kpis_resumen_texto, tabla_cobertura_html


def refrescar_resumen():
    """Vuelve a leer gold/dim_* -- útil tras un `python -m src.data.gold.dimensions`."""
    kpis = kpis_resumen_texto()
    mapa_posiciones = generar_mapa_posiciones_html()
    mapa_cobertura = generar_mapa_cobertura_html()
    tabla = tabla_cobertura_html()
    return (*kpis, mapa_posiciones, mapa_cobertura, tabla)
