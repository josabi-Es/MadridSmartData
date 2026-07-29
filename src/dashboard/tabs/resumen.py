"""Summary tab: catalog (# districts/stations/points, coverage).

No measurement data -- reads only from `gold/dim_*`, not `silver`/`bronze`.
"""

from src.dashboard.components.map import (
    generar_mapa_cobertura_html,
    generar_mapa_posiciones_html,
)
from src.dashboard.components.resumen import kpis_resumen_texto, tabla_cobertura_html


def refrescar_resumen():
    """Reload gold/dim_* -- useful after `python -m src.data.gold.dimensions`."""
    kpis = kpis_resumen_texto()
    mapa_posiciones = generar_mapa_posiciones_html()
    mapa_cobertura = generar_mapa_cobertura_html()
    tabla = tabla_cobertura_html()
    return (*kpis, mapa_posiciones, mapa_cobertura, tabla)
