"""KPIs and coverage table for Summary tab -- all from gold/dim_*."""

import geopandas as gpd

GOLD_DIM_DISTRITO_PATH = "data/gold/dim_distrito.parquet"


def _cargar_dim_distrito():
    return gpd.read_parquet(GOLD_DIM_DISTRITO_PATH)


def kpis_resumen_texto():
    """4 cards: # districts, air stations, traffic points, districts without coverage."""
    df = _cargar_dim_distrito()
    n_distritos = len(df)
    n_estaciones = int(df["n_estaciones_aire"].sum())
    n_puntos = int(df["n_puntos_trafico"].sum())
    n_sin_cobertura = int((~df["cobertura_aire"]).sum())
    return (
        f"**Districts**  \n{n_distritos}",
        f"**Air stations**  \n{n_estaciones}",
        f"**Traffic points**  \n{n_puntos}",
        f"**Districts without air coverage**  \n{n_sin_cobertura}",
    )


def tabla_cobertura_html():
    """Table with green/red badge per district -- high traffic with no
    air station stands out with just this rule."""
    df = _cargar_dim_distrito().sort_values("NOMBRE")

    filas = ""
    for _, row in df.iterrows():
        if row["cobertura_aire"]:
            badge = (
                "<span style='background:#d4f4dd;color:#1a7a3a;"
                "padding:2px 8px;border-radius:4px;'>With coverage</span>"
            )
        else:
            badge = (
                "<span style='background:#fbdada;color:#a11212;"
                "padding:2px 8px;border-radius:4px;'>No coverage</span>"
            )
        filas += (
            "<tr style='border-top:1px solid #ddd;'>"
            f"<td style='padding:6px 8px;'>{row['NOMBRE']}</td>"
            f"<td style='padding:6px 8px;'>{row['n_estaciones_aire']}</td>"
            f"<td style='padding:6px 8px;'>{row['n_puntos_trafico']}</td>"
            f"<td style='padding:6px 8px;'>{badge}</td>"
            "</tr>"
        )

    return (
        "<table style='width:100%;border-collapse:collapse;font-family:sans-serif;font-size:13px;'>"
        "<tr style='text-align:left;color:#666;'>"
        "<th style='padding:6px 8px;font-weight:400;'>District</th>"
        "<th style='padding:6px 8px;font-weight:400;'>Air stations</th>"
        "<th style='padding:6px 8px;font-weight:400;'>Traffic points</th>"
        "<th style='padding:6px 8px;font-weight:400;'>Coverage</th>"
        "</tr>"
        f"{filas}"
        "</table>"
    )
