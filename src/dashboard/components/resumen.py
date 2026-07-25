"""KPIs y tabla de cobertura de la pestaña Resumen -- todo desde gold/dim_*."""

import geopandas as gpd

GOLD_DIM_DISTRITO_PATH = "data/gold/dim_distrito.parquet"


def _cargar_dim_distrito():
    return gpd.read_parquet(GOLD_DIM_DISTRITO_PATH)


def kpis_resumen_texto():
    """4 tarjetas: nº distritos, estaciones aire, puntos tráfico, distritos sin cobertura."""
    df = _cargar_dim_distrito()
    n_distritos = len(df)
    n_estaciones = int(df["n_estaciones_aire"].sum())
    n_puntos = int(df["n_puntos_trafico"].sum())
    n_sin_cobertura = int((~df["cobertura_aire"]).sum())
    return (
        f"**Distritos**  \n{n_distritos}",
        f"**Estaciones de aire**  \n{n_estaciones}",
        f"**Puntos de tráfico**  \n{n_puntos}",
        f"**Distritos sin cobertura de aire**  \n{n_sin_cobertura}",
    )


def tabla_cobertura_html():
    """Tabla con badge verde/rojo por distrito -- mucho tráfico y sin
    estación de aire salta a la vista sin más proceso que esta regla."""
    df = _cargar_dim_distrito().sort_values("NOMBRE")

    filas = ""
    for _, row in df.iterrows():
        if row["cobertura_aire"]:
            badge = (
                "<span style='background:#d4f4dd;color:#1a7a3a;"
                "padding:2px 8px;border-radius:4px;'>Con cobertura</span>"
            )
        else:
            badge = (
                "<span style='background:#fbdada;color:#a11212;"
                "padding:2px 8px;border-radius:4px;'>Sin cobertura</span>"
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
        "<th style='padding:6px 8px;font-weight:400;'>Distrito</th>"
        "<th style='padding:6px 8px;font-weight:400;'>Estaciones aire</th>"
        "<th style='padding:6px 8px;font-weight:400;'>Puntos tráfico</th>"
        "<th style='padding:6px 8px;font-weight:400;'>Cobertura</th>"
        "</tr>"
        f"{filas}"
        "</table>"
    )
