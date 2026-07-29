import os

import gradio as gr
from dotenv import load_dotenv

from src.dashboard.components.filters import (
    AIR_VARIABLES,
    obtener_distritos,
    obtener_variables,
)
from src.dashboard.tabs.overview import (
    graficar_correlacion,
    refrescar,
)
from src.dashboard.tabs.prediction import (
    gases_disponibles,
    graficar_prediccion,
    metricas_texto,
    obtener_estaciones_prediccion,
)
from src.dashboard.tabs.resumen import refrescar_resumen
from src.dashboard.tabs.tabla import (
    COLUMNAS,
    obtener_estaciones_tabla,
    obtener_magnitudes_tabla,
    tabla_diaria,
    unidad_texto,
)
from src.data.access.queries import TRAFFIC_VARIABLES

load_dotenv()
ANIOS_FALLBACK = list(range(int(os.getenv("INGEST_YEAR_START", "2023")), int(os.getenv("INGEST_YEAR_END", "2025")) + 1))
MESES_FALLBACK = list(range(1, 13))

with gr.Blocks() as demo:
    with gr.Tabs():
# --------------------------------------------- #
#     1st Tab - Summary (catalog, reads gold)     #
# --------------------------------------------- #
        with gr.TabItem("📋 Resumen"):
            gr.Markdown(
                "# Catalog summary"
            )

            (
                kpi_distritos_ini, kpi_estaciones_ini, kpi_puntos_ini, kpi_cobertura_ini,
                mapa_pos_ini, mapa_cob_ini, tabla_ini,
            ) = refrescar_resumen()  # fmt: skip

            with gr.Row():
                resumen_kpi_distritos = gr.Markdown(kpi_distritos_ini)
                resumen_kpi_estaciones = gr.Markdown(kpi_estaciones_ini)
                resumen_kpi_puntos = gr.Markdown(kpi_puntos_ini)
                resumen_kpi_cobertura = gr.Markdown(kpi_cobertura_ini)

            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("**Posiciones** (estaciones/puntos)")
                    resumen_mapa_posiciones = gr.HTML(mapa_pos_ini)
                with gr.Column(scale=1):
                    gr.Markdown("**Air coverage** (green = with station, red = without)")
                    resumen_mapa_cobertura = gr.HTML(mapa_cob_ini)

            resumen_tabla = gr.HTML(tabla_ini)

            resumen_boton = gr.Button("🔄 Refresh")
            resumen_boton.click(
                fn=refrescar_resumen,
                outputs=[
                    resumen_kpi_distritos, resumen_kpi_estaciones,
                    resumen_kpi_puntos, resumen_kpi_cobertura,
                    resumen_mapa_posiciones, resumen_mapa_cobertura, resumen_tabla,
                ],  # fmt: skip
            )

# --------------------------------------------- #
#     2nd Tab - Overview dashboard               #
# --------------------------------------------- #
        with gr.TabItem("📊 Dashboard"):
            gr.Markdown("# Madrid: air quality and traffic by district")

            with gr.Row():
                with gr.Column(scale=0, min_width=180):
                    gr.Markdown("**Colors**")
                    leyenda_html = gr.HTML()
                with gr.Column(scale=4):
                    mapa_colores_html = gr.HTML()

            with gr.Row():
                selector_dominio = gr.Dropdown(
                    choices=["Aire", "Tráfico"], value="Aire", label="Air/Traffic"
                )
                selector_variable = gr.Dropdown(
                    choices=obtener_variables("Aire"), label="Variable"
                )
                selector_distrito = gr.Dropdown(
                    choices=obtener_distritos(), label="District"
                )
                selector_anio = gr.Dropdown(
                    choices=ANIOS_FALLBACK, value=2024, label="Year"
                )
                selector_mes = gr.Dropdown(
                    choices=MESES_FALLBACK, value=1, label="Month"
                )

            with gr.Row():
                boton_buscar = gr.Button("🔍 Buscar")

            with gr.Row():
                kpi_conteos = gr.Markdown()
                kpi_media = gr.Markdown()

            grafico_evolucion = gr.Plot(label="Temporal evolution")

            with gr.Accordion("Air ↔ traffic correlation", open=False):
                with gr.Row():
                    selector_gas_corr = gr.Dropdown(
                        choices=AIR_VARIABLES, value="NO2", label="Gas"
                    )
                    selector_var_trafico_corr = gr.Dropdown(
                        choices=sorted(TRAFFIC_VARIABLES),
                        value="intensidad",
                        label="Traffic variable",
                    )
                boton_corr = gr.Button("Show correlation")
                grafico_correlacion = gr.Plot()

            salidas_refresco = [
                leyenda_html, mapa_colores_html,
                kpi_conteos, kpi_media, grafico_evolucion,
            ]  # fmt: skip

            # Botón único "Buscar" dispara refrescar(), sin cascadas automáticas
            boton_buscar.click(
                fn=refrescar,
                inputs=[selector_dominio, selector_variable, selector_distrito, selector_anio, selector_mes],
                outputs=salidas_refresco,
            )

            # Mínima cascada: solo Dominio → Variable
            # Años/Meses vienen del .env (INGEST_YEAR_START, INGEST_YEAR_END, meses 1-12)
            def _refrescar_variables(dominio, variable_actual):
                opciones = obtener_variables(dominio)
                if variable_actual in opciones:
                    return gr.Dropdown(choices=opciones, value=variable_actual)
                valor = opciones[0] if opciones else None
                return gr.Dropdown(choices=opciones, value=valor)

            selector_dominio.change(
                fn=_refrescar_variables,
                inputs=[selector_dominio, selector_variable],
                outputs=selector_variable,
            )

            selector_distrito.change(
                fn=lambda distrito: gr.Dropdown(
                    choices=obtener_variables("Aire", distrito)
                ),
                inputs=selector_distrito,
                outputs=selector_gas_corr,
            )

            boton_corr.click(
                fn=graficar_correlacion,
                inputs=[selector_gas_corr, selector_var_trafico_corr, selector_distrito],
                outputs=grafico_correlacion,
            )

# --------------------------------------------- #
#     3rd Tab - Filterable table                 #
# --------------------------------------------- #
        with gr.TabItem("📄 Tabla"):
            gr.Markdown("### Daily readings by station and gas")

            selector_estacion_tabla = gr.Dropdown(
                choices=obtener_estaciones_tabla(), label="Station"
            )
            selector_magnitud_tabla = gr.Dropdown(
                label="Magnitude (gas) -- only what this station measures",
                interactive=True,
            )
            unidad_tabla = gr.Markdown()
            tabla_diaria_df = gr.Dataframe(headers=COLUMNAS, label="Daily readings")

            selector_estacion_tabla.change(
                fn=lambda estacion: gr.Dropdown(
                    choices=obtener_magnitudes_tabla(estacion)
                ),
                inputs=selector_estacion_tabla,
                outputs=selector_magnitud_tabla,
            )
            selector_magnitud_tabla.change(
                fn=unidad_texto, inputs=selector_magnitud_tabla, outputs=unidad_tabla
            )
            selector_magnitud_tabla.change(
                fn=tabla_diaria,
                inputs=[selector_estacion_tabla, selector_magnitud_tabla],
                outputs=tabla_diaria_df,
            )

# --------------------------------------------- #
#     4th Tab - Forecast (real vs. predicted)    #
# --------------------------------------------- #
        with gr.TabItem("🔮 Predicción"):
            gr.Markdown(
                "### Air Quality Forecast"
            )

            selector_variable_pred = gr.Dropdown(
                choices=gases_disponibles(),
                label="Gas"
            )

            selector_estacion_pred = gr.Dropdown(
                label="Station / Measurement Point",
                interactive=True
            )

            boton_pred = gr.Button("Show prediction")
            grafico_pred = gr.Plot()
            metricas_pred = gr.Markdown()

            selector_variable_pred.change(
                fn=lambda variable: gr.Dropdown(
                    choices=obtener_estaciones_prediccion(variable)
                ),
                inputs=selector_variable_pred,
                outputs=selector_estacion_pred
            )
            selector_variable_pred.change(
                fn=metricas_texto,
                inputs=selector_variable_pred,
                outputs=metricas_pred
            )
            boton_pred.click(
                fn=graficar_prediccion,
                inputs=[selector_variable_pred, selector_estacion_pred],
                outputs=grafico_pred
            )

demo.launch()
