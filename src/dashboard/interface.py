import gradio as gr

from src.dashboard.components.filters import (
    AIR_VARIABLES,
    obtener_distritos,
    obtener_variables,
)
from src.dashboard.tabs.overview import (
    ANIOS,
    MESES,
    graficar_correlacion,
    refrescar,
)
from src.dashboard.tabs.prediction import (
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
)
from src.data.access.queries import TRAFFIC_VARIABLES

with gr.Blocks() as demo:
    with gr.Tabs():
# --------------------------------------------- #
#     1st Tab - Resumen (catálogo, lee gold)     #
# --------------------------------------------- #
        with gr.TabItem("📋 Resumen"):
            gr.Markdown(
                "# Resumen del catálogo — lee de `data/gold/`, sin datos de medición"
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
                    gr.Markdown("**Cobertura de aire** (verde = con estación, rojo = sin)")
                    resumen_mapa_cobertura = gr.HTML(mapa_cob_ini)

            resumen_tabla = gr.HTML(tabla_ini)

            resumen_boton = gr.Button("🔄 Actualizar (releer gold/)")
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
            gr.Markdown("# Madrid: calidad del aire y tráfico por distrito")

            with gr.Row():
                selector_dominio = gr.Dropdown(
                    choices=["Aire", "Tráfico"], value="Aire", label="Aire/Tráfico"
                )
                selector_variable = gr.Dropdown(
                    choices=obtener_variables("Aire"), label="Variable"
                )
                selector_distrito = gr.Dropdown(
                    choices=obtener_distritos(), label="Distrito"
                )
                selector_anio = gr.Dropdown(choices=ANIOS, value=2024, label="Año")
                selector_mes = gr.Dropdown(choices=MESES, value=1, label="Mes")

            with gr.Row():
                kpi_conteos = gr.Markdown()
                kpi_media = gr.Markdown()

            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("**Posiciones** (estaciones/puntos)")
                    mapa_posiciones_html = gr.HTML()
                with gr.Column(scale=1):
                    gr.Markdown("**Colores** (media por distrito)")
                    leyenda_html = gr.HTML()
                    mapa_colores_html = gr.HTML()

            grafico_evolucion = gr.Plot(label="Evolución temporal")

            with gr.Accordion("Correlación aire ↔ tráfico", open=False):
                with gr.Row():
                    selector_gas_corr = gr.Dropdown(
                        choices=AIR_VARIABLES, value="NO2", label="Gas"
                    )
                    selector_var_trafico_corr = gr.Dropdown(
                        choices=sorted(TRAFFIC_VARIABLES),
                        value="intensidad",
                        label="Variable de tráfico",
                    )
                boton_corr = gr.Button("Mostrar correlación")
                grafico_correlacion = gr.Plot()

            actualizables = [selector_variable, selector_distrito, selector_anio, selector_mes]
            salidas_refresco = [
                leyenda_html, mapa_posiciones_html, mapa_colores_html,
                kpi_conteos, kpi_media, grafico_evolucion,
            ]  # fmt: skip

            selector_dominio.change(
                fn=lambda dominio: gr.Dropdown(choices=obtener_variables(dominio)),
                inputs=selector_dominio,
                outputs=selector_variable,
            )
            for selector in actualizables:
                selector.change(
                    fn=refrescar,
                    inputs=[selector_dominio, selector_variable, selector_distrito, selector_anio, selector_mes],
                    outputs=salidas_refresco,
                )

            boton_corr.click(
                fn=graficar_correlacion,
                inputs=[selector_gas_corr, selector_var_trafico_corr, selector_distrito],
                outputs=grafico_correlacion,
            )

# --------------------------------------------- #
#     3rd Tab - Tabla filtrable                  #
# --------------------------------------------- #
        with gr.TabItem("📄 Tabla"):
            gr.Markdown("### Lecturas diarias por estación y gas")

            selector_estacion_tabla = gr.Dropdown(
                choices=obtener_estaciones_tabla(), label="Estación"
            )
            selector_magnitud_tabla = gr.Dropdown(
                label="Magnitud (gas)", interactive=True
            )
            tabla_diaria_df = gr.Dataframe(headers=COLUMNAS, label="Lecturas diarias")

            selector_estacion_tabla.change(
                fn=lambda estacion: gr.Dropdown(
                    choices=obtener_magnitudes_tabla(estacion)
                ),
                inputs=selector_estacion_tabla,
                outputs=selector_magnitud_tabla,
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
                "### Real vs. predicho (tramo escondido en la validación) "
                "del modelo ganador de la fase 4 — no se reentrena nada aquí"
            )

            selector_variable_pred = gr.Dropdown(
                choices=["NO2", "PM10", "PM2.5", "intensidad"],
                label="Variable"
            )

            selector_estacion_pred = gr.Dropdown(
                label="Estación / Punto de Medida",
                interactive=True
            )

            boton_pred = gr.Button("Mostrar predicción")
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
