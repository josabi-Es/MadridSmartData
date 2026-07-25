import os

import gradio as gr
from dotenv import load_dotenv

from src.app.gradio.fifth_tab import (
    graficar_prediccion,
    metricas_texto,
    obtener_estaciones_prediccion,
)
from src.app.gradio.first_tab import (
    graficar_serie_temporal,
    obtener_estaciones,
    obtener_magnitudes,
)
from src.app.gradio.fourth_tab import plot_tendencia_temporal
from src.app.gradio.second_tab import graficar_serie_trafico
from src.app.gradio.third_tab import generar_leyenda_html, generar_mapa_html
from src.data.access.queries import get_traffic_districts

load_dotenv()

TRAFFIC_POINTS_PATH = os.getenv(
    "TRAFFIC_POINTS_PATH", "data/bronze/trafico_puntos_medida/*.parquet"
)


with gr.Blocks() as demo:
    with gr.Tabs():
# --------------------------------------------- #
#             1st Tab - Pollution               #
# --------------------------------------------- #
        with gr.TabItem("🌫️ Contaminación"):
            gr.Markdown("# Visualizador de Contaminación en Madrid")

            selector_id = gr.Dropdown(
                choices=obtener_estaciones(),
                label="Estación / Punto de Medida"
            )

            selector_magnitud = gr.Dropdown(
                choices=obtener_magnitudes(),
                label="Magnitud (gases o partículas)",
                interactive=True
            )

            boton = gr.Button("Actualizar gráfico")
            grafico = gr.Plot()

            boton.click(
                fn=graficar_serie_temporal,
                inputs=[selector_id, selector_magnitud],
                outputs=grafico
            )

# --------------------------------------------- #
#             2nd Tab - Traffic                 #
# --------------------------------------------- #

        with gr.TabItem("🚗 Tráfico"):
            gr.Markdown("# Visualizador de Tráfico - Señales de movilidad urbana")

            input_id_trafico = gr.Textbox(
                label="ID de Punto de Tráfico", placeholder="Ej: 3906"
            )

            selector_variable = gr.Dropdown(
                choices=["intensidad", "ocupacion", "carga", "vmed"],
                label="Variable de tráfico"
            )

            boton_trafico = gr.Button("Mostrar gráfico")
            grafico_trafico = gr.Plot()

            boton_trafico.click(
                fn=graficar_serie_trafico,
                inputs=[input_id_trafico, selector_variable],
                outputs=grafico_trafico
            )


# --------------------------------------------- #
#         3rd Tab - Map by District             #
# --------------------------------------------- #
        with gr.TabItem("🗺️ Mapa Distritos"):
            gr.Markdown("### Visualizador por distrito según gas, año y mes")

            selector_gas = gr.Dropdown(
                choices=["NO2", "PM10", "PM2.5", "O3", "NOx"],
                label="Tipo de gas"
            )

            selector_year = gr.Dropdown(
                choices=[2020, 2021, 2022, 2023, 2024],
                label="Año"
            )

            selector_month = gr.Dropdown(
                choices=list(range(1, 13)),
                label="Mes (número)"
            )
            leyenda_html = gr.HTML()
            mapa_html = gr.HTML()

            selector_gas.change(
                fn=generar_leyenda_html,
                inputs=selector_gas,
                outputs=leyenda_html
            )
            selector_month.change(
                fn=generar_mapa_html,
                inputs=[selector_gas, selector_year, selector_month],
                outputs=mapa_html
            )



# --------------------------------------------- #
#     4th Tab - Traffic vs Gas Correlation       #
# --------------------------------------------- #
        with gr.TabItem("📊 Correlación"):
            gr.Markdown("### Relación entre tráfico y contaminación por distrito")

            selector_gas_corr = gr.Dropdown(
                choices=["NO2", "PM10", "PM2.5", "O3", "NOx", "CO","PM2.5"],
                label="Gas contaminante"
            )

            selector_var_trafico = gr.Dropdown(
                choices=["intensidad", "ocupacion", "carga", "vmed"],
                label="Variable de tráfico"
            )

            selector_distrito = gr.Dropdown(
                choices=get_traffic_districts(TRAFFIC_POINTS_PATH),
                label="Distrito"
            )

            boton_corr = gr.Button("Mostrar relación")
            grafico_corr = gr.Plot()

            boton_corr.click(
            fn=plot_tendencia_temporal,
            inputs=[selector_gas_corr, selector_var_trafico, selector_distrito],
            outputs=grafico_corr
        )

# --------------------------------------------- #
#     5th Tab - Forecast (real vs. predicted)    #
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
