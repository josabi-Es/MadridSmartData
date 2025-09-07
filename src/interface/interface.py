import gradio as gr
from first_tab import iniciar_spark, cargar_datos, obtener_estaciones, obtener_magnitudes, graficar_serie_temporal
from second_tab import graficar_serie_trafico , cargar_datos_trafico
from third_tab import generar_mapa_html
import os
import certifi
from third_tab import generar_mapa_html, generar_estaciones_distrito, generar_leyenda_html 
from fourth_tab import plot_tendencia_temporal

# Initialize Spark only once
spark = iniciar_spark()

# Load both datasets only once
df_cont = cargar_datos(spark)
df_trafico = cargar_datos_trafico(spark)

# Function to update pollution plot
def actualizar_grafico(estacion_id, magnitud):
    return graficar_serie_temporal(df_cont, estacion_id, magnitud)


with gr.Blocks() as demo:
    with gr.Tabs():
# --------------------------------------------- #
#             1st Tab - Pollution               #
# --------------------------------------------- #
        with gr.TabItem("🌫️ Contaminación"):
            gr.Markdown("# Visualizador de Contaminación en Madrid")

            selector_id = gr.Dropdown(
                choices=obtener_estaciones(df_cont),
                label="Estación / Punto de Medida"
            )
            
            selector_magnitud = gr.Dropdown(
                choices=obtener_magnitudes(df_cont),
                label="Magnitud (gases o partículas)",
                interactive=True
            )

            boton = gr.Button("Actualizar gráfico")
            grafico = gr.Plot()

            boton.click(
                fn=lambda est, mag: graficar_serie_temporal(df_cont, est, mag),
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
                fn=lambda id_text, variable: graficar_serie_trafico(df_trafico, id_text, variable),
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

            
            estaciones_distrito_df = generar_estaciones_distrito(df_cont)

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
                choices=sorted(df_trafico.select("distrito").distinct().rdd.flatMap(lambda x: x).collect()),
                label="Distrito"
            )

            boton_corr = gr.Button("Mostrar relación")
            grafico_corr = gr.Plot()

            boton_corr.click(
            fn=lambda gas, var, dist: plot_tendencia_temporal(df_cont, df_trafico, estaciones_distrito_df, gas, var, dist),
            inputs=[selector_gas_corr, selector_var_trafico, selector_distrito],
            outputs=grafico_corr
        )

demo.launch(share=True)