import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Análisis Técnico MIAA", layout="wide")

# Estilo visual idéntico a tu proyecto original
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stSelectbox label { color: #00CED1 !important; font-size: 18px; font-weight: bold; }
    div[data-baseweb="select"] { background-color: #162a3d; border-radius: 5px; }
    </style>
""", unsafe_allow_html=True)

# --- CREDENCIALES (EXTRAÍDAS DE TU RESPALDO) ---
db_config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}

# --- DICCIONARIO DE POZOS (Estructura de tu archivo) ---
# He dejado los primeros como ejemplo, aquí debes tener pegado todo tu mapa_pozos_dict
mapa_pozos_dict = {
    "P002": {"caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS"},
    "P003": {"caudal": "PZ_003_CAU_INS", "presion": "PZ_003_PRES_INS"},
    "P004": {"caudal": "PZ_004_CAU_INS", "presion": "PZ_004_PRES_INS"},
    # ... Pega aquí todos los que tienes en el archivo original ...
}

# --- MOTOR DE CONSULTA SQL ---
def obtener_datos_ingenieria(pozo_id):
    info = mapa_pozos_dict[pozo_id]
    tag_cau = info["caudal"]
    tag_pre = info["presion"]
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Consultamos últimos 7 días como en tu respaldo
        fecha_ini = datetime.now() - timedelta(days=7)
        
        query = """
            SELECT T1.FECHA, T2.NAME, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN (%s, %s) AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        cursor.execute(query, (tag_cau, tag_pre, fecha_ini))
        raw_data = cursor.fetchall()
        conn.close()

        if not raw_data:
            return pd.DataFrame()

        # Procesamiento de datos
        df = pd.DataFrame(raw_data, columns=['FECHA', 'TAG', 'VALOR'])
        df_pivot = df.pivot_table(index='FECHA', columns='TAG', values='VALOR').reset_index()
        
        # Renombrar columnas para facilitar manejo
        df_pivot = df_pivot.rename(columns={tag_cau: 'Caudal', tag_pre: 'Presion'})
        return df_pivot

    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return pd.DataFrame()

# --- INTERFAZ DE USUARIO ---
st.title("📊 Análisis de Caudal y Presión")
st.write("Seleccione un pozo de la lista para extraer los históricos de la base de datos.")

# Listado desplegable
pozo_seleccionado = st.selectbox("LISTADO DE POZOS DISPONIBLES:", list(mapa_pozos_dict.keys()))

if pozo_seleccionado:
    with st.spinner(f"Consultando históricos de {pozo_seleccionado}..."):
        df_plot = obtener_datos_ingenieria(pozo_seleccionado)

        if not df_plot.empty:
            # --- CONSTRUCCIÓN DE GRÁFICA TÉCNICA ---
            fig = go.Figure()

            # Traza de Caudal (Cian)
            fig.add_trace(go.Scatter(
                x=df_plot['FECHA'], y=df_plot['Caudal'],
                name="Caudal (l/s)",
                line=dict(color='#00CED1', width=2),
                mode='lines'
            ))

            # Traza de Presión (Naranja)
            fig.add_trace(go.Scatter(
                x=df_plot['FECHA'], y=df_plot['Presion'],
                name="Presión (kg/cm²)",
                line=dict(color='#FF8C00', width=2),
                mode='lines',
                yaxis="y2" # Eje secundario
            ))

            # Formato de gráfica idéntico a tu lógica de Plotly
            fig.update_layout(
                title=f"POZO: {pozo_seleccionado} - COMPORTAMIENTO HIDRÁULICO",
                template="plotly_dark",
                hovermode="x unified",
                xaxis=dict(title="Historial de 7 días"),
                yaxis=dict(
                    title="Caudal (l/s)",
                    titlefont=dict(color="#00CED1"),
                    tickfont=dict(color="#00CED1")
                ),
                yaxis2=dict(
                    title="Presión (kg/cm²)",
                    titlefont=dict(color="#FF8C00"),
                    tickfont=dict(color="#FF8C00"),
                    overlaying='y',
                    side='right'
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )

            st.plotly_chart(fig, use_container_width=True)

            # --- TABLA DE DATOS CRUDOS ---
            with st.expander("Ver tabla de valores"):
                st.dataframe(df_plot, use_container_width=True)
        else:
            st.warning(f"⚠️ No se encontraron registros para {pozo_seleccionado} en los últimos 7 días.")

st.divider()
st.caption("MIAA - Departamento de Telemetría")
