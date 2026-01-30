import streamlit as st
from streamlit_folium import st_folium
import folium
import pandas as pd
import mysql.connector
import psycopg2
import plotly.graph_objects as go
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Telemetría Web", layout="wide")

# --- CREDENCIALES (Extraídas de tu respaldo) ---
DB_CONFIG = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}

# --- FUNCIONES DE BASE DE DATOS ---
@st.cache_resource
def get_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG, autocommit=True)
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

def fetch_realtime(tag):
    conn = get_connection()
    if not conn: return 0.0, "S/C"
    cursor = conn.cursor()
    query = """
        SELECT T1.VALUE, T1.FECHA 
        FROM VfiTagNumHistory_Ultimo T1
        JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
        WHERE T2.NAME = %s LIMIT 1
    """
    cursor.execute(query, (tag,))
    res = cursor.fetchone()
    return (round(float(res[0]), 2), res[1]) if res else (0.0, "N/A")

# --- INTERFAZ ---
st.title("🛰️ Monitoreo MIAA - Web")

# Sidebar
with st.sidebar:
    st.header("Filtros de Mapa")
    ver_pozos = st.checkbox("Mostrar Pozos", value=True)
    if st.button("🔄 Actualizar"):
        st.cache_data.clear()
        st.rerun()

# Layout
col_map, col_metrics = st.columns([3, 1])

with col_map:
    # Creamos el mapa centrado en Aguascalientes
    m = folium.Map(location=[21.8818, -102.2917], zoom_start=12, tiles="cartodbpositron")
    
    # Ejemplo con un pozo (P002) de tu diccionario original
    if ver_pozos:
        val, fecha = fetch_realtime("PZ_002_TRC_CAU_INS")
        color = "blue" if val > 0 else "red"
        
        folium.Marker(
            location=[21.88229, -102.31542],
            popup=f"Pozo P002: {val} l/s",
            icon=folium.Icon(color=color, icon='tint')
        ).add_to(m)

    # Renderizar el mapa
    st_folium(m, width="100%", height=600)

with col_metrics:
    st.subheader("Estado de Activos")
    st.metric("P002 - Caudal", f"{val} l/s", delta=f"Última: {fecha}")
    
    # Espacio para más métricas
    st.write("---")
    st.info("Haz clic en un marcador para ver detalles.")

# --- GRÁFICO HISTÓRICO ---
st.divider()
st.subheader("📈 Análisis de Datos (Plotly)")
# Aquí puedes reusar tu lógica de históricos
fig = go.Figure()
fig.add_trace(go.Scatter(y=[10, 12, 11, 14, 15], name="Presión", line=dict(color="#00CED1")))
fig.update_layout(template="plotly_dark", height=300)
st.plotly_chart(fig, use_container_width=True)