import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import Fullscreen
import pandas as pd
import mysql.connector
import psycopg2
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Telemetría Avanzada", layout="wide", page_icon="🛰️")

# Estilo Dark que manejas en Tkinter
st.markdown("""
    <style>
    .main { background-color: #0b1a29; color: white; }
    .stMetric { background-color: #162a3d !important; border: 1px solid #00CED1 !important; border-radius: 10px; padding: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- CREDENCIALES ORIGINALES ---
config_pozos = {'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'host': 'miaa.mx', 'database': 'miaamx_telemetria'}
config_macro = {'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'host': 'miaa.mx', 'database': 'miaamx_telemetria2'}
config_postgres = {'user': 'map_tecnica', 'password': 'M144.Tec', 'host': 'ti.miaa.mx', 'database': 'qgis'}

# ==============================================================================
# AQUÍ PEGA TUS DICCIONARIOS COMPLETOS (mapa_pozos_dict, mapa_tanques_dict, etc.)
# ==============================================================================
mapa_pozos_dict = {
    "P002": {"coord": (21.88229, -102.31542), "caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS", "voltajes_l": ["PZ_002_TRC_VOL_L1_L2", "PZ_002_TRC_VOL_L2_L3", "PZ_002_TRC_VOL_L1_L3"]},
    # ... Pega todos los demás aquí
}

# --- FUNCIONES DE INGENIERÍA (Tu lógica de datos) ---
@st.cache_resource
def get_conn(conf):
    return mysql.connector.connect(**conf, autocommit=True)

def obtener_datos_actuales(tags):
    conn = get_conn(config_pozos)
    if not conn: return {}
    cursor = conn.cursor()
    format_strings = ','.join(['%s'] * len(tags))
    query = f"SELECT T2.NAME, T1.VALUE, T1.FECHA FROM VfiTagNumHistory_Ultimo T1 JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID WHERE T2.NAME IN ({format_strings})"
    cursor.execute(query, tuple(tags))
    return {row[0]: (float(row[1]), row[2]) for row in cursor.fetchall()}

def obtener_historico_ingenieria(tag_cau, tag_pre):
    conn = get_conn(config_pozos)
    cursor = conn.cursor()
    # Consulta de 7 días exactos como en tu respaldo
    inicio = datetime.now() - timedelta(days=7)
    query = """
        SELECT FECHA, 
               MAX(CASE WHEN NAME = %s THEN VALUE END) as Caudal,
               MAX(CASE WHEN NAME = %s THEN VALUE END) as Presion
        FROM (
            SELECT T1.FECHA, T2.NAME, T1.VALUE FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN (%s, %s) AND T1.FECHA >= %s
        ) AS subquery GROUP BY FECHA ORDER BY FECHA ASC
    """
    cursor.execute(query, (tag_cau, tag_pre, tag_cau, tag_pre, inicio))
    return pd.DataFrame(cursor.fetchall(), columns=['FECHA', 'Caudal', 'Presion'])

# --- GENERADOR DE GRÁFICAS (Espejo de tu función de Plotly) ---
def renderizar_grafica_miia(pozo_id, df):
    fig = go.Figure()
    # Caudal - Color Cian original
    fig.add_trace(go.Scatter(x=df['FECHA'], y=df['Caudal'], name='Caudal (l/s)', line=dict(color='#00CED1', width=2), yaxis='y1'))
    # Presión - Color Naranja original
    fig.add_trace(go.Scatter(x=df['FECHA'], y=df['Presion'], name='Presión (kg/cm²)', line=dict(color='#FF8C00', width=2), yaxis='y2'))
    
    fig.update_layout(
        title=f"ANÁLISIS TÉCNICO: {pozo_id}",
        template="plotly_dark",
        xaxis=dict(title="Historial 7 Días"),
        yaxis=dict(title="Caudal (l/s)", titlefont=dict(color="#00CED1"), tickfont=dict(color="#00CED1")),
        yaxis2=dict(title="Presión (kg/cm²)", overlaying='y', side='right', titlefont=dict(color="#FF8C00"), tickfont=dict(color="#FF8C00")),
        legend=dict(orientation="h", y=1.1)
    )
    st.plotly_chart(fig, use_container_width=True)

# --- INTERFAZ ---
st.title("🛰️ Sistema de Monitoreo MIAA - Web")

# Sidebar con el selector de pozo para las gráficas
with st.sidebar:
    st.header("Control de Activos")
    pozo_sel = st.selectbox("Seleccionar Pozo para Análisis", list(mapa_pozos_dict.keys()))
    st.divider()
    ver_sectores = st.checkbox("Cargar Sectores GIS (Postgres)", False)

col_map, col_metric = st.columns([3, 1])

with col_map:
    # Centrar mapa en pozo seleccionado si existe
    centro = mapa_pozos_dict[pozo_sel]["coord"] if pozo_sel else (21.8818, -102.2917)
    m = folium.Map(location=centro, zoom_start=14 if pozo_sel else 12, tiles="cartodbpositron")
    Fullscreen().add_to(m)

    # Carga masiva de datos actuales para no saturar
    tags_todos = [info["caudal"] for info in mapa_pozos_dict.values()]
    datos_actuales = obtener_datos_actuales(tags_todos)

    for id_p, info in mapa_pozos_dict.items():
        val, fecha = datos_actuales.get(info["caudal"], (0.0, "N/A"))
        color = "#00CED1" if val > 0.5 else "#FF4B4B"
        folium.CircleMarker(
            location=info["coord"], radius=7, color=color, fill=True,
            popup=f"<b>{id_p}</b><br>Caudal: {val} l/s<br>Refresco: {fecha}"
        ).add_to(m)

    st_folium(m, width="100%", height=600)

with col_metric:
    if pozo_sel:
        st.subheader(f"Detalle {pozo_sel}")
        val, fecha = datos_actuales.get(mapa_pozos_dict[pozo_sel]["caudal"], (0.0, "N/A"))
        st.metric("Caudal Actual", f"{val} l/s", delta="OPERATIVO" if val > 0.5 else "PARADO")
        st.caption(f"Última lectura: {fecha}")

# --- SECCIÓN DE GRÁFICAS DE INGENIERÍA ---
if pozo_sel:
    st.divider()
    with st.spinner(f"Extrayendo históricos de {pozo_sel}..."):
        df_h = obtener_historico_ingenieria(mapa_pozos_dict[pozo_sel]["caudal"], mapa_pozos_dict[pozo_sel]["presion"])
        renderizar_grafica_miia(pozo_sel, df_h)
