import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import Fullscreen, LocateControl
import pandas as pd
import mysql.connector
import psycopg2
import json
import plotly.graph_objects as go
from datetime import datetime, timedelta
import concurrent.futures

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Sistema de Telemetría", layout="wide", page_icon="🛰️")

# Estilo Dark Mode MIAA
st.markdown("""
    <style>
    .main { background-color: #0b1a29; color: white; }
    .stMetric { background-color: #162a3d !important; border: 1px solid #00CED1 !important; border-radius: 10px; padding: 10px; }
    div[data-testid="stExpander"] { background-color: #162a3d; border: none; }
    </style>
    """, unsafe_allow_html=True)

# --- CONFIGURACIONES DE BASES DE DATOS (Tus credenciales reales) ---
CONFIG_POZOS = {'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'host': 'miaa.mx', 'database': 'miaamx_telemetria'}
CONFIG_MACRO = {'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'host': 'miaa.mx', 'database': 'miaamx_telemetria2'}
CONFIG_POSTGRES = {'user': 'map_tecnica', 'password': 'M144.Tec', 'host': 'ti.miaa.mx', 'database': 'qgis'}

# --- TUS DICCIONARIOS (He puesto los ejemplos de tu código, pega aquí el resto) ---
mapa_pozos_dict = {
    "P002": {"coord": (21.88229, -102.31542), "caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS", "voltajes_l": ["PZ_002_TRC_VOL_L1_L2", "PZ_002_TRC_VOL_L2_L3", "PZ_002_TRC_VOL_L1_L3"]},
    "P003": {"coord": (21.88603, -102.26653), "caudal": "PZ_003_CAU_INS", "presion": "PZ_003_PRES_INS", "voltajes_l": ["PZ_003_VOL_L1_L2", "PZ_003_VOL_L2_L3", "PZ_003_VOL_L1_L3"]},
}

mapa_tanques_dict = {
    "TQ003 (P145 IV Centenario)": {"coord": (21.870261,-102.280607), "nivel_tag": "TQ_T_17A_DR_NIV", "nivel_max": 7.5},
}

# --- FUNCIONES DE EXTRACCIÓN DE DATOS ---
@st.cache_resource
def get_mysql_conn(conf):
    return mysql.connector.connect(**conf, autocommit=True)

def fetch_realtime_data(tags, conf):
    conn = get_mysql_conn(conf)
    if not conn: return {}
    cursor = conn.cursor()
    format_strings = ','.join(['%s'] * len(tags))
    query = f"SELECT T2.NAME, T1.VALUE, T1.FECHA FROM VfiTagNumHistory_Ultimo T1 JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID WHERE T2.NAME IN ({format_strings})"
    try:
        cursor.execute(query, tuple(tags))
        return {row[0]: (float(row[1]), row[2]) for row in cursor.fetchall()}
    except: return {}

def obtener_datos_historicos_web(tag_caudal, tag_presion):
    conn = get_mysql_conn(CONFIG_POZOS)
    cursor = conn.cursor()
    fin = datetime.now()
    inicio = fin - timedelta(days=7)
    
    query = """
        SELECT FECHA, 
               MAX(CASE WHEN NAME = %s THEN VALUE END) as Caudal,
               MAX(CASE WHEN NAME = %s THEN VALUE END) as Presion
        FROM (
            SELECT T1.FECHA, T2.NAME, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN (%s, %s) AND T1.FECHA BETWEEN %s AND %s
        ) AS subquery
        GROUP BY FECHA ORDER BY FECHA ASC
    """
    cursor.execute(query, (tag_caudal, tag_presion, tag_caudal, tag_presion, inicio, fin))
    df = pd.DataFrame(cursor.fetchall(), columns=['FECHA', 'Caudal', 'Presion'])
    return df

# --- FUNCIÓN DE GRÁFICA (Tu lógica de Plotly) ---
def generar_grafica_web(pozo_id, df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['FECHA'], y=df['Caudal'], name='Caudal (l/s)', line=dict(color='#00CED1', width=2), yaxis='y1'))
    fig.add_trace(go.Scatter(x=df['FECHA'], y=df['Presion'], name='Presión (kg/cm²)', line=dict(color='#FF8C00', width=2), yaxis='y2'))
    
    fig.update_layout(
        title=f"Histórico 7 Días - {pozo_id}",
        template="plotly_dark",
        yaxis=dict(title="Caudal (l/s)", titlefont=dict(color="#00CED1"), tickfont=dict(color="#00CED1")),
        yaxis2=dict(title="Presión (kg/cm²)", overlaying='y', side='right', titlefont=dict(color="#FF8C00"), tickfont=dict(color="#FF8C00")),
        legend=dict(orientation="h", y=1.1)
    )
    st.plotly_chart(fig, use_container_width=True)

# --- INTERFAZ PRINCIPAL ---
st.title("🛰️ Monitoreo MIAA - Telemetría Avanzada")

with st.sidebar:
    st.header("Capas del Mapa")
    ver_pozos = st.checkbox("Mostrar Pozos", True)
    ver_tanques = st.checkbox("Mostrar Tanques", True)
    st.divider()
    pozo_foco = st.selectbox("🎯 Enfocar Pozo / Ver Gráfica", ["Seleccionar..."] + list(mapa_pozos_dict.keys()))

col_map, col_info = st.columns([3, 1])

with col_map:
    # Coordenadas iniciales
    lat, lon = (21.8818, -102.2917)
    if pozo_foco != "Seleccionar...":
        lat, lon = mapa_pozos_dict[pozo_foco]["coord"]

    m = folium.Map(location=[lat, lon], zoom_start=14 if pozo_foco != "Seleccionar..." else 12, tiles="cartodbpositron")
    Fullscreen().add_to(m)

    if ver_pozos:
        tags_p = [info["caudal"] for info in mapa_pozos_dict.values()]
        data_p = fetch_realtime_data(tags_p, CONFIG_POZOS)
        
        for id_p, info in mapa_pozos_dict.items():
            val, fecha = data_p.get(info["caudal"], (0.0, "S/D"))
            color = "#00CED1" if val > 0.5 else "#FF4B4B"
            folium.CircleMarker(
                location=info["coord"], radius=8, color=color, fill=True,
                popup=f"<b>{id_p}</b><br>Caudal: {val} l/s<br>{fecha}"
            ).add_to(m)

    # Render del mapa
    st_folium(m, width="100%", height=600, key="main_map")

with col_info:
    st.subheader("Estado Local")
    if pozo_foco != "Seleccionar...":
        info = mapa_pozos_dict[pozo_foco]
        val, fecha = fetch_realtime_data([info["caudal"]], CONFIG_POZOS).get(info["caudal"], (0.0, "N/A"))
        st.metric(pozo_foco, f"{val} l/s")
        st.write(f"🕒 {fecha}")
        if val < 0.5: st.error("⚠️ Pozo fuera de línea")
    else:
        st.info("Selecciona un pozo en la lista de la izquierda para ver su detalle.")

# --- SECCIÓN DE GRÁFICAS (Solo aparece al seleccionar pozo) ---
if pozo_foco != "Seleccionar...":
    st.divider()
    with st.spinner(f"Consultando históricos de {pozo_foco}..."):
        df_h = obtener_datos_historicos_web(mapa_pozos_dict[pozo_foco]["caudal"], mapa_pozos_dict[pozo_foco]["presion"])
        generar_grafica_web(pozo_foco, df_h)
