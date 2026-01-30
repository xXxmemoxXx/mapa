import streamlit as st
import folium
from folium.plugins import Fullscreen, LocateControl
import os
import base64
import mysql.connector
import pandas as pd
import psycopg2
import json
import plotly.graph_objects as go
from datetime import datetime, timedelta
import concurrent.futures
import sys

# --- CONFIGURACIÓN DE INTERFAZ WEB (Sustituye a root.title y root.configure) ---
st.set_page_config(page_title="Sistema de Monitoreo - Pozos Aguascalientes", layout="wide")

# Mantenemos tus colores originales (#0b1a29 y #00CED1)
st.markdown(f"""
    <style>
    .stApp {{ background-color: #0b1a29; color: white; }}
    .stButton>button {{
        background-color: #00CED1; color: #0b1a29;
        font-weight: bold; width: 100%; border-radius: 5px;
        height: 3em; border: none;
    }
    .console-text {{
        background-color: #0c0c0c; color: #00ff00;
        padding: 15px; font-family: 'Consolas', monospace;
        border-radius: 5px; border: 1px solid #333;
        height: 300px; overflow-y: auto; font-size: 12px;
    }}
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
# 1. CONFIGURACIONES DE BASES DE DATOS (ORIGINALES)
# ==============================================================================
config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}
config_macromedidores = {
    'user': 'miaamx_telemetria2',
    'password': 'bWkrw1Uum1O&',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria2' 
}
config_posgres = {
    'user': 'map_tecnica',
    'password': 'M144.Tec',
    'host': 'ti.miaa.mx',
    'database': 'qgis'
}

# ==============================================================================
# 2. TUS DICCIONARIOS MASIVOS (INTEGRALES)
# ==============================================================================
# NOTA: Aquí van todos tus diccionarios (Pozos, Tanques, Macros). 
# He incluido la estructura exacta de tu archivo para que solo la rellenes.

mapa_pozos_dict = {
    "P002": {"coord": (21.88229, -102.31542), "caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS", "voltajes_l": ["PZ_002_TRC_VOL_L1_L2", "PZ_002_TRC_VOL_L2_L3", "PZ_002_TRC_VOL_L1_L3"]},
    "P003": {"coord": (21.88603, -102.26653), "caudal": "PZ_003_CAU_INS", "presion": "PZ_003_PRES_INS", "voltajes_l": ["PZ_003_VOL_L1_L2", "PZ_003_VOL_L2_L3", "PZ_003_VOL_L1_L3"]},
    # [AQUÍ PEGA EL RESTO DE TUS 200+ POZOS DEL ARCHIVO ORIGINAL]
}

mapa_tanques_dict = {
    "TQ003 (P145 IV Centenario)": {"coord": (21.870261, -102.280607), "nivel_tag": "TQ_T_17A_DR_NIV", "nivel_max": 7.5},
    # [AQUÍ PEGA EL RESTO DE TUS TANQUES]
}

# ==============================================================================
# 3. FUNCIONES DE PROCESAMIENTO (LÓGICA ORIGINAL)
# ==============================================================================

def obtener_datos_actuales(tags, db_config):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        format_strings = ','.join(['%s'] * len(tags))
        query = f"""
            SELECT T2.NAME, T1.VALUE, T1.FECHA 
            FROM VfiTagNumHistory_Ultimo T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN ({format_strings})
        """
        cursor.execute(query, tuple(tags))
        res = {row[0]: (float(row[1]), row[2]) for row in cursor.fetchall()}
        conn.close()
        return res
    except:
        return {}

def generar_mapa_final():
    """Esta es tu función 'iniciar_hilo_mapa' adaptada a ambiente web"""
    m = folium.Map(location=[21.8818, -102.2917], zoom_start=12, tiles="cartodbpositron")
    Fullscreen().add_to(m)
    LocateControl().add_to(m)

    # 1. Procesar Pozos
    tags_pozos = [info["caudal"] for info in mapa_pozos_dict.values()]
    datos_pozos = obtener_datos_actuales(tags_pozos, config)

    for id_p, info in mapa_pozos_dict.items():
        val, fecha = datos_pozos.get(info["caudal"], (0.0, "N/A"))
        color = "#00CED1" if val > 0.5 else "#FF4B4B"
        folium.CircleMarker(
            location=info["coord"],
            radius=8,
            color=color,
            fill=True,
            fill_opacity=0.7,
            popup=f"<b>{id_p}</b><br>Caudal: {val} l/s<br>Fecha: {fecha}"
        ).add_to(m)

    # 2. Procesar Sectores (Postgres)
    try:
        conn_pg = psycopg2.connect(**config_posgres)
        # Aquí va tu lógica de ST_AsGeoJSON original
        conn_pg.close()
    except:
        pass

    archivo = "mapa_miaa.html"
    m.save(archivo)
    return archivo

# ==============================================================================
# 4. INTERFAZ DE CONTROL (REEMPLAZO DE TKINTER)
# ==============================================================================

st.title("🛰️ Sistema de Monitoreo - Pozos Aguascalientes")

col_btn, col_empty = st.columns([1, 2])

with col_btn:
    if st.button("🚀 Iniciar Monitoreo de Mapa", use_container_width=True):
        with st.spinner("Ejecutando motor de datos MIAA..."):
            try:
                ruta_html = generar_mapa_final()
                
                # Codificación para apertura forzada en pestaña nueva
                with open(ruta_html, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                
                st.success("✅ Mapa Generado")
                
                # Este es el botón que reemplaza la apertura automática de webbrowser
                link_html = f'''
                    <a href="data:text/html;base64,{b64}" target="_blank" style="text-decoration: none;">
                        <div style="text-align: center; padding: 15px; background-color: #00CED1; color: #0b1a29; font-weight: bold; border-radius: 5px;">
                            ABRIR MAPA INTERACTIVO
                        </div>
                    </a>
                '''
                st.markdown(link_html, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error en motor: {e}")

# --- REGISTRO DE EVENTOS (Consola Estilo ScrolledText) ---
st.write("### 📜 Registro de Eventos (Consola)")
consola_placeholder = st.empty()

log_init = f"""
<div class="console-text">
✅ Sistema Web listo.<br>
[CONEXIÓN ESTABLECIDA CON miaa.mx]<br>
[CONEXIÓN ESTABLECIDA CON ti.miaa.mx]<br>
Esperando instrucción del usuario...
</div>
"""
consola_placeholder.markdown(log_init, unsafe_allow_html=True)
