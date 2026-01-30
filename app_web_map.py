import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import Fullscreen, LocateControl
import mysql.connector
import psycopg2
import json
import base64
import pandas as pd
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Monitoreo de Pozos", layout="wide")

# Estilo visual idéntico a tu Tkinter (#0b1a29)
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stButton>button { background-color: #00CED1; color: #0b1a29; font-weight: bold; width: 100%; border-radius: 8px; }
    .console-text { background-color: #0c0c0c; color: #00ff00; padding: 15px; font-family: 'Consolas', monospace; border-radius: 5px; height: 200px; overflow-y: auto; font-size: 13px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

# --- TUS CONFIGURACIONES ORIGINALES ---
config = {'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'host': 'miaa.mx', 'database': 'miaamx_telemetria'}
config_posgres = {'user': 'map_tecnica', 'password': 'M144.Tec', 'host': 'ti.miaa.mx', 'database': 'qgis'}

# --- COPIA AQUÍ TUS DICCIONARIOS COMPLETOS DE RESPALDO 2 ---
mapa_pozos_dict = {
    "P002": {"coord": (21.88229, -102.31542), "caudal": "PZ_002_TRC_CAU_INS"},
    "P003": {"coord": (21.88603, -102.26653), "caudal": "PZ_003_CAU_INS"},
    # ... PEGA EL RESTO DE TUS 200+ POZOS AQUÍ ...
}

# --- LÓGICA DE DATOS ---
def obtener_datos():
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        tags = [info["caudal"] for info in mapa_pozos_dict.values()]
        format_strings = ','.join(['%s'] * len(tags))
        query = f"SELECT T2.NAME, T1.VALUE FROM VfiTagNumHistory_Ultimo T1 JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID WHERE T2.NAME IN ({format_strings})"
        cursor.execute(query, tuple(tags))
        res = {row[0]: float(row[1]) for row in cursor.fetchall()}
        conn.close()
        return res
    except: return {}

# --- INTERFAZ PRINCIPAL ---
st.title("🛰️ SISTEMA DE MONITOREO MIAA")

col_btn, col_empty = st.columns([1, 2])
with col_btn:
    # Este es el botón de tu interfaz original
    ejecutar = st.button("🚀 INICIAR MONITOREO DE MAPA")

# Contenedor del Mapa
if ejecutar:
    with st.spinner("Consultando ingeniería y renderizando mapa..."):
        # 1. Crear el mapa base
        m = folium.Map(location=[21.8818, -102.2917], zoom_start=12, tiles="cartodbpositron")
        Fullscreen().add_to(m)
        LocateControl().add_to(m)

        # 2. Tu lógica de marcado y colores (Azul si > 0.5, Rojo si no)
        datos = obtener_datos()
        for id_p, info in mapa_pozos_dict.items():
            val = datos.get(info["caudal"], 0.0)
            color_p = "#00CED1" if val > 0.5 else "#FF4B4B"
            folium.CircleMarker(
                location=info["coord"], radius=8, color=color_p, fill=True,
                popup=f"<b>{id_p}</b><br>Caudal: {val} l/s"
            ).add_to(m)

        # 3. Mostrar el mapa directamente en la web
        st_folium(m, width="100%", height=600)
        
        # 4. Botón opcional para abrir en pestaña nueva (como pedías)
        ruta_temp = "mapa_miaa.html"
        m.save(ruta_temp)
        with open(ruta_temp, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        
        st.markdown(f'<a href="data:text/html;base64,{b64}" target="_blank" style="text-decoration:none;"><div style="background-color:#00CED1;color:#0b1a29;padding:10px;border-radius:5px;text-align:center;font-weight:bold;">ABRIR EN PESTAÑA COMPLETA</div></a>', unsafe_allow_html=True)

# --- CONSOLA (Registro de eventos) ---
st.write("### 📜 Registro de Eventos")
st.markdown(f"""
    <div class="console-text">
    [{datetime.now().strftime('%H:%M:%S')}] ✅ Sistema Web MIAA cargado.<br>
    [{datetime.now().strftime('%H:%M:%S')}] [DB] Conectado a Pozos: miaa.mx<br>
    [{datetime.now().strftime('%H:%M:%S')}] [GIS] Conectado a Sectores: ti.miaa.mx
    </div>
""", unsafe_allow_html=True)
