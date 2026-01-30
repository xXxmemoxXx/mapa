import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import Fullscreen, LocateControl
import mysql.connector
import pandas as pd
import base64
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Sistema de Monitoreo", layout="wide")

# CSS para mantener tu identidad visual oscura
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stButton>button { 
        background-color: #00CED1 !important; color: #0b1a29 !important; 
        font-weight: bold; width: 100%; height: 3em; border-radius: 8px;
    }
    .console-box {
        background-color: #0c0c0c; color: #00ff00; padding: 15px;
        font-family: 'Consolas', monospace; border-radius: 5px;
        height: 200px; overflow-y: auto; border: 1px solid #333;
    }
    </style>
""", unsafe_allow_html=True)

# --- CREDENCIALES (TAL CUAL TU ARCHIVO) ---
config = {'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'host': 'miaa.mx', 'database': 'miaamx_telemetria'}

# --- TUS DICCIONARIOS (Copia aquí tus 200+ pozos del respaldo) ---
mapa_pozos_dict = {
    "P002": {"coord": (21.88229, -102.31542), "caudal": "PZ_002_TRC_CAU_INS"},
    "P003": {"coord": (21.88603, -102.26653), "caudal": "PZ_003_CAU_INS"},
    # PEGA AQUÍ EL RESTO...
}

# --- MOTOR DE DATOS ---
def obtener_caudales_reales():
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

# --- INTERFAZ ---
st.title("🛰️ MONITOREO TÉCNICO MIAA")

# Botón de Inicio
if st.button("🚀 INICIAR MONITOREO DE MAPA"):
    # 1. Crear el Mapa
    m = folium.Map(location=[21.8818, -102.2917], zoom_start=12, tiles="cartodbpositron")
    Fullscreen().add_to(m)
    LocateControl().add_to(m)

    # 2. Tu lógica de ingeniería para pintar pozos
    datos = obtener_caudales_reales()
    for id_p, info in mapa_pozos_dict.items():
        val = datos.get(info["caudal"], 0.0)
        color_p = "#00CED1" if val > 0.5 else "#FF4B4B"
        folium.CircleMarker(
            location=info["coord"], radius=8, color=color_p, fill=True,
            popup=f"<b>{id_p}</b><br>Caudal: {val} l/s"
        ).add_to(m)

    # 3. MOSTRAR EL MAPA EN PANTALLA (Aquí es donde se ve el mapa)
    st_folium(m, width="100%", height=600)
    
    # 4. Opción para descargar/abrir HTML
    m.save("mapa_miaa.html")
    st.success("Mapa renderizado con éxito.")

# --- CONSOLA ---
st.write("### 📜 Registro de Eventos")
st.markdown(f"""
    <div class="console-box">
    [{datetime.now().strftime('%H:%M:%S')}] ✅ Sistema Web Listo.<br>
    [{datetime.now().strftime('%H:%M:%S')}] Esperando clic en el botón para mostrar el mapa interactivo...
    </div>
""", unsafe_allow_html=True)
