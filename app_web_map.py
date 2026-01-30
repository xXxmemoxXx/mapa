import streamlit as st
import folium
from folium.plugins import Fullscreen, LocateControl
import webbrowser
import os
import mysql.connector
import psycopg2
import pandas as pd
import json
import plotly.graph_objects as go
from datetime import datetime, timedelta
import concurrent.futures

# --- CONFIGURACIÓN DE LA PÁGINA WEB ---
st.set_page_config(page_title="MIAA - Lanzador de Mapa", page_icon="🛰️", layout="centered")

# Estilo para imitar tu interfaz oscura de Tkinter
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; }
    h1 { color: #00CED1; text-align: center; font-family: Arial; }
    .stButton>button { 
        background-color: #00CED1; color: #0b1a29; font-weight: bold; 
        width: 100%; border-radius: 5px; height: 3em;
    }
    .stButton>button:hover { background-color: #008b8b; color: white; }
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
# AQUÍ VAN TUS CONFIGURACIONES Y DICCIONARIOS (Copiados íntegros de tu respaldo)
# ==============================================================================
config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}

# (Pega aquí todos tus diccionarios: mapa_pozos_dict, mapa_tanques_dict, etc.)
# He dejado este de ejemplo para que veas que el botón funciona
mapa_pozos_dict = {
    "P002": {"coord": (21.88229, -102.31542), "caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS"},
}

# --- LA FUNCIÓN QUE GENERA EL MAPA (Tu lógica original de Folium) ---
def generar_y_lanzar_mapa():
    # 1. Crear el objeto mapa
    m = folium.Map(location=[21.8818, -102.2917], zoom_start=12, tiles="cartodbpositron")
    Fullscreen().add_to(m)
    LocateControl().add_to(m)

    # 2. Tu lógica de conexión y marcado (Simplificada para el ejemplo)
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        for id_p, info in mapa_pozos_dict.items():
            # Esta es tu consulta original a la tabla _Ultimo
            query = f"SELECT T1.VALUE FROM VfiTagNumHistory_Ultimo T1 JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID WHERE T2.NAME = '{info['caudal']}' LIMIT 1"
            cursor.execute(query)
            res = cursor.fetchone()
            val = float(res[0]) if res else 0.0
            
            color = "blue" if val > 0.5 else "red"
            folium.CircleMarker(
                location=info["coord"],
                radius=8,
                color=color,
                fill=True,
                popup=f"<b>{id_p}</b><br>Caudal: {val} l/s"
            ).add_to(m)
        conn.close()
    except Exception as e:
        st.error(f"Error en BD: {e}")

    # 3. GUARDAR Y ABRIR (Exactamente como tu código original)
    nombre_archivo = "mapa_miaa.html"
    ruta_completa = os.path.abspath(nombre_archivo)
    m.save(ruta_completa)
    
    # Esto es lo que abre la pestaña nueva
    webbrowser.open(f"file://{ruta_completa}")
    return ruta_completa

# --- INTERFAZ DE STREAMLIT (Sustituye a tu root.mainloop) ---
st.title("🛰️ SISTEMA DE MONITOREO MIAA")
st.write("")

# El botón que pediste
if st.button("INICIAR MONITOREO DE MAPA"):
    with st.spinner("Procesando datos y generando mapa..."):
        try:
            ruta = generar_y_lanzar_mapa()
            st.success(f"Mapa generado en: {ruta}")
            st.info("Se ha abierto una nueva pestaña con el mapa interactivo.")
        except Exception as e:
            st.error(f"No se pudo abrir el mapa: {e}")

st.write("---")
st.caption("Versión Web de Control de Telemetría - MIAA")
