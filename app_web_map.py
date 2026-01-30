import streamlit as st
import folium
from folium.plugins import Fullscreen
import os
import base64
import mysql.connector

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="MIAA - Generador de Mapa", layout="centered")

# Estilo para que se vea profesional como tu proyecto
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; }
    .btn-abrir {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background-color: #00CED1;
        color: #0b1a29 !important;
        padding: 15px 25px;
        font-weight: bold;
        text-decoration: none;
        border-radius: 5px;
        width: 100%;
        margin-top: 10px;
    }
    </style>
""", unsafe_allow_html=True)

# --- TUS CONFIGURACIONES (Pega aquí lo de tu respaldo) ---
config = {'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'host': 'miaa.mx', 'database': 'miaamx_telemetria'}

# PEGA AQUÍ TUS DICCIONARIOS (mapa_pozos_dict, etc.)
mapa_pozos_dict = {"P002": {"coord": (21.88229, -102.31542), "caudal": "PZ_002_TRC_CAU_INS"}}

def generar_mapa_html():
    # Creamos el mapa con tu lógica
    m = folium.Map(location=[21.8818, -102.2917], zoom_start=12)
    Fullscreen().add_to(m)
    
    # Simulación de tu lógica de BD (aquí va tu bucle real)
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        for id_p, info in mapa_pozos_dict.items():
            # ... tu lógica de marcado ...
            folium.Marker(location=info["coord"], popup=id_p).add_to(m)
        conn.close()
    except:
        pass
        
    # Guardamos temporalmente
    nombre_archivo = "mapa_miaa.html"
    m.save(nombre_archivo)
    return nombre_archivo

# --- INTERFAZ ---
st.title("🛰️ MONITOR DE MAPA MIAA")

if st.button("🚀 GENERAR MAPA ACTUALIZADO"):
    with st.spinner("Procesando datos de ingeniería..."):
        archivo = generar_mapa_html()
        
        # Leemos el archivo para convertirlo en un link de apertura
        with open(archivo, "rb") as f:
            html_bytes = f.read()
            b64 = base64.b64encode(html_bytes).decode()
            
        st.success("Mapa generado correctamente.")
        
        # ESTO ES LO QUE ABRE LA VENTANA: Un link real de HTML
        href = f'<a href="data:text/html;base64,{b64}" target="_blank" class="btn-abrir">CLIC AQUÍ PARA ABRIR MAPA EN OTRA PESTAÑA</a>'
        st.markdown(href, unsafe_allow_html=True)

st.info("Nota: Al dar clic en el botón verde, se abrirá tu mapa con toda la configuración de Folium en una pestaña nueva.")
