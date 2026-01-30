import streamlit as st
import pandas as pd
import mysql.connector
import folium
import base64
import os
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Telemetría Web", layout="wide")

st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stSelectbox label { color: #00CED1 !important; font-size: 20px; font-weight: bold; }
    .stButton>button { background-color: #00CED1; color: #0b1a29; font-weight: bold; border-radius: 5px; }
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
# CONFIGURACIONES Y DICCIONARIOS (EXTRAÍDOS DE TU RESPALDO 2)
# ==============================================================================
config = {'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'host': 'miaa.mx', 'database': 'miaamx_telemetria'}

# PEGA AQUÍ TU mapa_pozos_dict COMPLETO. He dejado estos de ejemplo:
mapa_pozos_dict = {
    "P002": {"coord": (21.88229, -102.31542), "caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS", "voltajes": ["PZ_002_TRC_VOL_L1_L2", "PZ_002_TRC_VOL_L2_L3", "PZ_002_TRC_VOL_L1_L3"]},
    "P003": {"coord": (21.88603, -102.26653), "caudal": "PZ_003_CAU_INS", "presion": "PZ_003_PRES_INS", "voltajes": ["PZ_003_VOL_L1_L2", "PZ_003_VOL_L2_L3", "PZ_003_VOL_L1_L3"]}
}

# ==============================================================================
# MOTOR DE DATOS
# ==============================================================================

def consulta_historica(pozo_id):
    info = mapa_pozos_dict[pozo_id]
    tags = [info["caudal"], info["presion"]] + info.get("voltajes", [])
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        f_inicio = datetime.now() - timedelta(days=7)
        
        # Query idéntica a tu lógica de respaldo
        format_strings = ','.join(['%s'] * len(tags))
        query = f"""
            SELECT T1.FECHA, T2.NAME, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN ({format_strings}) AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        cursor.execute(query, tuple(tags + [f_inicio]))
        df = pd.DataFrame(cursor.fetchall(), columns=['FECHA', 'Variable', 'Valor'])
        conn.close()
        
        if df.empty: return pd.DataFrame()
        # Pivoteamos para que cada variable sea una columna
        return df.pivot_table(index='FECHA', columns='Variable', values='Valor').reset_index()
    except: return pd.DataFrame()

# ==============================================================================
# INTERFAZ PRINCIPAL
# ==============================================================================
st.title("🛰️ Sistema de Monitoreo MIAA")

tab1, tab2 = st.tabs(["🗺️ Mapa Operativo", "📊 Análisis de Pozo"])

with tab1:
    st.info("Presiona el botón para procesar todos los pozos y generar el mapa interactivo.")
    if st.button("🚀 GENERAR MAPA EN VIVO"):
        m = folium.Map(location=[21.8818, -102.2917], zoom_start=12)
        # (Aquí va tu lógica de CircleMarkers que ya conoces)
        m.save("mapa_web.html")
        with open("mapa_web.html", "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        st.markdown(f'<a href="data:text/html;base64,{b64}" target="_blank" style="background-color:#00CED1; color:#0b1a29; padding:15px; border-radius:5px; text-decoration:none; font-weight:bold;">VER MAPA EN PESTAÑA NUEVA</a>', unsafe_allow_html=True)

with tab2:
    pozo_sel = st.selectbox("Seleccione Pozo del Listado:", list(mapa_pozos_dict.keys()))
    
    if pozo_sel:
        df_h = consulta_historica(pozo_sel)
        
        if not df_h.empty:
            info_p = mapa_pozos_dict[pozo_sel]
            
            # Gráfica Hidráulica (Caudal/Presión)
            fig = go.Figure()
            if info_p["caudal"] in df_h.columns:
                fig.add_trace(go.Scatter(x=df_h['FECHA'], y=df_h[info_p["caudal"]], name="Caudal (l/s)", line=dict(color='#00CED1')))
            if info_p["presion"] in df_h.columns:
                fig.add_trace(go.Scatter(x=df_h['FECHA'], y=df_h[info_p["presion"]], name="Presión (kg/cm²)", yaxis="y2", line=dict(color='#FF8C00')))
            
            fig.update_layout(
                template="plotly_dark", title=f"Análisis Hidráulico - {pozo_sel}",
                yaxis=dict(title="Caudal (l/s)", titlefont=dict(color="#00CED1"), tickfont=dict(color="#00CED1")),
                yaxis2=dict(title="Presión (kg/cm²)", overlaying='y', side='right', titlefont=dict(color="#FF8C00"), tickfont=dict(color="#FF8C00"))
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Gráfica Eléctrica
            fig_e = go.Figure()
            for v in info_p.get("voltajes", []):
                if v in df_h.columns:
                    fig_e.add_trace(go.Scatter(x=df_h['FECHA'], y=df_h[v], name=v))
            fig_e.update_layout(template="plotly_dark", title="Variables Eléctricas", yaxis_title="Voltaje (V)")
            st.plotly_chart(fig_e, use_container_width=True)
        else:
            st.error("No hay datos recientes en la base de datos para este pozo.")
