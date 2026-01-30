import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Análisis de Pozos", layout="wide")

# Estilo visual original (#0b1a29)
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stSelectbox label { color: #00CED1 !important; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# --- CREDENCIALES ---
config = {'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'host': 'miaa.mx', 'database': 'miaamx_telemetria'}

# --- DICCIONARIO DE POZOS (Extraído de tu Respaldo 2) ---
# Aquí solo pongo una muestra, pega aquí tu mapa_pozos_dict completo.
mapa_pozos_dict = {
    "P002": {
        "caudal": "PZ_002_TRC_CAU_INS", 
        "presion": "PZ_002_TRC_PRES_INS",
        "voltajes": ["PZ_002_TRC_VOL_L1_L2", "PZ_002_TRC_VOL_L2_L3", "PZ_002_TRC_VOL_L1_L3"],
        "corrientes": ["PZ_002_TRC_AMP_L1", "PZ_002_TRC_AMP_L2", "PZ_002_TRC_AMP_L3"]
    },
    "P003": {
        "caudal": "PZ_003_CAU_INS", 
        "presion": "PZ_003_PRES_INS",
        "voltajes": ["PZ_003_VOL_L1_L2", "PZ_003_VOL_L2_L3", "PZ_003_VOL_L1_L3"],
        "corrientes": ["PZ_003_AMP_L1", "PZ_003_AMP_L2", "PZ_003_AMP_L3"]
    }
}

# --- MOTOR DE CONSULTA HISTÓRICA ---
def obtener_historico_pozo(pozo_id):
    info = mapa_pozos_dict[pozo_id]
    tags = [info["caudal"], info["presion"]] + info["voltajes"] + info["corrientes"]
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        format_strings = ','.join(['%s'] * len(tags))
        fecha_inicio = datetime.now() - timedelta(days=7)
        
        query = f"""
            SELECT FECHA, T2.NAME, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN ({format_strings}) AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        cursor.execute(query, (fecha_inicio,))
        data = cursor.fetchall()
        conn.close()
        
        df = pd.DataFrame(data, columns=['FECHA', 'Variable', 'Valor'])
        return df.pivot(index='FECHA', columns='Variable', values='Valor').reset_index()
    except Exception as e:
        st.error(f"Error en DB: {e}")
        return pd.DataFrame()

# --- INTERFAZ DE SELECCIÓN ---
st.title("📊 Análisis Técnico de Pozos")

with st.sidebar:
    st.header("Listado de Pozos")
    pozo_seleccionado = st.selectbox("Seleccione un pozo para ver información:", list(mapa_pozos_dict.keys()))

if pozo_seleccionado:
    st.subheader(f"Histórico de Variables: {pozo_seleccionado}")
    
    with st.spinner("Consultando datos históricos..."):
        df = obtener_historico_pozo(pozo_seleccionado)
        
        if not df.empty:
            info = mapa_pozos_dict[pozo_seleccionado]
            
            # --- GRÁFICA 1: CAUDAL Y PRESIÓN ---
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(x=df['FECHA'], y=df[info["caudal"]], name="Caudal (l/s)", line=dict(color='#00CED1')))
            fig1.add_trace(go.Scatter(x=df['FECHA'], y=df[info["presion"]], name="Presión (kg/cm²)", line=dict(color='#FF8C00'), yaxis="y2"))
            
            fig1.update_layout(
                template="plotly_dark", title="Caudal vs Presión",
                yaxis=dict(title="Caudal (l/s)", titlefont=dict(color="#00CED1"), tickfont=dict(color="#00CED1")),
                yaxis2=dict(title="Presión (kg/cm²)", overlaying='y', side='right', titlefont=dict(color="#FF8C00"), tickfont=dict(color="#FF8C00"))
            )
            st.plotly_chart(fig1, use_container_width=True)

            # --- GRÁFICA 2: ELÉCTRICOS (VOLTAJES) ---
            fig2 = go.Figure()
            for v_tag in info["voltajes"]:
                if v_tag in df.columns:
                    fig2.add_trace(go.Scatter(x=df['FECHA'], y=df[v_tag], name=v_tag.split('_')[-2:]))
            
            fig2.update_layout(template="plotly_dark", title="Monitoreo Eléctrico (Voltajes L-L)", yaxis_title="Voltios (V)")
            st.plotly_chart(fig2, use_container_width=True)
            
        else:
            st.warning("No se encontraron datos para este pozo en los últimos 7 días.")
