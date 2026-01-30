import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE INTERFAZ ---
st.set_page_config(page_title="MIAA - Análisis Hidráulico", layout="wide")

st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stSelectbox label { color: #00CED1 !important; font-size: 20px; font-weight: bold; }
    h1 { color: #00CED1; text-align: center; border-bottom: 2px solid #00CED1; }
    </style>
""", unsafe_allow_html=True)

# --- CREDENCIALES (DE TU RESPALDO) ---
db_config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}

# --- DICCIONARIOS (COPIA AQUÍ TODOS LOS DE TU ARCHIVO ORIGINAL) ---
# He incluido estos para que el código corra de inmediato
mapa_pozos_dict = {
    "P002": {"caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS"},
    "P003": {"caudal": "PZ_003_CAU_INS", "presion": "PZ_003_PRES_INS"},
    "P004": {"caudal": "PZ_004_CAU_INS", "presion": "PZ_004_PRES_INS"},
    # ... PEGA EL RESTO AQUÍ ...
}

# --- MOTOR DE EXTRACCIÓN DE DATOS ---
def fetch_telemetria(pozo_id):
    info = mapa_pozos_dict[pozo_id]
    tag_cau = info["caudal"]
    tag_pre = info["presion"]
    
    try:
        conn = mysql.connector.connect(**db_config)
        
        # Consultamos los últimos 7 días de registros
        fecha_inicio = datetime.now() - timedelta(days=7)
        
        query = """
            SELECT T1.FECHA, T2.NAME as TAG, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN (%s, %s) 
            AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        
        # Cargamos a DataFrame directamente
        df = pd.read_sql(query, conn, params=(tag_cau, tag_pre, fecha_inicio))
        conn.close()

        if df.empty:
            return pd.DataFrame()

        # PIVOTE: Esto es lo que hace que la gráfica funcione
        # Convierte los tags en columnas individuales (Caudal y Presion)
        df_pivot = df.pivot_table(index='FECHA', columns='TAG', values='VALUE').reset_index()
        
        # Renombramos para que Plotly encuentre las columnas siempre igual
        df_pivot = df_pivot.rename(columns={tag_cau: 'Caudal', tag_pre: 'Presion'})
        return df_pivot

    except Exception as e:
        st.error(f"Falla en conexión: {e}")
        return pd.DataFrame()

# --- INTERFAZ ---
st.title("📊 ANÁLISIS TÉCNICO DE POZOS - MIAA")

# 1. LISTADO DESPLEGABLE
pozo_sel = st.selectbox("SELECCIONE UN POZO PARA VER GRÁFICA:", list(mapa_pozos_dict.keys()))

if pozo_sel:
    with st.spinner(f"Extrayendo históricos de {pozo_sel}..."):
        df_final = fetch_telemetria(pozo_sel)
        
        if not df_final.empty:
            # --- CONSTRUCCIÓN DE GRÁFICA ---
            fig = go.Figure()

            # Serie Caudal (Eje Y1)
            fig.add_trace(go.Scatter(
                x=df_final['FECHA'], y=df_final['Caudal'],
                name="Caudal (l/s)",
                line=dict(color='#00CED1', width=2),
                fill='tozeroy' # Efecto de área para mejor visualización
            ))

            # Serie Presión (Eje Y2)
            fig.add_trace(go.Scatter(
                x=df_final['FECHA'], y=df_final['Presion'],
                name="Presión (kg/cm²)",
                line=dict(color='#FF8C00', width=2),
                yaxis="y2"
            ))

            # Formato de Ingeniería (Ejes gemelos)
            fig.update_layout(
                template="plotly_dark",
                hovermode="x unified",
                xaxis=dict(title="Últimos 7 días de monitoreo", gridcolor="#333"),
                yaxis=dict(
                    title="Caudal (l/s)", 
                    titlefont=dict(color="#00CED1"), 
                    tickfont=dict(color="#00CED1")
                ),
                yaxis2=dict(
                    title="Presión (kg/cm²)", 
                    titlefont=dict(color="#FF8C00"), 
                    tickfont=dict(color="#FF8C00"),
                    overlaying='y', 
                    side='right',
                    gridcolor="#444"
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )

            # MOSTRAR GRÁFICA
            st.plotly_chart(fig, use_container_width=True)
            
            # Auditoría de datos
            st.write(f"Última lectura recibida: **{df_final['FECHA'].iloc[-1]}**")
        else:
            st.error(f"❌ Sin datos: El pozo {pozo_sel} no tiene registros históricos en esta semana.")

st.divider()
st.caption("Sistema de Telemetría MIAA - 2026")
