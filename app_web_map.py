import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Telemetría", layout="wide")

# Estilo visual original
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stSelectbox label { color: #00CED1 !important; font-size: 18px; font-weight: bold; }
    h1 { color: #00CED1; text-align: center; }
    </style>
""", unsafe_allow_html=True)

# --- CREDENCIALES ---
config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}

# --- DICCIONARIO DE POZOS (Estructura de tu archivo) ---
# He incluido estos para prueba, aquí pega todos los tuyos.
mapa_pozos_dict = {
    "P002": {"caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS"},
    "P003": {"caudal": "PZ_003_CAU_INS", "presion": "PZ_003_PRES_INS"},
    "P004": {"caudal": "PZ_004_CAU_INS", "presion": "PZ_004_PRES_INS"},
}

def obtener_datos(pozo_id):
    info = mapa_pozos_dict[pozo_id]
    tag_cau = info["caudal"]
    tag_pre = info["presion"]
    
    try:
        conn = mysql.connector.connect(**config)
        # Siete días atrás tal cual tu lógica original
        hace_7_dias = datetime.now() - timedelta(days=7)
        
        # Query exacta de tu respaldo
        query = """
            SELECT T1.FECHA, T2.NAME, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN (%s, %s) AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        
        # Leemos los datos
        df_raw = pd.read_sql(query, conn, params=(tag_cau, tag_pre, hace_7_dias))
        conn.close()

        if df_raw.empty:
            return pd.DataFrame()

        # PIVOTEO: Esto es lo que permite que se vea la gráfica
        df_pivot = df_raw.pivot(index='FECHA', columns='NAME', values='VALUE').reset_index()
        
        # Renombrar para que Plotly las identifique
        df_pivot = df_pivot.rename(columns={tag_cau: 'Caudal', tag_pre: 'Presion'})
        return df_pivot

    except Exception as e:
        st.error(f"Error en conexión MIAA: {e}")
        return pd.DataFrame()

# --- INTERFAZ ---
st.title("📊 Análisis de Ingeniería - MIAA")

# Listado de pozos
seleccion = st.selectbox("Seleccione un pozo para ver caudal y presión:", list(mapa_pozos_dict.keys()))

if seleccion:
    with st.spinner(f"Consultando base de datos para {seleccion}..."):
        df_final = obtener_datos(seleccion)
        
        if not df_final.empty:
            # --- GRÁFICA TÉCNICA ---
            fig = go.Figure()

            # Caudal (Eje Izquierdo)
            if 'Caudal' in df_final.columns:
                fig.add_trace(go.Scatter(
                    x=df_final['FECHA'], y=df_final['Caudal'],
                    name="Caudal (l/s)", line=dict(color='#00CED1', width=2)
                ))

            # Presión (Eje Derecho)
            if 'Presion' in df_final.columns:
                fig.add_trace(go.Scatter(
                    x=df_final['FECHA'], y=df_final['Presion'],
                    name="Presión (kg/cm²)", line=dict(color='#FF8C00', width=2),
                    yaxis="y2"
                ))

            # Configuración de ejes de ingeniería
            fig.update_layout(
                template="plotly_dark",
                hovermode="x unified",
                xaxis=dict(title="Últimos 7 días"),
                yaxis=dict(title="Caudal (l/s)", titlefont=dict(color="#00CED1"), tickfont=dict(color="#00CED1")),
                yaxis2=dict(title="Presión (kg/cm²)", overlaying='y', side='right', 
                            titlefont=dict(color="#FF8C00"), tickfont=dict(color="#FF8C00")),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.error(f"⚠️ El pozo {seleccion} no tiene registros históricos recientes.")

st.divider()
st.caption("MIAA - Departamento de Telemetría")
