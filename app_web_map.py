import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Análisis Técnico", layout="wide")

# Estilo visual MIAA (#0b1a29)
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stSelectbox label { color: #00CED1 !important; font-size: 18px; font-weight: bold; }
    .stAlert { background-color: #162a3d; border: 1px solid #00CED1; }
    </style>
""", unsafe_allow_html=True)

# --- CREDENCIALES ---
db_config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}

# --- DICCIONARIOS (Copia aquí tu lista completa del archivo original) ---
mapa_pozos_dict = {
    "P002": {"caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS"},
    "P003": {"caudal": "PZ_003_CAU_INS", "presion": "PZ_003_PRES_INS"},
    "P004": {"caudal": "PZ_004_CAU_INS", "presion": "PZ_004_PRES_INS"},
    # ... pega el resto aquí
}

# --- MOTOR DE DATOS ---
def fetch_data(pozo_id):
    info = mapa_pozos_dict[pozo_id]
    tags = [info["caudal"], info["presion"]]
    
    try:
        conn = mysql.connector.connect(**db_config)
        # Usamos una ventana de 7 días exactos
        fecha_fin = datetime.now()
        fecha_ini = fecha_fin - timedelta(days=7)
        
        query = """
            SELECT T1.FECHA, T2.NAME, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN (%s, %s) AND T1.FECHA BETWEEN %s AND %s
            ORDER BY T1.FECHA ASC
        """
        
        df_raw = pd.read_sql(query, conn, params=(tags[0], tags[1], fecha_ini, fecha_fin))
        conn.close()

        if df_raw.empty:
            return pd.DataFrame()

        # Pivoteamos los datos para tener columnas: FECHA, Caudal, Presion
        df_pivot = df_raw.pivot(index='FECHA', columns='NAME', values='VALUE').reset_index()
        
        # Renombramos dinámicamente según los tags del pozo
        df_pivot = df_pivot.rename(columns={info["caudal"]: "Caudal", info["presion"]: "Presion"})
        return df_pivot

    except Exception as e:
        st.error(f"Error técnico en DB: {e}")
        return pd.DataFrame()

# --- INTERFAZ ---
st.title("📊 Análisis de Ingeniería de Pozos")

# Lista desplegable de pozos
pozo_sel = st.selectbox("Seleccione un pozo para ver su comportamiento:", list(mapa_pozos_dict.keys()))

if pozo_sel:
    with st.spinner(f"Extrayendo históricos de {pozo_sel}..."):
        df_final = fetch_data(pozo_sel)
        
        if not df_final.empty:
            # Construcción de la gráfica de doble eje
            fig = go.Figure()

            # CAUDAL (Eje Izquierdo)
            if 'Caudal' in df_final.columns:
                fig.add_trace(go.Scatter(
                    x=df_final['FECHA'], y=df_final['Caudal'],
                    name="Caudal (l/s)", line=dict(color='#00CED1', width=2),
                    fill='tozeroy', fillcolor='rgba(0, 206, 209, 0.1)'
                ))

            # PRESIÓN (Eje Derecho)
            if 'Presion' in df_final.columns:
                fig.add_trace(go.Scatter(
                    x=df_final['FECHA'], y=df_final['Presion'],
                    name="Presión (kg/cm²)", line=dict(color='#FF8C00', width=2),
                    yaxis="y2"
                ))

            # Diseño de ingeniería
            fig.update_layout(
                template="plotly_dark",
                hovermode="x unified",
                xaxis=dict(title="Tiempo (Últimos 7 días)", gridcolor="#333"),
                yaxis=dict(
                    title="Caudal (l/s)", titlefont=dict(color="#00CED1"), 
                    tickfont=dict(color="#00CED1"), gridcolor="#333"
                ),
                yaxis2=dict(
                    title="Presión (kg/cm²)", titlefont=dict(color="#FF8C00"), 
                    tickfont=dict(color="#FF8C00"), overlaying='y', side='right', gridcolor="#333"
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1)
            )

            st.plotly_chart(fig, use_container_width=True)
            
            # Tabla de auditoría
            with st.expander("Ver bitácora de datos crudos"):
                st.write(df_final.tail(100))
        else:
            st.warning(f"⚠️ El pozo **{pozo_sel}** no tiene registros en `VfiTagNumHistory` para los últimos 7 días. Verifique que los tags coincidan exactamente en la tabla `VfiTagRef`.")

st.divider()
st.caption("MIAA - Sistema de Análisis de Telemetría v2026")
