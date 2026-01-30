import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Análisis de Pozos", layout="wide")

# Estilo visual MIAA (Oscuro y Cian)
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stSelectbox label { color: #00CED1 !important; font-size: 18px; font-weight: bold; }
    h1, h3 { color: #00CED1; text-align: center; }
    </style>
""", unsafe_allow_html=True)

# --- CREDENCIALES ---
db_config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}

# --- DICCIONARIO DE EJEMPLO (Aquí pega tu lista completa de Respaldo 2) ---
mapa_pozos_dict = {
    "P002": {"caudal": "PZ_002_TRC_CAU_INS", "presion": "PZ_002_TRC_PRES_INS"},
    "P003": {"caudal": "PZ_003_CAU_INS", "presion": "PZ_003_PRES_INS"},
    "P004": {"caudal": "PZ_004_CAU_INS", "presion": "PZ_004_PRES_INS"},
}

# --- FUNCIÓN DE EXTRACCIÓN DE DATOS ---
def obtener_historicos(pozo_id):
    info = mapa_pozos_dict[pozo_id]
    tags = [info["caudal"], info["presion"]]
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Consultar últimos 7 días
        hace_una_semana = datetime.now() - timedelta(days=7)
        
        query = """
            SELECT T1.FECHA, T2.NAME, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN (%s, %s) AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        cursor.execute(query, (tags[0], tags[1], hace_una_semana))
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return pd.DataFrame()

        # Convertir a DataFrame y pivotear
        df = pd.DataFrame(rows, columns=['FECHA', 'TAG', 'VALOR'])
        df_pivot = df.pivot_table(index='FECHA', columns='TAG', values='VALOR').reset_index()
        
        # Mapear nombres para la gráfica
        df_pivot = df_pivot.rename(columns={info["caudal"]: "Caudal", info["presion"]: "Presion"})
        return df_pivot

    except Exception as e:
        st.error(f"Error en base de datos: {e}")
        return pd.DataFrame()

# --- INTERFAZ ---
st.title("📊 Análisis de Caudal y Presión")

# Selector
pozo_sel = st.selectbox("Seleccione un pozo del listado:", list(mapa_pozos_dict.keys()))

if pozo_sel:
    with st.spinner(f"Consultando información de {pozo_sel}..."):
        df_final = obtener_historicos(pozo_sel)
        
        if not df_final.empty:
            # Crear Gráfica
            fig = go.Figure()

            # Línea de Caudal
            fig.add_trace(go.Scatter(
                x=df_final['FECHA'], y=df_final['Caudal'],
                name="Caudal (l/s)", line=dict(color='#00CED1', width=2)
            ))

            # Línea de Presión (Eje Derecho)
            fig.add_trace(go.Scatter(
                x=df_final['FECHA'], y=df_final['Presion'],
                name="Presión (kg/cm²)", line=dict(color='#FF8C00', width=2),
                yaxis="y2"
            ))

            # Diseño de la gráfica
            fig.update_layout(
                template="plotly_dark",
                hovermode="x unified",
                yaxis=dict(title="Caudal (l/s)", titlefont=dict(color="#00CED1"), tickfont=dict(color="#00CED1")),
                yaxis2=dict(title="Presión (kg/cm²)", overlaying='y', side='right', titlefont=dict(color="#FF8C00"), tickfont=dict(color="#FF8C00")),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )

            st.plotly_chart(fig, use_container_width=True)
            
            # Mostrar tabla de datos
            with st.expander("Ver registros detallados"):
                st.dataframe(df_final, use_container_width=True)
        else:
            st.error(f"El pozo {pozo_sel} no ha reportado datos en los últimos 7 días.")

st.divider()
st.caption("MIAA - Control de Pozos 2026")
