import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Análisis Técnico", layout="wide")

# Estilo visual de tu proyecto original
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; color: white; }
    .stSelectbox label { color: #00CED1 !important; font-size: 18px; }
    h3 { color: #00CED1; }
    </style>
""", unsafe_allow_html=True)

# --- CREDENCIALES (Respaldo 2) ---
config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}

# --- COPIA AQUÍ TU DICCIONARIO COMPLETO (mapa_pozos_dict) ---
# He incluido estos de ejemplo con la estructura de tu archivo
mapa_pozos_dict = {
    "P002": {
        "caudal": "PZ_002_TRC_CAU_INS", 
        "presion": "PZ_002_TRC_PRES_INS",
        "voltajes": ["PZ_002_TRC_VOL_L1_L2", "PZ_002_TRC_VOL_L2_L3", "PZ_002_TRC_VOL_L1_L3"]
    },
    "P003": {
        "caudal": "PZ_003_CAU_INS", 
        "presion": "PZ_003_PRES_INS",
        "voltajes": ["PZ_003_VOL_L1_L2", "PZ_003_VOL_L2_L3", "PZ_003_VOL_L1_L3"]
    }
}

# --- MOTOR DE DATOS (Consulta Robusta) ---
def obtener_datos_pozo(pozo_id):
    info = mapa_pozos_dict[pozo_id]
    all_tags = [info["caudal"], info["presion"]] + info.get("voltajes", [])
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Consultamos los últimos 5 días para no saturar, pero asegurar datos
        fecha_limite = datetime.now() - timedelta(days=5)
        
        format_strings = ','.join(['%s'] * len(all_tags))
        query = f"""
            SELECT T1.FECHA, T2.NAME, T1.VALUE 
            FROM VfiTagNumHistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T2.NAME IN ({format_strings}) 
            AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        
        cursor.execute(query, tuple(all_tags + [fecha_limite]))
        records = cursor.fetchall()
        conn.close()

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records, columns=['FECHA', 'TAG', 'VALUE'])
        # Pivotar para tener una columna por cada variable
        df_pivot = df.pivot_table(index='FECHA', columns='TAG', values='VALUE').reset_index()
        return df_pivot

    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return pd.DataFrame()

# --- INTERFAZ ---
st.title("🛰️ Panel de Información de Pozos")

# Listado de Pozos
pozo_sel = st.selectbox("Seleccione un Pozo del Listado:", list(mapa_pozos_dict.keys()))

if pozo_sel:
    st.divider()
    with st.spinner(f"Extrayendo información histórica de {pozo_sel}..."):
        df_final = obtener_datos_pozo(pozo_sel)
        
        if not df_final.empty:
            tags_p = mapa_pozos_dict[pozo_sel]
            
            # Gráfica de Caudal y Presión (Ejes combinados)
            fig = go.Figure()
            
            # Caudal (Cian)
            if tags_p["caudal"] in df_final.columns:
                fig.add_trace(go.Scatter(
                    x=df_final['FECHA'], y=df_final[tags_p["caudal"]],
                    name="Caudal (l/s)", line=dict(color='#00CED1', width=2)
                ))
            
            # Presión (Naranja)
            if tags_p["presion"] in df_final.columns:
                fig.add_trace(go.Scatter(
                    x=df_final['FECHA'], y=df_final[tags_p["presion"]],
                    name="Presión (kg/cm²)", line=dict(color='#FF8C00', width=2),
                    yaxis="y2"
                ))

            fig.update_layout(
                title=f"Comportamiento Hidráulico - {pozo_sel}",
                template="plotly_dark",
                hovermode="x unified",
                yaxis=dict(title="Caudal (l/s)", titlefont=dict(color="#00CED1"), tickfont=dict(color="#00CED1")),
                yaxis2=dict(title="Presión (kg/cm²)", overlaying='y', side='right', titlefont=dict(color="#FF8C00"), tickfont=dict(color="#FF8C00")),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Gráfica de Voltajes (Si existen)
            if "voltajes" in tags_p:
                fig_v = go.Figure()
                for v_tag in tags_p["voltajes"]:
                    if v_tag in df_final.columns:
                        fig_v.add_trace(go.Scatter(x=df_final['FECHA'], y=df_final[v_tag], name=v_tag))
                
                fig_v.update_layout(title="Variables Eléctricas (Voltajes)", template="plotly_dark", yaxis_title="Voltios (V)")
                st.plotly_chart(fig_v, use_container_width=True)

        else:
            st.error(f"⚠️ No se encontró información en la base de datos para el pozo {pozo_sel} en los últimos 5 días. Verifique que los tags coincidan con la tabla VfiTagRef.")
