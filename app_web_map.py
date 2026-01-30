import streamlit as st
import folium
import webbrowser
import os
import mysql.connector
import pandas as pd
import psycopg2
import json
import base64
from datetime import datetime, timedelta
import concurrent.futures

# --- CONFIGURACIÓN DE PÁGINA (AMBIENTE WEB) ---
st.set_page_config(page_title="MIAA - Telemetría", layout="centered")

# CSS para mantener los colores de tu diseño original (#0b1a29 y #00CED1)
st.markdown("""
    <style>
    .stApp { background-color: #0b1a29; }
    h1 { color: #00CED1; font-family: 'Arial'; text-align: center; }
    .stButton>button {
        background-color: #00CED1; color: #0b1a29;
        font-weight: bold; width: 100%; border-radius: 5px;
        height: 3em; border: none;
    }
    .stButton>button:hover { background-color: #008b8b; color: white; }
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
# AQUÍ VA TODA TU CONFIGURACIÓN Y DICCIONARIOS (COPIADOS DE TU RESPALDO 2)
# ==============================================================================
config = {'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'host': 'miaa.mx', 'database': 'miaamx_telemetria'}
config_macromedidores = {'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'host': 'miaa.mx', 'database': 'miaamx_telemetria2'}
config_posgres = {'user': 'map_tecnica', 'password': 'M144.Tec', 'host': 'ti.miaa.mx', 'database': 'qgis'}

# --- PEGA AQUÍ TUS DICCIONARIOS MASIVOS (mapa_pozos_dict, mapa_tanques_dict, etc.) ---
# (Usa los mismos que tienes en tu archivo .py original)

# ==============================================================================
# TU LÓGICA DE INGENIERÍA (TAL CUAL TU CÓDIGO)
# ==============================================================================
def reintentar_conexion(retries=3):
    """Intenta conectar a la base de datos de Pozos con reintentos."""
    for i in range(retries):
        try:
            conn = mysql.connector.connect(**config, use_pure=True, autocommit=True)
            return conn
        except mysql.connector.Error as err:
            if err.errno == 1203:
                time.sleep(2 * (i + 1))
            else:
                raise err
    return None

def reintentar_conexion_macrometers(retries=3):
    """Intenta conectar a la base de datos de macromedidores con reintentos."""
    for i in range(retries):
        try:
            conn = mysql.connector.connect(**config_macromedidores, use_pure=True, autocommit=True)
            return conn
        except mysql.connector.Error as err:
            if err.errno == 1203:
                time.sleep(2 * (i + 1))
            else:
                raise err
    return None

def conectar_postgres(config: Dict[str, str]) -> Any:
    """Establece conexión con PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=config['database'],
            user=config['user'],
            password=config['password'],
            host=config['host']
        )
        return conn
    except psycopg2.Error as err:
        print(f"❌ Error de conexión a PostgreSQL: {err}")
        return None

MACROMEDIDORES_TAGS = {
    "43000002":  "43000002",
    "43000012":  "43000012",
    "43000013":  "43000013",
    "43000014":  "43000014",
    "43000015":  "43000015", 
    
    # Agrega aquí más macromedidores: "ID_Medidor": "ID_Medidor"
}

#============================================================================ DICCIONARIO DE COORDENADAS PARA MACROMEDIDORES (ADICIÓN) ========================================================
mapa_macrometros_dict = {
    "43000002": {"coord": (21.83256, -102.31936)},  
    "43000012": {"coord": (21.84923, -102.29378)},  
    "43000013": {"coord": (21.84276, -102.32173)}, 
    "43000014": {"coord": (21.86613, -102.37191)}, 
    "43000015": {"coord": (21.92031, -102.26093)},         
    # Añade coordenadas de los macromedidores restantes
}
#============================================================================ DICCIONARIO DE COORDENADAS Y VARIABLES DE CADA POZO (Completo) ===================================================
mapa_pozos_dict = {
"P002": {
    "coord": (21.88229, -102.31542), "corriente": "PZ_002_TRC_BBA_CRUDO", "caudal": "PZ_002_TRC_CAU_INS", "corrientes_l": ["PZ_002_TRC_CORR_L1", "PZ_002_TRC_CORR_L2", "PZ_002_TRC_CORR_L3"
    ], "presion": "PZ_002_TRC_PRES_INS", "voltajes_l": ["PZ_002_TRC_VOL_L1_L2", "PZ_002_TRC_VOL_L2_L3", "PZ_002_TRC_VOL_L1_L3"
    ], "nivel_estatico": "PZ_002_TRC_NIV_EST", "sumergencia": "PZ_002_TRC_SUMERG", "nivel_tanque": "0", 
},
"P003": {
    "coord": (21.88603, -102.26653), "corriente": "PZ_003_BBA_CRUDO", "caudal": "PZ_003_CAU_INS", "corrientes_l": ["PZ_003_CORR_L1", "PZ_003_CORR_L2", "PZ_003_CORR_L3"
    ], "presion": "PZ_003_PRES_INS", "voltajes_l": ["PZ_003_VOL_L1_L2", "PZ_003_VOL_L2_L3", "PZ_003_VOL_L1_L3"
    ], "nivel_estatico": "PZ_003_NIV_EST", "sumergencia": "PZ_003_SUMERG", "nivel_tanque": "PZ_159_NIV_TQ", 
},
"P004": {
    "coord": (21.86897, -102.30354), "corriente": "PZ_004_BBA_CRUDO", "caudal": "PZ_004_CAU_INS", "corrientes_l": ["PZ_004_CORR_L1", "PZ_004_CORR_L2", "PZ_004_CORR_L3"
    ], "presion": "PZ_004_PRES_INS", "voltajes_l": ["PZ_004_VOL_L1_L2", "PZ_004_VOL_L2_L3", "PZ_004_VOL_L1_L3"
    ], "nivel_estatico": "PZ_004_NIV_EST", "sumergencia": "PZ_004_SUMERG", "nivel_tanque": "0", 
},

}
#============================================================================ DICCIONARIO DE COORDENADAS Y VARIABLES DE CADA TANQUE (ADICIÓN) ==================================================
mapa_tanques_dict = {
"TQ003 (P145 IV Centenario)": {"coord": (21.870261,-102.280607), "nivel_tag": "TQ_T_17A_DR_NIV",  "nivel_max": 7.5,},
"TQ005 (Gremial)": {"coord": (21.894776,-102.285661), "nivel_tag": "PZ_023A_DR_NIV_TQ",  "nivel_max": 9.35,},
"TQ007 (P027A Altavista)": {"coord": (21.89491,-102.307839), "nivel_tag": "PZ_027A_DR_NIV_TQ",  "nivel_max": 7.95,},

}
#============================================================================ DICCIONARIO DE REBOMBEOS  ========================================================================================
mapa_rebombeos_dict = {
"REB203": {"coord": (21.87509, -102.28734), "corriente": "Sin Telemetria",},
"REB204": {"coord": (21.90445, -102.29717), "corriente": "Sin Telemetria",},
"REB205": {"coord": (21.8594801, -102.306544), "corriente": "Sin Telemetria",},
"REB208": {"coord": (21.87271, -102.26086), "corriente": "Sin Telemetria",},
"P210": {"coord": (21.87379, -102.2814), "corriente": "RB_210_I_L1", "corrientes_l":["RB_210_I_L1","RB_210_I_L2","RB_210_I_L3"],"presion":"RB_210_PRES_R","voltajes_l":["RB_210_V_L1","RB_210_V_L2","RB_210_V_L3"], "nivel_tanque": "RB_210_NIV_TQ_R",},
"REB212": {"coord": (21.876064, -102.26405), "corriente": "Sin Telemetria",},
"REB213": {"coord": (21.908707, -102.27409), "corriente": "Sin Telemetria",},

}
def generar_mapa_miaa():
    global LAT_CENTRO, LON_CENTRO 
    # Inicialización del mapa y ZOOM del mapa en el navegador 
    m = folium.Map(location=[LAT_CENTRO, LON_CENTRO], zoom_start=13, tiles="cartodbdarkmatter")

    
# --- INICIO: CÓDIGO PARA QUITAR EL MARCO BLANCO DE LOS POPUPS (SOLICITADO) ---
    css_color_de_fondo = "#0b1a29" 
    css_contorno_blanco = "0 0 0 2px white" 

    css_style = f"""
    <style>
        /* 1. Contenedor principal: Marco blanco y fondo oscuro */
        .leaflet-popup-content-wrapper {{
            background: {css_color_de_fondo} !important; 
            box-shadow: {css_contorno_blanco} !important; 
            padding: 1px;                             
            border-radius: 6px;                       
        }}
        
        /* 2. Punta del popup: Mismo color y contorno */
        .leaflet-popup-tip {{
            background: {css_color_de_fondo} !important; 
            box-shadow: {css_contorno_blanco} !important;
        }}
        
        /* 3. Contenido: Letras blancas */
        .leaflet-popup-content {{
            background: none !important;
            color: white !important; 
        }}
        
        /* 4. CLASE DE ALINEACIÓN: Alinea dato a la izquierda y fecha a la derecha */
        .pozo-line {{
            display: flex; 
            justify-content: space-between; 
            width: 100%; 
            margin-bottom: 3px; 
        }}
        
        /* 5. Estilo para la fecha */
        .pozo-fecha {{
            opacity: 0.7; 
            white-space: nowrap; 
        }}
    </style>
    """
    
    # Añade el CSS al mapa como un objeto HTML de Folium
    m.get_root().html.add_child(folium.Element(css_style))
    # --- FIN: CÓDIGO PARA MARCO Y PUNTA DEL POPUP DEL MISMO COLOR
    
    # 1. OBTENCIÓN DE DATOS (CONCURRENTE)
    datos_pozos_agregados = obtener_datos_totales_pozos(mapa_pozos_dict)
    datos_macrometros = obtener_datos_totales_macrometros(MACROMEDIDORES_TAGS)
    datos_tanques_agregados = obtener_datos_totales_tanques(mapa_tanques_dict) # <--- NUEVA OBTENCIÓN DE DATOS
    datos_rebombeos_agregados = obtener_datos_totales_rebombeos(mapa_rebombeos_dict)
    sector_names = fetch_sectors_geojson(config_posgres)
    Estado_Aguascalientes_names = fetch_Estado_Aguascalientes_geojson(config_posgres)
    Municipio_Aguascalientes_names = fetch_Municipio_Aguascalientes_geojson(config_posgres)
    # -----------------------------------------------------------
    # Listado de pozos por estado
    # -----------------------------------------------------------
    pozos_por_estado = {
        'ON': [], 
        'OFF': [], 
        'OBSOLETO': [], 
        'SIN_TELEMETRIA': []
    }
    rebombeos_por_estado = {
        'ON': [], 
        'OFF': [], 
        'OBSOLETO': [], 
        'SIN_TELEMETRIA': []
    }
    # -----------------------------------------------------------
    # Vairables para el Resumen Global (NUEVO)
    # -----------------------------------------------------------
    lps_total_on = 0.0
    presiones_validas = []
    sumergencias_validas = []
    niveles_estaticos_validos = []
    # 🚨 CAMBIO 1: Obtener el consumo mensual total
    consumo_mensual_total_global = obtener_consumo_mensual_total_global(datos_macrometros)
    # -----------------------------------------------------------
    try:
        # ==================== DEFINICIÓN DE CONSTANTES DE MAPAS BASE Y ATRIBUCIONES ====================
        CARTO_ATTRIBUTION = '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
        OSM_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'

        # Añadir Tiles de CartoDB DarkMatter (ya es el default, pero lo dejamos para control de capas)
        folium.TileLayer(
            'cartodbdarkmatter', 
            name='CartoDB DarkMatter (Default)', 
            attr=CARTO_ATTRIBUTION,
            overlay=True, 
            control=True
        ).add_to(m)
        
        # Añadir Tiles de OSM para alternar
        folium.TileLayer(
            'openstreetmap', 
            name='OpenStreetMap (Claro)', 
            attr=OSM_ATTRIBUTION,
            overlay=False, # Debe ser False para Base Map
            control=True
        ).add_to(m)

        # Añadir Tiles de Esri Satellite para alternar
        folium.TileLayer(
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            attr='Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
            name='Esri Satellite',
            overlay=False, # Debe ser False para Base Map
            control=True
        ).add_to(m)


        # =================================================== BLOQUE DE ACTIVADORES DE LAS CAPAS DEL MAPA =========================================================================

        # 1. ACTIVADOR DEL POLIGONO DEL MUNICIPIO DE AGUASCALIENTES (CAPA DINÁMICA DEL MAPA) --------------------------------------------------------------------------------------------------------
        if Municipio_Aguascalientes_names:
            geojson_layer = folium.GeoJson(
                {
                    'type': 'FeatureCollection',
                    'features': Municipio_Aguascalientes_names
                },
                name='Municipio de Aguascalientes (Polígonos)', # NOMBRE QUE APARECE EN EL CUADRO DEL MAPA
                # Configuración de Popups y Estilo
                style_function=lambda feature: {
                    'fillColor': "#FFC400",
                    'color': "#D10700",
                    'weight': 1,
                    'fillOpacity': 0.00,
                },
            )
            geojson_layer.add_to(m)

        # 2. ACTIVADOR DEL POLIGONO DEL ESTADO DE AGUASCALIENTES (CAPA DINÁMICA DEL MAPA) --------------------------------------------------------------------------------------------------------
        if Estado_Aguascalientes_names:
            geojson_layer = folium.GeoJson(
                {
                    'type': 'FeatureCollection',
                    'features': Estado_Aguascalientes_names
                },
                name='Estado de Aguascalientes (Polígonos)', # NOMBRE QUE APARECE EN EL CUADRO DEL MAPA
                # Configuración de Popups y Estilo
                style_function=lambda feature: {
                    'fillColor': "#FFC400",
                    'color': "#00D11C",
                    'weight': 1,
                    'fillOpacity': 0.00,
                },
            )
            geojson_layer.add_to(m)

        # 3. ACTIVADOR DE SECTORES HIDRAULICOS (CAPA DINÁMICA DEL MAPA) -----------------------------------------------------------------------------------------------------------------------
        if sector_names:
            geojson_layer = folium.GeoJson(
                {
                    'type': 'FeatureCollection',
                    'features': sector_names
                },
                name='Sectores Hidráulicos (Polígonos)', # NOMBRE QUE APARECE EN EL CUADRO DEL MAPA
                # Configuración de Popups y Estilo
                style_function=lambda feature: {
                    'fillColor': '#00FFFF',
                    'color': '#00CED1',
                    'weight': 1,
                    'fillOpacity': 0.10,
                },
                popup=folium.GeoJsonPopup(fields=['popup_html'], labels=False),
                tooltip=folium.GeoJsonTooltip(fields=['sector', 'Poblacion', 'Vol_Prod'], aliases=['Sector:', 'Población:', 'Vol. Prod.:'])
            )
            geojson_layer.add_to(m)

        # 4. ACTIVADOR DE POZOS (CAPA DINÁMICA DEL MAPA) -----------------------------------------------------------------------------------------------------------------------------------
        pozos_layer = folium.FeatureGroup(name='Pozos', show=True).add_to(m)

        # 5. ACTIVADOR DE MACROMEDIDORES (CAPA DINÁMICA DEL MAPA) --------------------------------------------------------------------------------------------------------------------------
        macrometros_layer = folium.FeatureGroup(name='Macromedidores', show=False).add_to(m)

        # 6. ACTIVADOR DE TANQUES (CAPA DINÁMICA DEL MAPA) --------------------------------------------------------------------------------------------------------------------------------
        tanques_layer = folium.FeatureGroup(name='Tanques', show=False).add_to(m) # <--- NUEVA CAPA DE TANQUES
        
        # 7. CAPA DE REBOMBEOS
        rebombeos_layer = folium.FeatureGroup(name='Rebombeos', show=False).add_to(m)
        # =========================================================== FIN DEL BLOQUE DE LOS ACTIVADORES DE LAS CAPAS DEL MAPA ======================================================================

        # ============================================================ DIBUJO DE LOS PUNTOS EN EL MAPA DE POZOS Y MACROS ===========================================================================

        # DIBUJO DE MARCADORES (Pozos)
        for pozo, info_dict in mapa_pozos_dict.items():
            if "coord" not in info_dict:
                continue
            
            lat, lon = info_dict["coord"]
            datos = datos_pozos_agregados.get(pozo, {})
                                   
            # --- CLASIFICACIÓN SIN TELEMETRÍA ---    
            corriente_txt = str(info_dict.get("corriente", "")).strip().lower()    
            if corriente_txt == "sin telemetria":
                # Lógica de dibujo para SIN_TELEMETRIA
                pozos_por_estado['SIN_TELEMETRIA'].append(pozo)
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=4,
                    color="gray",
                    fill=True,
                    fill_color="gray",
                    fill_opacity=0.9,
                    popup=f"{pozo} - SIN TELEMETRÍA"
                ).add_to(pozos_layer) 
                
                folium.map.Marker(
                    [lat, lon],
                    icon=folium.DivIcon(
                        html=f"""<div style="font-size:10px;color:gray;
                                white-space:nowrap;
                                transform: translate(15px, -2px); 
                                font-weight:500;">{pozo}</div>"""
                    )
                ).add_to(pozos_layer) 
                
                continue
            
            datos = datos_pozos_agregados.get(pozo, {})
            encendido = datos.get("encendido", False)
            caudal = datos.get("caudal", 0.0)
            caudal_fecha = datos.get("caudal_fecha", None)
            presion = datos.get("presion", 0.0)
            presion_fecha = datos.get("presion_fecha", None)
            nivel_estatico = datos.get("nivel_estatico", 0.0)
            nivel_estatico_fecha = datos.get("nivel_estatico_fecha", None)
            nivel_tanque = datos.get("nivel_tanque", 0.0)
            nivel_tanque_fecha = datos.get("nivel_tanque_fecha", None)
            voltajes_resultados = datos.get("voltajes_resultados", [])
            corrientes_resultados = datos.get("corrientes_resultados", [])
            sumergencia = datos.get("sumergencia", 0.0)
            sumergencia_fecha = datos.get("sumergencia_fecha", None)
            datos_historicos = datos.get("datos_historicos", pd.DataFrame())

            # La fecha de referencia es el primer dato que obtuvimos (voltaje L1-L2, si existe)
            fecha_referencia = None
            if voltajes_resultados and isinstance(voltajes_resultados[0], tuple) and len(voltajes_resultados[0]) > 1:
                fecha_referencia = voltajes_resultados[0][1]
            
            es_obsoleto = False
            try:
                if fecha_referencia:
                    if isinstance(fecha_referencia, str):
                        dt_pozo = datetime.strptime(fecha_referencia.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    else:
                        dt_pozo = fecha_referencia
                    if (datetime.now() - dt_pozo) >= timedelta(hours=4):
                        es_obsoleto = True
                else:
                    # Si no hay fecha de referencia válida, se considera obsoleto o sin telemetría.
                    if info_dict.get("corriente") == "Sin telemetria":
                        pozos_por_estado['SIN_TELEMETRIA'].append(pozo)
                        # No dibujar marcador para "Sin Telemetría" si esta lógica se aplica aquí
                        continue 
                    else:
                        es_obsoleto = True # Si tiene tag, pero no hay dato ni fecha
            except Exception:
                es_obsoleto = True # Cualquier error en el parsing de fecha

            # -----------------------------------------------------------
            # RECOLECCIÓN PARA DATOS GLOBALES (MODIFICADO: SOLO SI NO ES OBSOLETO)
            # -----------------------------------------------------------
            if not es_obsoleto:
                if encendido and info_dict.get("caudal") and caudal > 0.1 and caudal <= 150.0:
                    lps_total_on += caudal
                if info_dict.get("presion") and presion > 0.0:
                    presiones_validas.append(presion)
                if info_dict.get("sumergencia") and sumergencia > 0.0:
                    sumergencias_validas.append(sumergencia)
                # <--- AÑADIR ESTE BLOQUE:
                if info_dict.get("nivel_estatico") and nivel_estatico > 0.0:
                    niveles_estaticos_validos.append(nivel_estatico)
            # -----------------------------------------------------------
            # -----------------------------------------------------------
            # Definición de colores y clasificación
            # -----------------------------------------------------------
            if info_dict.get("corriente") == "Sin telemetria":
                 # Se agregó a SIN_TELEMETRIA arriba y se continuó el bucle para no dibujarlo
                 pass
            elif es_obsoleto:
                color = '#FF69B4'
                borde = '#C71585'
                radio = 4
                color_texto = '#FF69B4'
                estado_display = "OBSOLETO/OFF"
                pozos_por_estado['OBSOLETO'].append(pozo)
            elif encendido:
                color = 'lime'
                borde = 'darkgreen'
                radio = 5
                color_texto = 'lime'
                estado_display = 'ON'
                pozos_por_estado['ON'].append(pozo)
            else: # Apagado (OFF)
                color = 'red'
                borde = 'darkred'
                radio = 5
                color_texto = 'red'
                estado_display = 'OFF'
                pozos_por_estado['OFF'].append(pozo)
            # -----------------------------------------------------------

            # --- FUNCIÓN HELPER PARA LIMPIAR EL CÓDIGO DE FECHA ---
            def clean_date_html(date):
                """Llama a format_fecha_simple y elimina los paréntesis para un look más SCADA.
                Ahora devolverá la fecha COMPLETA (D/M/A H:M) y el color de obsolescencia."""
                return format_fecha_simple(date).replace("(", "").replace(")", "").strip()
            # --------------------------------------------------------------------------------------------------------------------------------------------------------------
# ==================================================================== CONSTRUCCIÓN DEL POPUP HTML (Pozos) ===================================================================================
            # --- GENERACIÓN DEL GRÁFICO (AHORA INTERACTIVO) ----------------------------------------------------------------
            grafico_html = generar_grafico_caudal_y_presion(pozo, datos_historicos)
            grafico_pozos_html = generar_grafico_popup_pozos(pozo, datos_historicos)
            
# ==================================================================== CONSTRUCCIÓN DEL POPUP HTML (Pozos) ===================================================================================
            
            # 1. Crear un ID válido para JS (sin espacios ni puntos)
            id_seguro = pozo.replace(' ', '_').replace('.', '_').replace('-', '_')

            # ==================================================================== 
            # CONSTRUCCIÓN DEL POPUP HTML (Pozos) 
            # ==================================================================== 
            popup_html = f"""
            <div style="width: 730px; background-color: #0b1a29; padding: 10px; border-radius: 6px; color: white; font-family: Arial, sans-serif;">
                <h3 style="color: {color_texto}; margin: 0 0 10px 0; border-bottom: 2px solid #333; padding-bottom: 5px;">Pozo: {pozo} - <span style="font-size: 14pt;">{estado_display}</span></h3>
                <div style="display: flex; justify-content: space-between;">
                    
                    <div style="width: 48%;">
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: #00CED1;">Caudal:</span> 
                            <b style="font-size: 14pt;">{caudal:.2f} l/s ---------------- </b> <span style="font-size: 9pt;">{clean_date_html(caudal_fecha)}</span>
                        </div>
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: lime;">Presión:</span> 
                            <b style="font-size: 14pt;">{presion:.2f} Kg/cm² ---------- </b><span style="font-size: 9pt;">{clean_date_html(presion_fecha)}</span>
                        </div>
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: #FFD700;">Nivel Estático:</span> 
                            <b>{nivel_estatico:.2f} mts. ------- </b><span style="font-size: 9pt;">{clean_date_html(nivel_estatico_fecha)}</span>
                        </div>
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: #F08080;">Sumergencia:</span> 
                            <b>{sumergencia:.2f} mts. ---------- </b><span style="font-size: 9pt;">{clean_date_html(sumergencia_fecha)}</span>
                        </div>
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: #FFA500;">Nivel Tanque Adj:</span> 
                            <b>{nivel_tanque:.2f} mts. ------ </b><span style="font-size: 9pt;">{clean_date_html(nivel_tanque_fecha)}</span>
                        </div>
                    </div>
                    
                    <div style="width: 48%; border-left: 1px solid #333; padding-left: 10px;">
                        
                        <div>
                            <div style='margin-top: 0; margin-bottom: 5px; font-weight: bold; color: #00CED1;'>Voltajes (V):</div>
                            <div style='margin-left: 10px; font-size: 9pt;'>
                                L1-L2: <b>{voltajes_resultados[0][0]:.0f} Volts ----------------------------------- </b><span style="font-size: 9pt;">{clean_date_html(voltajes_resultados[0][1])}</span><br>
                                L2-L3: <b>{voltajes_resultados[1][0]:.0f} Volts ----------------------------------- </b><span style="font-size: 9pt;">{clean_date_html(voltajes_resultados[1][1])}</span><br>
                                L1-L3: <b>{voltajes_resultados[2][0]:.0f} Volts ----------------------------------- </b><span style="font-size: 9pt;">{clean_date_html(voltajes_resultados[2][1])}</span><br>
                            </div>
                        </div>

                        <div style='margin-top: 15px;'>
                            <div style='margin-top: 5px; margin-bottom: 5px; font-weight: bold; color: lime;'>Corrientes (A):</div>
                            <div style='margin-left: 10px; font-size: 9pt;'>
                                Total (Avg): <b style='font-size: 11pt;'>{datos.get("corriente_total", 0.0):.2f} A</b><br>
                                L1: <b>{corrientes_resultados[0][0]:.2f} Amp ------------------------------------ </b><span style="font-size: 9pt;">{clean_date_html(corrientes_resultados[0][1])}</span><br>
                                L2: <b>{corrientes_resultados[1][0]:.2f} Amp ------------------------------------ </b><span style="font-size: 9pt;">{clean_date_html(corrientes_resultados[1][1])}</span><br>
                                L3: <b>{corrientes_resultados[2][0]:.2f} Amp ------------------------------------ </b><span style="font-size: 9pt;">{clean_date_html(corrientes_resultados[2][1])}</span><br>
                            </div>
                        </div>
                    </div>
                </div>
                
                
                
                <div id="grafico_mini_{id_seguro}" style="margin-top: 15px; min-height: 200px; background: #050d14; border-radius: 4px;">
        {grafico_html} 
    </div>

    <div id="grafico_data_full_{id_seguro}" style="display: none;">
        {grafico_pozos_html}
    </div>
    
    <div style="text-align: right; margin-top: 15px;">
        <button type="button" onclick="abrirFull_{id_seguro}()" 
                style="background-color: #00CED1; color: #0b1a29; padding: 10px 20px; border-radius: 4px; border: none; cursor: pointer; font-weight: bold; font-size: 10pt;">
            📊 ABRIR GRÁFICO FULL
        </button>
    </div>

    <script type="text/javascript">
    function abrirFull_{id_seguro}() {{
        // EXTRAEMOS EL CONTENIDO DEL DIV OCULTO (grafico_pozos_html)
        var htmlGraficoDetallado = document.getElementById('grafico_data_full_{id_seguro}').innerHTML;
        
        var win = window.open('', '_blank');
        if (win) {{
            win.document.write('<html><head><title>Detalle Histórico - {pozo}</title>');
            win.document.write('<style>body {{ background-color: #0b1a29; margin: 0; padding: 20px; font-family: Arial; color: white; }} .container {{ width: 100%; height: 92vh; }}</style>');
            win.document.write('</head><body>');
            win.document.write('<h2 style="text-align: center;">Análisis Detallado: {pozo}</h2>');
            win.document.write('<div class="container">' + htmlGraficoDetallado + '</div>');
            win.document.write('</body></html>');
            win.document.close();
        }} else {{
            alert('El navegador bloqueó la ventana emergente.');
        }}
    }}
    </script>
</div>
"""
            # --- DIBUJO DEL MARCADOR (Pozos) ---
            if info_dict.get("corriente") != "Sin telemetria":
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=radio,
                    color=borde,
                    weight=1,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.8,
                    # QUI SE AJUSTA EL TAMAÑO Y ANCHO DEL POPUP DE LOS POZOS AJUSTADOS PARA EL GRÁFICO INTERACTIVO: AHORA 750px ancho, 550px alto
                    popup=folium.Popup(folium.IFrame(html=popup_html, width=950, height=600), max_width=950),
                    tooltip=pozo
                ).add_to(pozos_layer)

                # Etiqueta de texto (Nombre del Pozo)
                folium.Marker(
                    location=[lat, lon],
                    icon=folium.DivIcon(
                        icon_size=(150,36),
                        icon_anchor=(-10, 8),
                        html=f'<div style="font-size: 8pt; color: {color_texto}; white-space: nowrap;">{pozo}</div>',
                    ),
                    tooltip=pozo
                ).add_to(pozos_layer)


# ==================================================================== DIBUJO DE LOS PUNTOS DE LOS MACROMEDIDORES ======================================================================
        for nombre_mm, info_dict in mapa_macrometros_dict.items():
            if "coord" not in info_dict:
                continue
            lat, lon = info_dict["coord"]

            # 1. Obtener datos del macrómetro
            datos = datos_macrometros.get(nombre_mm, {})
            caudal = datos.get("caudal", 0.0)
            caudal_fecha = datos.get("caudal_fecha", None)
            consumo_mensual_total = datos.get("consumo_mensual_total", 0.0)
            avg_consumo_diario = datos.get("consumo_mensual_avg", 0.0)
            num_lecturas_mes = datos.get("lecturas_count", 0)
            datos_historicos_consumo = datos.get("datos_historicos_consumo", pd.DataFrame()) # <-- NUEVA VARIABLE

            # 2. Definir estilo y color
            color = '#1E90FF'
            borde = '#0000CD'
            radio = 5
            
            # 3. Generar Gráfico de Consumo (NUEVA LÍNEA)
            grafico_html = generar_grafico_consumo_macrometer(nombre_mm, datos_historicos_consumo)

# ==================================================================== 4. Construir Popup (Macromedidores) ===================================================================================
            # Se usa la función auxiliar localmente definida en el bloque principal, que ahora incluye la fecha completa
            try:
                def clean_date_html(date):
                    # Ahora devolverá la fecha COMPLETA (D/M/A H:M)
                    return format_fecha_simple(date, es_macromedidor=True).replace("(", "").replace(")", "").strip()
            except NameError:
                def clean_date_html(date):
                    return ""
                
            fecha_formato = clean_date_html(caudal_fecha)
            
            # 🚨 HTML DEL POPUP ACTUALIZADO (AÑADIDO EL GRÁFICO)
            popup_html = f"""
            <div style="width: 730px; background-color: #0b1a29; padding: 10px; border-radius: 6px; color: white; font-family: Arial, sans-serif;">
                <h3 style="color: #00FFFF; margin: 0 0 10px 0; border-bottom: 2px solid #333; padding-bottom: 5px;"> 
                    Macromedidor: {nombre_mm} 
                </h3>
                
                <div style="display: flex; justify-content: space-between; margin-bottom: 10px; padding-bottom: 10px; border-bottom: 1px solid #333;">
                    <div style="width: 48%;">
                        <div style="font-size: 10pt; padding: 2px 0;"> 
                            <span style="font-weight: bold; color: #00FFFF;">Ultima lectura:</span> <b style="font-size: 14pt;">{caudal:.2f} m3 </b> 
                            <span style="font-size: 8pt; color: white ">{fecha_formato}</span>
                        </div>
                    </div>

                    <div style="width: 48%; border-left: 1px solid #333; padding-left: 10px;">
                        <div style="font-size: 10pt; padding: 2px 0;"> 
                            <span style="font-weight: bold; color: #90EE90;">Consumo Acumulado (Mes):</span> <b style="font-size: 12pt;">{consumo_mensual_total:.2f} m³</b>
                        </div>
                        <div style="font-size: 10pt; padding: 2px 0;"> 
                            <span style="font-weight: bold; color: #FFD700;">Consumo Diario Promedio:</span> <b style="font-size: 12pt;">{avg_consumo_diario:.2f} m³</b>
                        </div>
                        <div style="font-size: 8pt; padding: 2px 0; color: #ccc;"> 
                            Total Lecturas en Mes: {num_lecturas_mes}
                        </div>
                    </div>
                </div>
                
                <div style="width: 100%; border-top: 2px solid #333; padding-top: 10px;">
                    {grafico_html}
                </div>
            </div>
            """

            # 5. Crear el marcador de Macrómetro (TAMAÑO AJUSTADO)
            folium.CircleMarker(
                location=[lat, lon],
                radius=radio,
                color=borde,
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.8,
                # 🚨 TAMAÑO AJUSTADO PARA EL GRÁFICO INTERACTIVO: AHORA 750px ancho, 550px alto
                popup=folium.Popup(folium.IFrame(html=popup_html, width=750, height=550), max_width=750),
                tooltip=nombre_mm
            ).add_to(macrometros_layer)

            # 6. Agregar etiqueta de texto (Nombre del Macrómetro)
            folium.Marker(
                location=[lat, lon],
                icon=folium.DivIcon(
                    icon_size=(150,36),
                    icon_anchor=(-10, 8),
                    html=f'<div style="font-size: 9pt; font-weight: bold; color: {borde}; background-color: rgba(255, 255, 255, 0.8); padding: 2px 4px; border-radius: 3px; border: 1px solid {borde}; white-space: nowrap;">Macromedidor: {nombre_mm}</div>',
                ),
                tooltip=nombre_mm
            ).add_to(macrometros_layer)

# ===================================================== <--- DIBUJO DE MARCADORES (TANQUES) (ACTUALIZADO CON GRÁFICO GRANDE Y FECHA COMPLETA) ===============================================
        for nombre_tanque, info_dict in mapa_tanques_dict.items():
            if "coord" not in info_dict:
                continue
            lat, lon = info_dict["coord"]

            # 1. Obtener datos del tanque
            datos = datos_tanques_agregados.get(nombre_tanque, {})
            nivel_actual = datos.get("nivel_actual", 0.0)
            nivel_fecha = datos.get("nivel_fecha", None)
            nivel_max = info_dict.get("nivel_max", 1.0) 
            datos_historicos = datos.get("datos_historicos", pd.DataFrame()) 
            
            # Calcular porcentaje de llenado y color de la barra
            porcentaje = (nivel_actual / nivel_max) * 100 if nivel_max > 0 else 0.0
            
            # Determinar color de barra basado en nivel y obsolescencia
            if nivel_fecha is None or (nivel_fecha is not None and (datetime.now() - nivel_fecha) >= UMBRAL_ANTIGUEDAD):
                color_progreso = "#808080" # Gris para obsoleto/sin dato
            elif porcentaje >= 80:
                color_progreso = "#008000" # Verde para lleno
            elif porcentaje >= 40:
                color_progreso = "#FFD700" # Amarillo para medio
            else:
                color_progreso = "#B22222" # Rojo para bajo
                
            
            # 2. Definir estilo y color (Naranja)
            color = '#FFA500'  # NARANJA (Orange)
            borde = '#FF8C00'  # NARANJA OSCURO (DarkOrange)
            radio = 5
            
            # 3. Generar Gráfico de Tendencia
            grafico_html = generar_grafico_nivel_tanque(nombre_tanque, nivel_max, datos_historicos)
            
#================================================================== 4. Construir Popup (Tanques) ==================================================================================
            # 🚨 Usa la función específica para tanques, que mantiene el estilo más grande y bold.
            fecha_formato_tanque = format_fecha_completa_tanque(nivel_fecha)
            
            popup_html = f"""
            <div style="width: 730px; background-color: #0b1a29; padding: 10px; border-radius: 6px; color: white; font-family: Arial, sans-serif;">
                <h3 style="color: {color}; margin: 0 0 10px 0; border-bottom: 2px solid #333; padding-bottom: 5px;"> 
                    Tanque: {nombre_tanque} 
                </h3>
                
                <div style="margin-bottom: 20px;">
                    <div style="font-size: 12pt; padding: 5px 0; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center;"> 
                        <div>
                            <span style="font-weight: bold; color: {color};">Nivel Actual:</span> 
                            <b style="color: white; font-size: 16pt;">{nivel_actual:.2f} mts.</b>
                        </div>
                        {fecha_formato_tanque}
                    </div>
                    <div style="font-size: 12pt; padding: 5px 0; border-bottom: 1px solid #333;"> 
                        <span style="font-weight: bold; color: #90EE90;">Nivel Máximo:</span> 
                        <b style="color: #90EE90; font-size: 14pt;">{nivel_max:.2f} mts.</b>
                    </div>
                    
                    <div style="margin-top: 15px; padding-top: 5px; border-top: 1px solid #333;">
                        <div style="font-weight: bold; margin-bottom: 5px; font-size: 11pt;">Nivel de Llenado ({porcentaje:.1f}%):</div>
                        <div style="height: 25px; background-color: #333; border-radius: 4px; overflow: hidden; position: relative; border: 1px solid {color_progreso};">
                            <div style="width: {min(100, max(0, porcentaje))}%; height: 100%; background-color: {color_progreso}; transition: width 0.5s;"></div>
                        </div>
                    </div>
                </div>
                
                <div style="width: 100%; border-top: 2px solid #333; padding-top: 10px;">
                    {grafico_html}
                </div>
            </div>
            """

            # 5. Crear el marcador de Tanque
            folium.CircleMarker(
                location=[lat, lon],
                radius=radio,
                color=borde,
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.8,
                # Ajustar el tamaño del popup para el gráfico
                popup=folium.Popup(folium.IFrame(html=popup_html, width=750, height=550), max_width=750), 
                tooltip=nombre_tanque
            ).add_to(tanques_layer)

            # 6. Agregar etiqueta de texto
            folium.Marker(
                location=[lat, lon],
                icon=folium.DivIcon(
                    icon_size=(150,36),
                    icon_anchor=(-10, 8),
                    html=f'<div style="font-size: 8pt; color: {color}; white-space: nowrap;">{nombre_tanque}</div>',
                ),
                tooltip=nombre_tanque
            ).add_to(tanques_layer)
        # FIN DEL NUEVO BLOQUE DE TANQUES
        
# ============================================================================== DIBUJO DE REBOMBEOS ========================================================================================
        # DIBUJO DE MARCADORES (Rebombeos)
        for rebombeo, info_dict in mapa_rebombeos_dict.items():
            if "coord" not in info_dict:
                continue
            
            lat, lon = info_dict["coord"]
            datos = datos_rebombeos_agregados.get(rebombeo, {})
                                   
            # --- CLASIFICACIÓN SIN TELEMETRÍA ---    
            corriente_txt = str(info_dict.get("corriente", "")).strip().lower()    
            if corriente_txt == "sin telemetria":
                # Lógica de dibujo para SIN_TELEMETRIA
                rebombeos_por_estado['SIN_TELEMETRIA'].append(rebombeo)
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=4,
                    color="gray",
                    fill=True,
                    fill_color="gray",
                    fill_opacity=0.9,
                    popup=f"{rebombeo} - SIN TELEMETRÍA"
                ).add_to(rebombeos_layer) 
                
                folium.map.Marker(
                    [lat, lon],
                    icon=folium.DivIcon(
                        html=f"""<div style="font-size:10px;color:gray;
                                white-space:nowrap;
                                transform: translate(15px, -2px); 
                                font-weight:500;">{rebombeo}</div>"""
                    )
                ).add_to(rebombeos_layer) 
                
                continue
            
            datos = datos_rebombeos_agregados.get(rebombeo, {})
            encendido = datos.get("encendido", False)
            caudal = datos.get("caudal", 0.0)
            caudal_fecha = datos.get("caudal_fecha", None)
            presion = datos.get("presion", 0.0)
            
            presion_fecha = datos.get("presion_fecha", None)
            nivel_estatico = datos.get("nivel_estatico", 0.0)
            nivel_estatico_fecha = datos.get("nivel_estatico_fecha", None)
            nivel_tanque = datos.get("nivel_tanque", 0.0)
            nivel_tanque_fecha = datos.get("nivel_tanque_fecha", None)
            voltajes_resultados = datos.get("voltajes_resultados", [])
            corrientes_resultados = datos.get("corrientes_resultados", [])
            sumergencia = datos.get("sumergencia", 0.0)
            sumergencia_fecha = datos.get("sumergencia_fecha", None)
            datos_historicos = datos.get("datos_historicos", pd.DataFrame())

            # La fecha de referencia es el primer dato que obtuvimos (presion, si existe)
            fecha_referencia = None
            if voltajes_resultados and isinstance(voltajes_resultados[0], tuple) and len(voltajes_resultados[0]) > 1:
                fecha_referencia = voltajes_resultados[0][1]
            
            es_obsoleto = False
            try:
                if fecha_referencia:
                    if isinstance(fecha_referencia, str):
                        dt_rebombeo = datetime.strptime(fecha_referencia.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    else:
                        dt_rebombeo = fecha_referencia
                    if (datetime.now() - dt_rebombeo) >= timedelta(hours=4):
                        es_obsoleto = True
                else:
                    # Si no hay fecha de referencia válida, se considera obsoleto o sin telemetría.
                    if info_dict.get("corriente") == "Sin telemetria":
                        rebombeos_por_estado['SIN_TELEMETRIA'].append(rebombeo)
                        # No dibujar marcador para "Sin Telemetría" si esta lógica se aplica aquí
                        continue 
                    else:
                        es_obsoleto = True # Si tiene tag, pero no hay dato ni fecha
            except Exception:
                es_obsoleto = True # Cualquier error en el parsing de fecha

            # -----------------------------------------------------------
            # RECOLECCIÓN PARA DATOS GLOBALES (MODIFICADO: SOLO SI NO ES OBSOLETO)
            # -----------------------------------------------------------
            if not es_obsoleto:
                if encendido and info_dict.get("caudal") and caudal > 0.1 and caudal <= 150.0:
                    lps_total_on += caudal
                if info_dict.get("presion") and presion > 0.0:
                    presiones_validas.append(presion)
                if info_dict.get("sumergencia") and sumergencia > 0.0:
                    sumergencias_validas.append(sumergencia)
                # <--- AÑADIR ESTE BLOQUE:
                if info_dict.get("nivel_estatico") and nivel_estatico > 0.0:
                    niveles_estaticos_validos.append(nivel_estatico)
            # -----------------------------------------------------------
            # -----------------------------------------------------------
            # Definición de colores y clasificación
            # -----------------------------------------------------------
            if info_dict.get("corriente") == "Sin telemetria":
                 # Se agregó a SIN_TELEMETRIA arriba y se continuó el bucle para no dibujarlo
                 pass
            elif es_obsoleto:
                color = '#FF69B4'
                borde = '#C71585'
                radio = 4
                color_texto = '#FF69B4'
                estado_display = "OBSOLETO"
                rebombeos_por_estado['OBSOLETO'].append(rebombeo)
            # --- NUEVA LÓGICA DE PRESIÓN ---
            elif presion > 0.10:
                color = 'lime'      # Verde si hay presión
                borde = 'darkgreen'
                radio = 5
                color_texto = 'lime'
                estado_display = 'ON'
                rebombeos_por_estado['ON'].append(rebombeo)
            else: 
                # Presión menor o igual a 0.10 (Apagado o Fallo)
                color = 'red'       # Rojo si no hay presión
                borde = 'darkred'
                radio = 5
                color_texto = 'red'
                estado_display = 'OFF'
                rebombeos_por_estado['OFF'].append(rebombeo)
            # -----------------------------------------------------------

            # --- FUNCIÓN HELPER PARA LIMPIAR EL CÓDIGO DE FECHA ---
            def clean_date_html(date):
                """Llama a format_fecha_simple y elimina los paréntesis para un look más SCADA.
                Ahora devolverá la fecha COMPLETA (D/M/A H:M) y el color de obsolescencia."""
                return format_fecha_simple(date).replace("(", "").replace(")", "").strip()
            # --------------------------------------------------------------------------------------------------------------------------------------------------------------

            # --- GENERACIÓN DEL GRÁFICO (AHORA INTERACTIVO) ----------------------------------------------------------------
            grafico_html = generar_grafico_rebombeo(rebombeo, datos_historicos)
            
# ==================================================================== CONSTRUCCIÓN DEL POPUP HTML (Rebombeos) ===================================================================================
            popup_html = f"""
            <div style="width: 730px; background-color: #0b1a29; padding: 10px; border-radius: 6px; color: white; font-family: Arial, sans-serif;">
                <h3 style="color: {color_texto}; margin: 0 0 10px 0; border-bottom: 2px solid #333; padding-bottom: 5px;">Pozo: {rebombeo} - <span style="font-size: 14pt;">{estado_display}</span></h3>
                <div style="display: flex; justify-content: space-between;">
                    
                    <div style="width: 48%;">
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: #00CED1;">Caudal:</span> 
                            <b style="font-size: 14pt;">{caudal:.2f} l/s ---------------- </b> <span style="font-size: 9pt;">{clean_date_html(caudal_fecha)}</span>
                        </div>
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: lime;">Presión:</span> 
                            <b style="font-size: 14pt;">{presion:.2f} Kg/cm² ---------- </b><span style="font-size: 9pt;">{clean_date_html(presion_fecha)}</span>
                        </div>
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: #FFD700;">Nivel Estático:</span> 
                            <b>{nivel_estatico:.2f} mts. ------- </b><span style="font-size: 9pt;">{clean_date_html(nivel_estatico_fecha)}</span>
                        </div>
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: #F08080;">Sumergencia:</span> 
                            <b>{sumergencia:.2f} mts. ---------- </b><span style="font-size: 9pt;">{clean_date_html(sumergencia_fecha)}</span>
                        </div>
                        <div style="margin-bottom: 5px; padding-bottom: 5px; border-bottom: 1px solid #333;">
                            <span style="font-weight: bold; color: #FFA500;">Nivel Tanque Adj:</span> 
                            <b>{nivel_tanque:.2f} mts. ------ </b><span style="font-size: 9pt;">{clean_date_html(nivel_tanque_fecha)}</span>
                        </div>
                    </div>
                    
                    <div style="width: 48%; border-left: 1px solid #333; padding-left: 10px;">
                        
                        <div>
                            <div style='margin-top: 0; margin-bottom: 5px; font-weight: bold; color: #00CED1;'>Voltajes (V):</div>
                            <div style='margin-left: 10px; font-size: 9pt;'>
                                L1-L2: <b>{voltajes_resultados[0][0]:.0f} Volts ----------------------------------- </b><span style="font-size: 9pt;">{clean_date_html(voltajes_resultados[0][1])}</span><br>
                                L2-L3: <b>{voltajes_resultados[1][0]:.0f} Volts ----------------------------------- </b><span style="font-size: 9pt;">{clean_date_html(voltajes_resultados[1][1])}</span><br>
                                L1-L3: <b>{voltajes_resultados[2][0]:.0f} Volts ----------------------------------- </b><span style="font-size: 9pt;">{clean_date_html(voltajes_resultados[2][1])}</span><br>
                            </div>
                        </div>

                        <div style='margin-top: 15px;'>
                            <div style='margin-top: 5px; margin-bottom: 5px; font-weight: bold; color: lime;'>Corrientes (A):</div>
                            <div style='margin-left: 10px; font-size: 9pt;'>
                                Total (Avg): <b style='font-size: 11pt;'>{datos.get("corriente_total", 0.0):.2f} A</b><br>
                                L1: <b>{corrientes_resultados[0][0]:.2f} Amp ------------------------------------ </b><span style="font-size: 9pt;">{clean_date_html(corrientes_resultados[0][1])}</span><br>
                                L2: <b>{corrientes_resultados[1][0]:.2f} Amp ------------------------------------ </b><span style="font-size: 9pt;">{clean_date_html(corrientes_resultados[1][1])}</span><br>
                                L3: <b>{corrientes_resultados[2][0]:.2f} Amp ------------------------------------ </b><span style="font-size: 9pt;">{clean_date_html(corrientes_resultados[2][1])}</span><br>
                            </div>
                        </div>
                    </div>
                </div>
                
                {grafico_html}
            </div>
            """
            
            # --- DIBUJO DEL MARCADOR (Rebombeo) ---
            if info_dict.get("corriente") != "Sin telemetria":
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=radio,
                    color=borde,
                    weight=1,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.8,
                    # TAMAÑO Y ANCHO DEL POPUP DE LOS POZOS AJUSTADOS PARA EL GRÁFICO INTERACTIVO: AHORA 750px ancho, 550px alto
                    popup=folium.Popup(folium.IFrame(html=popup_html, width=950, height=550), max_width=950),
                    tooltip=rebombeo
                ).add_to(rebombeos_layer)

                # Etiqueta de texto (Nombre del Pozo)
                folium.Marker(
                    location=[lat, lon],
                    icon=folium.DivIcon(
                        icon_size=(150,36),
                        icon_anchor=(-10, 8),
                        html=f'<div style="font-size: 8pt; color: {color_texto}; white-space: nowrap;">{rebombeo}</div>',
                    ),
                    tooltip=rebombeo
                ).add_to(rebombeos_layer)
        # ========================================================== CONTROL DE CAPAS Y MARCADORES =========================================================================================

        folium.LayerControl(collapsed=False).add_to(m)

        # ======================================================= EFECTO PARPADEO, RECARGA Y CONTROL DE CAPAS (JS) =============================================================================
        js_sector_names = json.dumps(sector_names)
        js = f'''
        <style>
        /* Clase CSS para el parpadeo en la lista flotante (Pozos OFF) */
        .warning-blink-icon {{
            animation: blinker 1.4s linear infinite;
        }}
        /* Se deja el estilo completo de la imagen de advertencia */
        .warning-icon-html {{
            display: inline-block;
            height: 12px;
            width: 12px;
            background-color: #FFD700;
            border: 1px solid #FFA500;
            transform: translateY(2px);
            clip-path: polygon(50% 0%, 100% 100%, 0% 100%);
            position: relative;
        }}
        .warning-icon-html::after {{
            content: '!';
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-weight: bold;
            color: black;
            font-size: 8pt;
            line-height: 1;
        }}
        @keyframes blinker {{
            50% {{
                opacity: 0.0;
            }}
        }}
        </style>
        <script>
        const sectorNames = {js_sector_names};
        const intervalId = setInterval(() => {{
            // --- 1. LÓGICA DE PARPADEO (Círculos en el mapa y en la lista de OFF) ---
            const apagados_mapa = document.querySelectorAll('path[stroke="darkred"]');
            const obsoletos_mapa = document.querySelectorAll('path[stroke="#C71585"]');
            
            const parpadear_mapa = (elementos) => {{
                elementos.forEach(el => {{
                    el.style.visibility = (el.style.visibility === 'hidden') ? 'visible' : 'hidden';
                }});
            }};
            
            parpadear_mapa(apagados_mapa);
            parpadear_mapa(obsoletos_mapa);

            // --- 2. LÓGICA DEL BOTÓN DE CONTROL DE SECTORES (Activar/Desactivar SOLO SECTORES) ---
            const overlaysContainer = document.querySelector('.leaflet-control-layers-overlays');

            // Solo ejecutar si el contenedor de capas existe y el control maestro aún no se ha creado
            if (overlaysContainer && !document.getElementById('toggle-all-sectors')) {{
                const firstSectorInput = overlaysContainer.querySelector('label[title^="Sector:"] input');
                let insertBeforeElement = null;

                if (firstSectorInput) {{
                    insertBeforeElement = firstSectorInput.closest('label').parentNode;
                }}

                // Crear el control "Activar/Desactivar TODOS los Sectores"
                if (insertBeforeElement) {{
                    const toggleAllDiv = document.createElement('div');
                    toggleAllDiv.style.padding = '5px';
                    toggleAllDiv.style.borderTop = '1px solid #ccc';
                    toggleAllDiv.style.cursor = 'pointer';
                    toggleAllDiv.id = 'toggle-all-sectors';
                    toggleAllDiv.innerHTML = '<span style="font-weight: bold; color: #00FFFF;">▶️ Activar TODOS los Sectores</span>';
                    toggleAllDiv.dataset.state = 'off';

                    toggleAllDiv.addEventListener('click', function() {{
                        const isTurningOn = this.dataset.state === 'off';
                        this.dataset.state = isTurningOn ? 'on' : 'off';
                        this.innerHTML = isTurningOn 
                            ? '<span style="font-weight: bold; color: #00FFFF;">⏸️ Desactivar TODOS los Sectores</span>'
                            : '<span style="font-weight: bold; color: #00FFFF;">▶️ Activar TODOS los Sectores</span>';
                        
                        // Busca todos los inputs de sectores y los activa/desactiva
                        overlaysContainer.querySelectorAll('label[title^="Sector:"] input').forEach(input => {{
                            if (input.checked !== isTurningOn) {{
                                input.click();
                            }}
                        }});
                    }});

                    // Insertar el nuevo control justo antes del primer sector
                    insertBeforeElement.parentNode.insertBefore(toggleAllDiv, insertBeforeElement);
                }}
            }}
        }}, 700);
        </script>
        '''
        m.get_root().html.add_child(folium.Element(js))


        # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
        # Función para generar el contenido HTML de cada grupo de pozos
        def generar_grupo_html(titulo, pozos, icono_html, color_borde, color_fondo_titulo, color_fondo_lista='#111'):
            lista_pozos_html = ""
            if not pozos:
                lista_pozos_html = f"""
                <div style="padding: 5px; text-align: center; color: #aaa; background-color: {color_fondo_lista}; font-style: italic;">
                    Ningún pozo en este estado.
                </div>
                """
            else:
                for pozo in sorted(pozos):
                    blink_class = ''
                    if 'Bombas OFF' in titulo or 'Obsoletos' in titulo:
                        blink_class = 'warning-blink-icon'
                    lista_pozos_html += f"""
                    <div style="padding: 2px 5px; border-bottom: 1px solid #222; display: flex; justify-content: space-between; align-items: center; background-color: {color_fondo_lista};">
                        <span style="color: white; font-size: 10pt; font-weight: bold;">{pozo}</span>
                        <span class="{blink_class}" style="font-size: 12pt; margin-left: 10px; display: inline-block;">{icono_html}</span>
                    </div>
                    """
            
            grupo_html = f"""
            <div style="margin-bottom: 15px; border: 1px solid {color_borde}; border-radius: 4px; overflow: hidden;">
                <h4 style="margin: 0; padding: 5px; background-color: {color_fondo_titulo}; color: white; text-align: center; font-size: 11pt;">
                    {titulo} ({len(pozos)})
                </h4>
                <div style="max-height: 150px; overflow-y: auto; background-color: {color_fondo_lista};">
                    {lista_pozos_html}
                </div>
            </div>
            """
            return grupo_html

# =============================================================== BLOQUE DEL RESUMEN GLOBAL ===============================================================================================
        # 🚨 CAMBIO 3: Modificación del HTML para la nueva métrica y reubicación
        resumen_global_html = f"""
        <div style="margin-bottom: 15px; border: 1px solid #1E90FF; border-radius: 4px; padding: 10px; background-color: #0b1a29; color: white;">
            <h4 style="margin: 0 0 10px 0; color: #1E90FF; text-align: center; font-size: 12pt;">RESUMEN GLOBAL</h4>
            <div style="font-size: 10pt;">
                <p style="margin: 5px 0;">
                    <span style="color: lime; font-weight: bold;">Caudal Total:</span> <b>{lps_total_on:.2f}</b> l/s
                </p>
                <p style="margin: 5px 0;">
                    <span style="color: lime; font-weight: bold;">Presión Prom:</span> <b>{(sum(presiones_validas) / len(presiones_validas) if presiones_validas else 0.0):.2f}</b> Kg/cm²
                </p>
                    <p style="margin: 5px 0;">
                    <span style="color: #FFD700; font-weight: bold;">Nivel Estático Prom:</span> <b>{(sum(niveles_estaticos_validos) / len(niveles_estaticos_validos) if niveles_estaticos_validos else 0.0):.2f}</b> mts.
                </p>
                <p style="margin: 5px 0; border-top: 1px solid #333; padding-top: 5px;">
                    <span style="color: #00FFFF; font-weight: bold;">Consumo Macros Total (Mes):</span> <b>{consumo_mensual_total_global:,.2f}</b> m³
                </p>
            </div>
        </div>
        """

        # Definición de Íconos para la Lista
        CIRCULO_VERDE = '<span style="color: lime;">&#x25CF;</span>'
        CIRCULO_ROJO = '<span style="color: red;">&#x25CF;</span>'
        CIRCULO_ROSA = '<span style="color: #FF69B4;">&#x25CF;</span>'
        CIRCULO_GRIS = '<span style="color: #808080;">&#x25CF;</span>'
        ADVERTENCIA_AMARILLA_HTML = '<div class="warning-icon-html"></div>'


        # Generación de las listas HTML
        lista_on_html = generar_grupo_html("Bombas ON", pozos_por_estado['ON'], CIRCULO_VERDE, '#008000', '#008000')
        lista_off_html = generar_grupo_html("Bombas OFF", pozos_por_estado['OFF'], ADVERTENCIA_AMARILLA_HTML, '#B22222', '#B22222')
        lista_obsoleto_html = generar_grupo_html("Obsoletos", pozos_por_estado['OBSOLETO'], CIRCULO_ROSA, '#C71585', '#C71585')
        lista_sintm_html = generar_grupo_html("Sin telemetria", pozos_por_estado['SIN_TELEMETRIA'], CIRCULO_GRIS, '#808080', '#808080')

        # Contenedor HTML/CSS para el listado flotante
        listado_html = f"""
        <div style="position: fixed; 
                    top: 10px; 
                    left: 10px; 
                    width: 250px; 
                    background-color: rgba(11, 26, 41, 0.95); 
                    padding: 10px; 
                    border-radius: 8px; 
                    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.5); 
                    z-index: 1000; 
                    max-height: 95vh; 
                    overflow-y: auto;">
            <h3 style="margin: 0 0 15px 0; color: white; text-align: center; font-size: 14pt;">Estado de Pozos</h3>
            
            {resumen_global_html}
            {lista_on_html}
            {lista_off_html}
            {lista_obsoleto_html}
            {lista_sintm_html}
        </div>
        """
        
        # Añadir el listado flotante al mapa
        m.get_root().html.add_child(folium.Element(listado_html))
        
        # -----------------------------------------------------------

        TIEMPO_RECARGA_SEGUNDOS = 351
        meta_refresh_html = f'<meta http-equiv="refresh" content="{TIEMPO_RECARGA_SEGUNDOS}">'
        m.get_root().header.add_child(folium.Element(meta_refresh_html))

        MAP_FILE_PATH = os.path.abspath('mapa_aguascalientes_live.html')
        
        m.save(MAP_FILE_PATH)

        return MAP_FILE_PATH

    except Exception as e:
        print(f"Error al generar el mapa: {e}.")
        return None

# ==============================================================================
# INTERFAZ WEB (REEMPLAZO DE TKINTER)
# ==============================================================================
st.title("🛰️ SISTEMA DE MONITOREO MIAA")
st.write("")

if st.button("Iniciar Monitoreo de Mapa"):
    with st.spinner("Procesando datos y generando mapa..."):
        try:
            # 1. Ejecuta tu lógica
            ruta_mapa = generar_mapa_miaa()
            
            # 2. Leer el HTML generado para enviarlo al navegador
            with open(ruta_mapa, "rb") as f:
                html_bytes = f.read()
                b64 = base64.b64encode(html_bytes).decode()
            
            # 3. Crear el link que abre el mapa en otra pestaña (target="_blank")
            # Esto soluciona que el navegador no bloquee la apertura
            st.success("✅ Mapa generado con éxito.")
            
            # Botón visual para abrir la pestaña
            href = f'<a href="data:text/html;base64,{b64}" target="_blank" style="text-decoration: none;"><div style="text-align: center; padding: 15px; background-color: #00CED1; color: #0b1a29; font-weight: bold; border-radius: 5px;">HAGA CLIC AQUÍ PARA VER EL MAPA</div></a>'
            st.markdown(href, unsafe_allow_html=True)
            
        except Exception as e:
            st.error(f"Error al generar el mapa: {e}")

st.write("---")
st.caption("MIAA - Ambiente de Control Web")
