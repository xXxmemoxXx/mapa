import folium
import webbrowser
from tkinter import messagebox
import tkinter as tk
from tkinter import ttk
import os
import time
import mysql.connector
import datetime as dt
from datetime import datetime, timedelta
import threading
import concurrent.futures 
import json 
import plotly.graph_objects as go
import plotly.io as pio 
import pandas as pd
import psycopg2 
from typing import List, Dict, Tuple, Any
from tkinter import scrolledtext
import sys


MAPA_EN_EJECUCION = False
LAT_CENTRO = 21.8818
LON_CENTRO = -102.2917

#========================================================================== CONFIGURACIÓN GENERAL BASES DE DATOS =============================================================================
# Configuración de base de datos de Pozos (original)
config = {
    'user': 'miaamx_dashboard',
    'password': 'h97_p,NQPo=l',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria'
}
# <--- CONFIGURACIÓN MACROMEDIDORES (SOLICITADA)
config_macromedidores = {
    'user': 'miaamx_telemetria2',
    'password': 'bWkrw1Uum1O&',
    'host': 'miaa.mx',
    'database': 'miaamx_telemetria2' 
}
config_posgres = {
    'user': 'map_tecnica', # Usuario del archivo original
    'password': 'M144.Tec', # Contraseña del archivo original
    'host': 'ti.miaa.mx',
    'database': 'qgis' # Base de datos donde están los sectores
}

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
#========================================================================== FUNCIÓN DE EXTRACCIÓN POLIGONOS DE SECTORES (NUEVA) =========================================================================
def fetch_sectors_geojson(config_posgres: Dict[str, str]) -> List[Dict[str, Any]]:
    conn = conectar_postgres(config_posgres)
    if conn is None:
        return []

    geojson_features = []
    
    try:
        # CONSULTA CORREGIDA FINAL: Incluye ST_Transform a 4326 y capitalización correcta
        final_query = """
            SELECT 
                sector, 
                "Pozos_Sector", 
                "Poblacion", 
                "Vol_Prod", 
                "Superficie",
                "Long_Red",
                "U_Domesticos",
                ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data 
            FROM 
                "COMPLEMENTARIOS"."Sectores_hidr";
        """
        
        print("⏳ Obteniendo datos geográficos de sectores de PostgreSQL...")
        df = pd.read_sql(final_query, conn)
        
        for _, row in df.iterrows():
            try:
                geojson_geometry = json.loads(row['geojson_data'])
            except:
                continue 

            # Crear el texto de popup con los atributos
            popup_html = f"<h4>Sector: {row['sector']}</h4>"
            popup_html += f"<b>Pozos:</b> {row['Pozos_Sector']}<br>"
            popup_html += f"<b>Población:</b> {row['Poblacion']}<br>"
            popup_html += f"<b>Volumen Producido:</b> {row['Vol_Prod']}<br>"
            popup_html += f"<b>Superficie:</b> {row['Superficie']} m²<br>"
            popup_html += f"<b>Longitud Red:</b> {row['Long_Red']} m<br>"
            popup_html += f"<b>U. Domésticos:</b> {row['U_Domesticos']}<br>"

            # Crear el Feature GeoJSON
            feature = {
                'type': 'Feature',
                'geometry': geojson_geometry,
                'properties': {
                    'sector': str(row['sector']),
                    'Poblacion': str(row['Poblacion']),
                    'Vol_Prod': str(row['Vol_Prod']),
                    'style': {
                        'fillColor': '#00FFFF',   # Color de relleno (Cian puro)
                        'color': '#00CED1',       # Color del borde (Turquesa oscuro/DarkTurquoise)
                        'weight': 1,
                        'fillOpacity': 0.6
                    },
                    'popup_html': popup_html # Contenido de HTML para el popup
                }
            }
            geojson_features.append(feature)

    except psycopg2.Error as err:
        print(f"❌ Error en la consulta de los sectores de PostgreSQL: {err}")
    except Exception as e:
        print(f"❌ Error al procesar datos de los sectotes de PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()

    return geojson_features

#========================================================================== FUNCIÓN DE EXTRACCIÓN DE POLIGONOS DEL MUNICIPIO DE AGUASCALIENTES==================================================================
def fetch_Municipio_Aguascalientes_geojson(config_posgres: Dict[str, str]) -> List[Dict[str, Any]]:
    conn = conectar_postgres(config_posgres)
    if conn is None:
        return []
    geojson_features = []
    try:
        # CONSULTA CORREGIDA FINAL: Incluye ST_Transform a 4326 y capitalización correcta
        final_query = """
            SELECT 
                
                ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data 
            FROM 
                "COMPLEMENTARIOS"."Municipio_ags";
        """
        print("⏳ Obteniendo datos geográficos del Municipio de Aguascalientes de PostgreSQL...")
        df = pd.read_sql(final_query, conn)
        
        for _, row in df.iterrows():
            try:
                geojson_geometry = json.loads(row['geojson_data'])
            except:
                continue 
            # Crear el Feature GeoJSON
            feature = {
                'type': 'Feature',
                'geometry': geojson_geometry,
                'properties': {
                        'style': {
                        'fillColor': '#00FFFF',   # Color de relleno (Cian puro)
                        'color': '#00CED1',       # Color del borde (Turquesa oscuro/DarkTurquoise)
                        'weight': 1,
                        'fillOpacity': 0.00,
                    },
                    
                }
            }
            geojson_features.append(feature)
    except psycopg2.Error as err:
        print(f"❌ Error en la consulta del Municipio de Aguascalientes de PostgreSQL: {err}")
    except Exception as e:
        print(f"❌ Error al procesar datos del Municipio de Aguascalientes de PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()
    return geojson_features

#========================================================================== FUNCIÓN DE EXTRACCIÓN DE POLIGONOS DEL ESTADO DE AGUASCALIENTES==================================================================
def fetch_Estado_Aguascalientes_geojson(config_posgres: Dict[str, str]) -> List[Dict[str, Any]]:
    conn = conectar_postgres(config_posgres)
    if conn is None:
        return []

    geojson_features = []
    
    try:
        # CONSULTA CORREGIDA FINAL: Incluye ST_Transform a 4326 y capitalización correcta
        final_query = """
            SELECT 
                
                ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data 
            FROM 
                "COMPLEMENTARIOS"."Estado_Aguascalientes";
        """
        print("⏳ Obteniendo datos geográficos del Estado de Aguascalientes de PostgreSQL...")
        df = pd.read_sql(final_query, conn)
        
        for _, row in df.iterrows():
            try:
                geojson_geometry = json.loads(row['geojson_data'])
            except:
                continue 
            # Crear el Feature GeoJSON
            feature = {
                'type': 'Feature',
                'geometry': geojson_geometry,
                'properties': {
                        'style': {
                        'fillColor': '#00FFFF',   # Color de relleno (Cian puro)
                        'color': '#00CED1',       # Color del borde (Turquesa oscuro/DarkTurquoise)
                        'weight': 1,
                        'fillOpacity': 0.00,
                    },
                    
                }
            }
            geojson_features.append(feature)
    except psycopg2.Error as err:
        print(f"❌ Error en la consulta del Estado de Aguascalientes de PostgreSQL: {err}")
    except Exception as e:
        print(f"❌ Error al procesar datos del Estado de Aguascalientes de PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()
    return geojson_features

#=======================================================================FUNCION QUE OPTIENE LAS VARIABLES (SIN CAMBIOS) =========================================================================================
def obtener_variables(nombre_variable):
    """
    Obtiene el valor y fecha de UNA variable (Pozos).
    """
    conn = None 
    try:
        conn = reintentar_conexion()
        if not conn:
             return None

        cursor = conn.cursor()
        
        # 1. Obtener GATEID
        cursor.execute("SELECT GATEID FROM VfiTagRef WHERE NAME = %s", (nombre_variable,))
        res = cursor.fetchone()
        
        if not res:
            return None
        gateid = res[0]
        
        # 2. Obtener VALUE y FECHA
        cursor.execute(
            "SELECT VALUE, FECHA FROM VfiTagNumHistory_Ultimo WHERE GATEID = %s ORDER BY FECHA DESC LIMIT 1", 
            (gateid,)
        )
        val_fecha = cursor.fetchone()
        
        if val_fecha:
            valor = val_fecha[0]
            fecha = val_fecha[1]
            return valor, fecha 
        else:
            return None
            
    except Exception as e:
        return None 
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass
#---------------------------------------------------------------obtener datos historicos de vfitagnumhistory ---------------------------------------------------------------------------

def obtener_datos_historicos_multiple(tag_caudal, tag_presion,tag_nivel_estatico, tag_sumergencia: List[str], tags_volt: List[str], tags_corr: List[str]):
    """
    Obtiene los datos históricos de caudal, presión y **voltajes de línea** de los últimos 7 días.
    Retorna un DataFrame de Pandas con columnas 'FECHA', 'Caudal', 'Presion', 'Voltaje_L1L2', etc.
    """
    conn = None 
    resultados = pd.DataFrame()
    
    hace_siete_dias = datetime.now() - timedelta(days=7)
    fecha_inicio_str = hace_siete_dias.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        conn = reintentar_conexion()
        if not conn:
             return resultados

        cursor = conn.cursor()
        
        # Obtener GATEIDs
        gateids = {}
        # 🟢 MODIFICACIÓN 1: Incluir tags individuales de voltaje y corriente en la lista para obtener GATEIDs
        all_tags = [tag_caudal, tag_presion,tag_nivel_estatico] + tags_volt + tags_corr
        
        for tag in all_tags:
            if tag and tag != '0': # Asegurar que '0' no se intente buscar
                cursor.execute("SELECT GATEID FROM VfiTagRef WHERE NAME = %s", (tag,))
                res = cursor.fetchone()
                if res:
                    gateids[tag] = res[0]
        
        if not gateids:
            return resultados
        
        # Obtener datos de VfiTagNumHistory
        query = """
            SELECT T1.VALUE, T1.FECHA, T2.NAME
            FROM vfitagnumhistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T1.GATEID IN (%s) AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        gateid_str = ', '.join(f"'{gid}'" for gid in gateids.values())
        
        cursor.execute(query % (gateid_str, '%s'), (fecha_inicio_str,))
        raw_data = cursor.fetchall()

        if not raw_data:
            return resultados
            
        # Transformar a DataFrame
        df = pd.DataFrame(raw_data, columns=['VALUE', 'FECHA', 'TAG_NAME'])
        df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')
        df['FECHA'] = pd.to_datetime(df['FECHA'])

        # Pivotar para tener variables como columnas separadas
        df_pivot = df.pivot(index='FECHA', columns='TAG_NAME', values='VALUE')
        
        df_final = pd.DataFrame(index=df_pivot.index)
        
        # Función auxiliar para obtener datos con ffill().bfill() o 0.0
        def get_and_clean_col(tag):
            return df_pivot.get(tag, pd.Series(0.0, index=df_pivot.index)).ffill().bfill()
        
        # Caudal y Presión
        df_final['Caudal'] = get_and_clean_col(tag_caudal)
        df_final['Presion'] = get_and_clean_col(tag_presion)
        df_final['Nivel_Estatico'] = get_and_clean_col(tag_nivel_estatico)
        df_final['Sumergencia'] = get_and_clean_col(tag_sumergencia)

        # 🟢 MODIFICACIÓN 2: Asignar las 3 líneas de voltaje a columnas separadas
        # Se asume que tags_volt tiene 3 elementos en el orden correcto (L1-L2, L2-L3, L1-L3)
        if len(tags_volt) >= 3:
            df_final['Voltaje_L1L2'] = get_and_clean_col(tags_volt[0])
            df_final['Voltaje_L2L3'] = get_and_clean_col(tags_volt[1])
            df_final['Voltaje_L1L3'] = get_and_clean_col(tags_volt[2])
        else:
            # Añadir columnas vacías si no hay suficientes tags de voltaje
            df_final['Voltaje_L1L2'] = 0.0
            df_final['Voltaje_L2L3'] = 0.0
            df_final['Voltaje_L1L3'] = 0.0
             
        # Corrientes (Se mantiene la estructura para corrientes L1, L2, L3 aunque no se usan en el gráfico solicitado)
        if len(tags_corr) >= 3:
            df_final['Corriente_L1'] = get_and_clean_col(tags_corr[0])
            df_final['Corriente_L2'] = get_and_clean_col(tags_corr[1])
            df_final['Corriente_L3'] = get_and_clean_col(tags_corr[2])
        
        return df_final.reset_index()
            
    except Exception as e:
        # print(f"Error al obtener datos históricos: {e}")
        return resultados 
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass
#---------------------------------------------------------------obtener datos historicos de tanques ---------------------------------------------------------------------------            
def obtener_datos_historicos_tanque(tag_nivel):
    """
    Obtiene los datos históricos de nivel del tanque de los últimos 7 días.
    Retorna un DataFrame de Pandas con columnas 'FECHA', 'Nivel'.
    """
    conn = None 
    resultados = pd.DataFrame()
    
    if not tag_nivel or tag_nivel == '0':
        return resultados
    
    hace_siete_dias = datetime.now() - timedelta(days=7)
    fecha_inicio_str = hace_siete_dias.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        conn = reintentar_conexion()
        if not conn:
             return resultados

        cursor = conn.cursor()
        
        # 1. Obtener GATEID del nivel
        gateids = {}
        cursor.execute("SELECT GATEID FROM VfiTagRef WHERE NAME = %s", (tag_nivel,))
        res = cursor.fetchone()
        if res:
            gateids[tag_nivel] = res[0]
        
        if not gateids:
            return resultados
        
        # 2. Obtener datos de VfiTagNumHistory
        query = """
            SELECT T1.VALUE, T1.FECHA
            FROM vfitagnumhistory T1
            JOIN VfiTagRef T2 ON T1.GATEID = T2.GATEID
            WHERE T1.GATEID = %s AND T1.FECHA >= %s
            ORDER BY T1.FECHA ASC
        """
        
        cursor.execute(query, (gateids[tag_nivel], fecha_inicio_str))
        raw_data = cursor.fetchall()

        if not raw_data:
            return resultados
            
        # 3. Transformar a DataFrame
        df = pd.DataFrame(raw_data, columns=['VALUE', 'FECHA'])
        df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')
        df['FECHA'] = pd.to_datetime(df['FECHA'])
        
        # Renombrar columna y rellenar nulos
        df_final = df.rename(columns={'VALUE': 'Nivel'}).set_index('FECHA')
        df_final['Nivel'] = df_final['Nivel'].ffill().bfill()
        
        return df_final.reset_index()
            
    except Exception as e:
        # print(f"Error al obtener datos históricos del tanque: {e}")
        return resultados 
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass         

    """
    Obtiene todos los datos de los pozos en paralelo usando ThreadPoolExecutor.
    """
    datos_rebombeo = {}
    print("⏳ Conectando y obteniendo variables para el mapa en paralelo...")
    # Usar ThreadPoolExecutor para ejecutar las llamadas a la DB de forma concurrente.
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        # Creamos un diccionario de futuros: {future_object: pozo_nombre}
        future_to_rebombeo = { 
            executor.submit(fetch_well_data, rebombeo, info): rebombeo 
            for rebombeo, info in mapa_rebombeos_dict.items() 
        }
        # Iteramos sobre los futuros a medida que se completan
        for future in concurrent.futures.as_completed(future_to_rebombeo):
            try:
                # El resultado es la tupla (pozo, datos) que devuelve fetch_well_data
                rebombeo, data = future.result()
                datos_rebombeo[rebombeo] = data
            except Exception as exc:
                rebombeo_nombre = future_to_rebombeo[future]
                # print(f'{pozo_nombre} generó una excepción: {exc}')
                # En caso de error, inicializa los datos como vacíos o cero para no fallar el mapa
                datos_rebombeo[rebombeo_nombre] = {
                    "encendido": False,
                    "corriente_total": 0.0,
                    "caudal": 0.0,
                    "caudal_fecha": None,
                    "presion": 0.0,
                    "presion_fecha": None,
                    "nivel_estatico": 0.0,
                    "nivel_estatico_fecha": None,
                    "nivel_tanque": 0.0,
                    "nivel_tanque_fecha": None,
                    "voltajes_resultados": [(None, None), (None, None), (None, None)],
                    "corrientes_resultados": [(None, None), (None, None), (None, None)],
                    "sumergencia": 0.0,
                    "sumergencia_fecha": None,
                    "datos_historicos": pd.DataFrame() 
                }
    return datos_rebombeo 
#---------------------------------------------------------------obtener datos historicos de macromedidores
def obtener_consumo_diario_historico_macrometer(macrometro_id):
    """
    Obtiene el Consumo_diario y la Fecha de los últimos 30 días para un macrómetro.
    Retorna un DataFrame de Pandas con columnas 'Fecha', 'Consumo'.
    """
    conn = None 
    resultados = pd.DataFrame()
    
    # 🚨 Se establece el rango de 30 días
    hace_treinta_dias = datetime.now() - timedelta(days=30)
    fecha_inicio_str = hace_treinta_dias.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        conn = reintentar_conexion_macrometers()
        if not conn:
             return resultados

        cursor = conn.cursor()
        
        # Consulta SQL: Consumo_diario y Fecha de los últimos 30 días
        query = """
            SELECT Fecha, Consumo_diario 
            FROM HES 
            WHERE Medidor = %s AND Fecha >= %s
            ORDER BY Fecha ASC
        """
        
        cursor.execute(query, (macrometro_id, fecha_inicio_str))
        raw_data = cursor.fetchall()

        if not raw_data:
            return resultados
            
        # Transformar a DataFrame y consolidar por día
        df = pd.DataFrame(raw_data, columns=['Fecha', 'Consumo'])
        df['Consumo'] = pd.to_numeric(df['Consumo'], errors='coerce')
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        
        # Si hay múltiples registros por día, los suma (asumiendo HES registra una vez al día)
        df_final = df.set_index('Fecha').resample('D').sum().reset_index()
        
        return df_final.fillna(0)
            
    except Exception as e:
        # print(f"Error al obtener datos históricos del macrómetro: {e}")
        return resultados 
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass

#---------------------------------------------------------------obtener datos para macromedidores ---------------------------------------------------------------------------            
def obtener_variable_macrometer(macrometro_id):
    """
    Obtiene el caudal más reciente (Lectura) y fecha de UNA variable de macrómetro.
    MODIFICADO: Consulta la tabla HES usando las columnas Lectura y Medidor.
    """
    conn = None 
    try:
        conn = reintentar_conexion_macrometers()
        if not conn:
             return (0.0, None)

        cursor = conn.cursor()
        
        # Se asume que macrometro_id (ej. "43000015") corresponde al campo 'Medidor'
        # CORRECCIÓN CLAVE 1: Se usa 'Lectura' y el filtro 'Medidor'
        cursor.execute(
            "SELECT Lectura, Fecha FROM HES WHERE Medidor = %s ORDER BY Fecha DESC LIMIT 1", 
            (macrometro_id,)
        )
        val_fecha = cursor.fetchone()
        
        if val_fecha:
            valor = val_fecha[0] # Lectura (caudal instantáneo)
            fecha = val_fecha[1]
            return (float(valor) if valor is not None else 0.0), fecha
        else:
            return (0.0, None)
            
    except Exception as e:
        # print(f"Error en obtener_variable_macrometer: {e}") 
        return (0.0, None)
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass
def obtener_consumo_mensual(macrometro_id):
    """
    Obtiene el Consumo_diario acumulado (total) del mes en curso para un macrómetro.
    """
    conn = None 
    consumo_acumulado = 0.0
    
    try:
        conn = reintentar_conexion_macrometers()
        if not conn:
             return 0.0

        cursor = conn.cursor()
        
        hoy = datetime.now()
        mes_actual = hoy.month
        anio_actual = hoy.year
        
        # Query para sumar Consumo_diario del mes en curso para un Medidor
        query = """
            SELECT SUM(Consumo_diario) 
            FROM HES 
            WHERE Medidor = %s
            AND YEAR(Fecha) = %s
            AND MONTH(Fecha) = %s
        """
        
        cursor.execute(query, (macrometro_id, anio_actual, mes_actual))
        resultado = cursor.fetchone()
        
        if resultado and resultado[0] is not None:
            consumo_acumulado = float(resultado[0]) 
            
    except Exception as e:
        # print(f"Error al obtener consumo mensual para {macrometro_id}: {e}")
        pass
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass
            
    return consumo_acumulado
def obtener_datos_mensuales_macrometer(macrometro_id):
    """
    🚨 NUEVA FUNCIÓN: Obtiene el promedio de Consumo_diario y el número total de lecturas
    para un macrómetro en el mes actual.
    """
    conn = None 
    try:
        conn = reintentar_conexion_macrometers()
        if not conn:
             return 0.0, 0 # promedio, num_lecturas

        cursor = conn.cursor()
        
        hoy = datetime.now()
        mes_actual = hoy.month
        anio_actual = hoy.year

        # Consulta SQL: Promedio de Consumo_diario y conteo de filas (lecturas)
        sql_query = """
        SELECT 
            AVG(Consumo_diario), 
            COUNT(*) 
        FROM 
            HES 
        WHERE 
            Medidor = %s AND 
            YEAR(Fecha) = %s AND 
            MONTH(Fecha) = %s
        """
        
        cursor.execute(sql_query, (macrometro_id, anio_actual, mes_actual))
        res = cursor.fetchone()
        
        if res and res[1] is not None and int(res[1]) > 0: # Verificar que hay lecturas
            avg_consumo = float(res[0]) if res[0] is not None else 0.0
            count_lecturas = int(res[1])
            return avg_consumo, count_lecturas
        else:
            return 0.0, 0
            
    except Exception as e:
        # print(f"Error al obtener datos mensuales para {macrometro_id}: {e}")
        return 0.0, 0
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass

#=====================================================================FUNCION DE EXTRACCIÓN DE DATOS  ===================================================================================


    """
    Obtiene el Consumo_diario y la Fecha de los últimos 30 días para un macrómetro.
    Retorna un DataFrame de Pandas con columnas 'Fecha', 'Consumo'.
    """
    conn = None 
    resultados = pd.DataFrame()
    
    # 🚨 Se establece el rango de 30 días
    hace_treinta_dias = datetime.now() - timedelta(days=30)
    fecha_inicio_str = hace_treinta_dias.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        conn = reintentar_conexion_macrometers()
        if not conn:
             return resultados

        cursor = conn.cursor()
        
        # Consulta SQL: Consumo_diario y Fecha de los últimos 30 días
        query = """
            SELECT Fecha, Consumo_diario 
            FROM HES 
            WHERE Medidor = %s AND Fecha >= %s
            ORDER BY Fecha ASC
        """
        
        cursor.execute(query, (macrometro_id, fecha_inicio_str))
        raw_data = cursor.fetchall()

        if not raw_data:
            return resultados
            
        # Transformar a DataFrame y consolidar por día
        df = pd.DataFrame(raw_data, columns=['Fecha', 'Consumo'])
        df['Consumo'] = pd.to_numeric(df['Consumo'], errors='coerce')
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        
        # Si hay múltiples registros por día, los suma (asumiendo HES registra una vez al día)
        df_final = df.set_index('Fecha').resample('D').sum().reset_index()
        
        return df_final.fillna(0)
            
    except Exception as e:
        # print(f"Error al obtener datos históricos del macrómetro: {e}")
        return resultados 
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass

#========================================================================FUNCIÓN DE AGREGACIÓN DE DATOS (CONCURRENTE) (sin cambios) =========================================================
def obtener_consumo_mensual_total_global(datos_macrometros):
    """
    Suma el consumo_mensual_total de todos los macromedidores.
    """
    total_consumo = 0.0
    for datos in datos_macrometros.values():
        total_consumo += datos.get("consumo_mensual_total", 0.0)
    return total_consumo            
#-------------------------------------------------------------------------------------------------
def obtener_datos_totales_pozos(mapa_pozos_dict):
    """
    Obtiene todos los datos de los pozos en paralelo usando ThreadPoolExecutor.
    """
    datos_pozos = {}
    print("⏳ Conectando y obteniendo variables para el mapa en paralelo...")
    # Usar ThreadPoolExecutor para ejecutar las llamadas a la DB de forma concurrente.
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        # Creamos un diccionario de futuros: {future_object: pozo_nombre}
        future_to_pozo = { 
            executor.submit(fetch_well_data, pozo, info): pozo 
            for pozo, info in mapa_pozos_dict.items() 
        }
        # Iteramos sobre los futuros a medida que se completan
        for future in concurrent.futures.as_completed(future_to_pozo):
            try:
                # El resultado es la tupla (pozo, datos) que devuelve fetch_well_data
                pozo, data = future.result()
                datos_pozos[pozo] = data
            except Exception as exc:
                pozo_nombre = future_to_pozo[future]
                # print(f'{pozo_nombre} generó una excepción: {exc}')
                # En caso de error, inicializa los datos como vacíos o cero para no fallar el mapa
                datos_pozos[pozo_nombre] = {
                    "encendido": False,
                    "corriente_total": 0.0,
                    "caudal": 0.0,
                    "caudal_fecha": None,
                    "presion": 0.0,
                    "presion_fecha": None,
                    "nivel_estatico": 0.0, 
                    "nivel_estatico_fecha": None,
                    "nivel_tanque": 0.0,
                    "nivel_tanque_fecha": None,
                    "voltajes_resultados": [(None, None), (None, None), (None, None)],
                    "corrientes_resultados": [(None, None), (None, None), (None, None)],
                    "sumergencia": 0.0,
                    "sumergencia_fecha": None,
                    "datos_historicos": pd.DataFrame() 
                }
    return datos_pozos
#---------------------------------------------------------------obtener datos historicos de rebombeos ---------------------------------------------------------------------------  
def obtener_datos_totales_rebombeos(mapa_rebombeos_dict):
    """
    Obtiene todos los datos de los pozos en paralelo usando ThreadPoolExecutor.
    """
    datos_rebombeo = {}
    print("⏳ Conectando y obteniendo variables para el mapa en paralelo...")
    # Usar ThreadPoolExecutor para ejecutar las llamadas a la DB de forma concurrente.
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        # Creamos un diccionario de futuros: {future_object: pozo_nombre}
        future_to_rebombeo = { 
            executor.submit(fetch_well_data, rebombeo, info): rebombeo 
            for rebombeo, info in mapa_rebombeos_dict.items() 
        }
        # Iteramos sobre los futuros a medida que se completan
        for future in concurrent.futures.as_completed(future_to_rebombeo):
            try:
                # El resultado es la tupla (pozo, datos) que devuelve fetch_well_data
                rebombeo, data = future.result()
                datos_rebombeo[rebombeo] = data
            except Exception as exc:
                rebombeo_nombre = future_to_rebombeo[future]
                # print(f'{pozo_nombre} generó una excepción: {exc}')
                # En caso de error, inicializa los datos como vacíos o cero para no fallar el mapa
                datos_rebombeo[rebombeo_nombre] = {
                    "encendido": False,
                    "corriente_total": 0.0,
                    "caudal": 0.0,
                    "caudal_fecha": None,
                    "presion": 0.0,
                    "presion_fecha": None,
                    "nivel_estatico": 0.0,
                    "nivel_estatico_fecha": None,
                    "nivel_tanque": 0.0,
                    "nivel_tanque_fecha": None,
                    "voltajes_resultados": [(None, None), (None, None), (None, None)],
                    "corrientes_resultados": [(None, None), (None, None), (None, None)],
                    "sumergencia": 0.0,
                    "sumergencia_fecha": None,
                    "datos_historicos": pd.DataFrame() 
                }
    return datos_rebombeo 
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def obtener_datos_totales_macrometros(macrometer_tags_dict: Dict[str, str]):
    """
    Obtiene todos los datos de los macromedidores en paralelo.
    """
    datos_macrometros = {}
    print("⏳ Conectando y obteniendo variables para macromedidores en paralelo...")
    
    def fetch_macrometer_data(nombre, id_numerico):
        """
        Función para obtener todos los datos necesarios para un solo macrómetro.
        """
        caudal, fecha = obtener_variable_macrometer(id_numerico) # Caudal instantáneo
        consumo_mensual = obtener_consumo_mensual(id_numerico) # Consumo Acumulado
        avg_consumo_diario, num_lecturas_mes = obtener_datos_mensuales_macrometer(id_numerico)
        datos_historicos_consumo = obtener_consumo_diario_historico_macrometer(id_numerico)
        
        return nombre, {
            "caudal": caudal,
            "caudal_fecha": fecha,
            "consumo_mensual_total": consumo_mensual, # Total
            "consumo_mensual_avg": avg_consumo_diario, # Promedio
            "lecturas_count": num_lecturas_mes, # Conteo
            "datos_historicos_consumo": datos_historicos_consumo # <-- AÑADIDO
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_macrometer = {
            executor.submit(fetch_macrometer_data, nombre, tag): nombre 
            for nombre, tag in macrometer_tags_dict.items()
        }
        for future in concurrent.futures.as_completed(future_to_macrometer):
            try:
                nombre, data = future.result()
                datos_macrometros[nombre] = data
            except Exception as exc:
                nombre_macro = future_to_macrometer[future]
                print(f'{nombre_macro} generó una excepción: {exc}')
                datos_macrometros[nombre_macro] = {
                    "caudal": 0.0,
                    "caudal_fecha": None,
                    "consumo_mensual_total": 0.0,
                    "consumo_mensual_avg": 0.0,
                    "lecturas_count": 0,
                    "datos_historicos_consumo": pd.DataFrame()
                }
    return datos_macrometros
#------------------------------------------------------------------------------
def obtener_datos_totales_tanques(mapa_tanques_dict):
    """
    Obtiene todos los datos de los tanques en paralelo usando ThreadPoolExecutor.
    """
    datos_tanques = {}
    print("⏳ Conectando y obteniendo variables para tanques en paralelo...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: # Menos workers que pozos
        future_to_tanque = { 
            executor.submit(fetch_tank_data, tanque, info): tanque 
            for tanque, info in mapa_tanques_dict.items() 
        }

        for future in concurrent.futures.as_completed(future_to_tanque):
            tanque = future_to_tanque[future]
            try:
                tanque_nombre, data = future.result()
                datos_tanques[tanque_nombre] = data
            except Exception as exc:
                print(f'{tanque} generó una excepción: {exc}')
                # En caso de error, inicializa los datos como vacíos o cero.
                datos_tanques[tanque] = {
                    "nivel_actual": 0.0, 
                    "nivel_fecha": None,
                    "nivel_max": mapa_tanques_dict[tanque].get("nivel_max", 1.0),
                    "porcentaje": 0.0,
                    "datos_historicos": pd.DataFrame() # <--- AGREGADO EN ERROR
                }
    return datos_tanques


#========================================================================FUNCIÓNES DE GRÁFICOS ================================================================================================
#-------------------------------------------------------------- 1 Grafico de caudla, presion, voltajes y amperajes
def generar_grafico_caudal_y_presion(pozo, df_historico):

    # 🟢 MODIFICACIÓN 3: Cambiar la verificación de columnas. Solo Caudal y Presion son obligatorias.
    if df_historico.empty or ('Caudal' not in df_historico.columns) or ('Presion' not in df_historico.columns):
        # Retornar un HTML simple para mostrar que no hay datos.
        return '<div style="color: #FF4500; text-align: center; margin: 10px 0;">Gráfico de C/P/V/A: Datos históricos insuficientes.</div>'
        
    df = df_historico.copy()
    
    # Redondear y limitar valores para limpiar el gráfico (opcional)
    df['Caudal'] = df['Caudal'].clip(upper=150) # Ejemplo: caudal máximo 150
    df['Presion'] = df['Presion'].clip(upper=10) # Ejemplo: presión máxima 10

    # Crear la figura Plotly
    fig = go.Figure()
    
    # Flags para verificar si se añadió alguna traza de Voltaje o Corriente
    voltaje_added = False
    corriente_added = False # <-- Nuevo flag para Corriente

    # 1. Traza para Caudal (Azul Turquesa - Eje Y Izquierdo)
    fig.add_trace(go.Scatter(
        x=df['FECHA'],
        y=df['Caudal'],
        name='Caudal (l/s)',
        mode='lines+markers', # <-- LÍNEA Y MARCADOR
        marker=dict(size=5), # <-- Definimos el tamaño del punto
        line=dict(color='#00CED1', width=2), # <-- AZUL TURQUESA (DarkTurquoise)
        yaxis='y1',
    ))

    # 2. Traza para Presión (Verde - Eje Y Derecho)
    fig.add_trace(go.Scatter(
        x=df['FECHA'],
        y=df['Presion'],
        name='Presión (Kg/cm²)',
        mode='lines+markers', # <-- LÍNEA Y MARCADOR
        marker=dict(size=5), # <-- Definimos el tamaño del punto
        line=dict(color="#09FF00", width=2),
        yaxis='y2'
    ))
    
    # 🟢 MODIFICACIÓN 4: Agregar las 3 trazas de Voltaje (L1-L2, L2-L3, L1-L3)
    if 'Voltaje_L1L2' in df.columns and df['Voltaje_L1L2'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L1L2'],
            name='Volt L1', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#FFD700', width=2),
            yaxis='y3'
        ))
        voltaje_added = True
        
    if 'Voltaje_L2L3' in df.columns and df['Voltaje_L2L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L2L3'],
            name='Volt L2', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#FFA07A', width=2), 
            yaxis='y3'
        ))
        voltaje_added = True
        
    if 'Voltaje_L1L3' in df.columns and df['Voltaje_L1L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L1L3'],
            name='Volt L3', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#8A2BE2', width=2), 
            yaxis='y3'
        ))
        voltaje_added = True

    # 🟢 NUEVA MODIFICACIÓN: Agregar las 3 trazas de Corriente (L1, L2, L3)
    if 'Corriente_L1' in df.columns and df['Corriente_L1'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L1'],
            name='Amp L1',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='red', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True
        
    if 'Corriente_L2' in df.columns and df['Corriente_L2'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L2'],
            name='Amp L2',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='orange', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True
        
    if 'Corriente_L3' in df.columns and df['Corriente_L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L3'],
            name='Amp L3',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='yellow', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True

    # Configuración de Layout para look SCADA
    fig.update_layout(
        
        plot_bgcolor='#111', # Fondo del área del gráfico
        paper_bgcolor='#0b1a29', # Fondo del contenedor
        font=dict(color='white'),
        hovermode="x unified",
        xaxis=dict(
            title='Fecha y Hora',
            showgrid=True, gridwidth=1, gridcolor='#333', zeroline=False
        ),
        yaxis=dict( # Y1: Caudal
            title='Caudal (l/s)',
            title_font=dict(color='#00CED1'),
            tickfont=dict(color='#00CED1'),
            showgrid=True, gridwidth=1, gridcolor='#333', zeroline=False,
            tickformat='.2f' # <-- AÑADIDO: Formato a dos decimales
        ),
            yaxis2=dict( # Y2: Presión (Derecha - Eje Central)
            title='Presión (Kg/cm²)',
            title_font=dict(color='lime'),
            tickfont=dict(color='lime'),
            overlaying='y',
            side='right',
            anchor='free', 
            position=1.00,
            showgrid=False,
            zeroline=False,
            tickformat='.2f' # <-- AÑADIDO: Formato a dos decimales
        ),
        # 🟢 MODIFICACIÓN 5: Ajustar el Eje Y3 para Voltaje (Derecha, Eje Izquierdo)
        yaxis3=dict( 
            title='Voltaje y Amperaje', 
            title_font=dict(color='#FFD700'), # Oro
            tickfont=dict(color='#FFD700'),
            overlaying='y',
            side='right',
            anchor='free', # Anclaje libre
            position=0.98, # <-- CORREGIDO: Más hacia el centro del gráfico (Izquierda de Y2)
            showgrid=False, 
            zeroline=False,
            tickformat='.0f', # <-- AÑADIDO: Formato a dos decimales
            rangemode='tozero',
            range=[0, 600], 
        ) if voltaje_added else None,
                
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor='rgba(0,0,0,0)',
        ),
        margin=dict(l=40, r=40, t=60, b=40)
    )

    # Generar el HTML
    html_output = pio.to_html(
        fig, 
        full_html=False, 
        include_plotlyjs='cdn', 
        config={'displayModeBar': True}
    )
    # Encapsular el HTML generado para darle un estilo base. AQUI SE CONTROLA EL ALTO Y ANCHO DEL GRAFICO MOSTRADO EN EL POPUP DE LOS POZOS
    return f'<div style="margin-top: 10px; border: 2px solid #333; height: 280px; width: 900px; overflow: hidden;">{html_output}</div>'
#-------------------------------------------------------------- 2 Grafico de rebombeos
def generar_grafico_rebombeo(rebombeo, df_historico):

    # 🟢 MODIFICACIÓN 3: Cambiar la verificación de columnas. Solo Caudal y Presion son obligatorias.
    if df_historico.empty or ('Caudal' not in df_historico.columns) or ('Presion' not in df_historico.columns):
        # Retornar un HTML simple para mostrar que no hay datos.
        return '<div style="color: #FF4500; text-align: center; margin: 10px 0;">Gráfico de C/P/V/A: Datos históricos insuficientes.</div>'
        
    df = df_historico.copy()
    
    # Redondear y limitar valores para limpiar el gráfico (opcional)
    df['Caudal'] = df['Caudal'].clip(upper=150) # Ejemplo: caudal máximo 150
    df['Presion'] = df['Presion'].clip(upper=10) # Ejemplo: presión máxima 10

    # Crear la figura Plotly
    fig = go.Figure()
    
    # Flags para verificar si se añadió alguna traza de Voltaje o Corriente
    voltaje_added = False
    corriente_added = False # <-- Nuevo flag para Corriente

    # 1. Traza para Caudal (Azul Turquesa - Eje Y Izquierdo)
    fig.add_trace(go.Scatter(
        x=df['FECHA'],
        y=df['Caudal'],
        name='Caudal (l/s)',
        mode='lines+markers', # <-- LÍNEA Y MARCADOR
        marker=dict(size=5), # <-- Definimos el tamaño del punto
        line=dict(color='#00CED1', width=2), # <-- AZUL TURQUESA (DarkTurquoise)
        yaxis='y1',
    ))

    # 2. Traza para Presión (Verde - Eje Y Derecho)
    fig.add_trace(go.Scatter(
        x=df['FECHA'],
        y=df['Presion'],
        name='Presión (Kg/cm²)',
        mode='lines+markers', # <-- LÍNEA Y MARCADOR
        marker=dict(size=5), # <-- Definimos el tamaño del punto
        line=dict(color="#09FF00", width=2),
        yaxis='y2'
    ))
    
    # 🟢 MODIFICACIÓN 4: Agregar las 3 trazas de Voltaje (L1-L2, L2-L3, L1-L3)
    if 'Voltaje_L1L2' in df.columns and df['Voltaje_L1L2'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L1L2'],
            name='Volt L1', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#FFD700', width=2),
            yaxis='y3'
        ))
        voltaje_added = True
        
    if 'Voltaje_L2L3' in df.columns and df['Voltaje_L2L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L2L3'],
            name='Volt L2', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#FFA07A', width=2), 
            yaxis='y3'
        ))
        voltaje_added = True
        
    if 'Voltaje_L1L3' in df.columns and df['Voltaje_L1L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L1L3'],
            name='Volt L3', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#8A2BE2', width=2), 
            yaxis='y3'
        ))
        voltaje_added = True

    # 🟢 NUEVA MODIFICACIÓN: Agregar las 3 trazas de Corriente (L1, L2, L3)
    if 'Corriente_L1' in df.columns and df['Corriente_L1'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L1'],
            name='Amp L1',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='red', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True
        
    if 'Corriente_L2' in df.columns and df['Corriente_L2'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L2'],
            name='Amp L2',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='orange', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True
        
    if 'Corriente_L3' in df.columns and df['Corriente_L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L3'],
            name='Amp L3',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='yellow', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True

    # Configuración de Layout para look SCADA
    fig.update_layout(
        
        plot_bgcolor='#111', # Fondo del área del gráfico
        paper_bgcolor='#0b1a29', # Fondo del contenedor
        font=dict(color='white'),
        hovermode="x unified",
        xaxis=dict(
            title='Fecha y Hora',
            showgrid=True, gridwidth=1, gridcolor='#333', zeroline=False
        ),
        yaxis=dict( # Y1: Caudal
            title='Caudal (l/s)',
            title_font=dict(color='#00CED1'),
            tickfont=dict(color='#00CED1'),
            showgrid=True, gridwidth=1, gridcolor='#333', zeroline=False,
            tickformat='.2f' # <-- AÑADIDO: Formato a dos decimales
        ),
            yaxis2=dict( # Y2: Presión (Derecha - Eje Central)
            title='Presión (Kg/cm²)',
            title_font=dict(color='lime'),
            tickfont=dict(color='lime'),
            overlaying='y',
            side='right',
            anchor='free', 
            position=1.00,
            showgrid=False,
            zeroline=False,
            tickformat='.2f' # <-- AÑADIDO: Formato a dos decimales
        ),
        # 🟢 MODIFICACIÓN 5: Ajustar el Eje Y3 para Voltaje (Derecha, Eje Izquierdo)
        yaxis3=dict( 
            title='Voltaje y Amperaje', 
            title_font=dict(color='#FFD700'), # Oro
            tickfont=dict(color='#FFD700'),
            overlaying='y',
            side='right',
            anchor='free', # Anclaje libre
            position=0.98, # <-- CORREGIDO: Más hacia el centro del gráfico (Izquierda de Y2)
            showgrid=False, 
            zeroline=False,
            tickformat='.0f', # <-- AÑADIDO: Formato a dos decimales
            rangemode='tozero',
            range=[0, 600], 
        ) if voltaje_added else None,
                
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor='rgba(0,0,0,0)',
        ),
        margin=dict(l=40, r=40, t=60, b=40)
    )

    # Generar el HTML
    html_output = pio.to_html(
        fig, 
        full_html=False, 
        include_plotlyjs='cdn', 
        config={'displayModeBar': True}
    )
    # Encapsular el HTML generado para darle un estilo base. AQUI SE CONTROLA EL ALTO Y ANCHO DEL GRAFICO MOSTRADO EN EL POPUP DE LOS POZOS
    return f'<div style="margin-top: 10px; border: 2px solid #333; height: 280px; width: 900px; overflow: hidden;">{html_output}</div>'
#-------------------------------------------------------------- 3 Grafico de tanques
def generar_grafico_nivel_tanque(tanque, nivel_max, df_historico):
    """
    Genera un gráfico Plotly interactivo de la tendencia de Nivel del tanque.
    """
    if df_historico.empty or 'Nivel' not in df_historico.columns:
        return '<div style="color: #FF4500; text-align: center; margin: 10px 0;">Gráfico: Datos históricos de Nivel insuficientes.</div>'
        
    df = df_historico.copy()
    
    # Crear la figura Plotly
    fig = go.Figure()

    # 1. Traza para Nivel (Naranja - Eje Y Izquierdo)
    fig.add_trace(go.Scatter(
        x=df['FECHA'], y=df['Nivel'], 
        name='Nivel (m)', 
        mode='lines+markers', # <-- LÍNEA Y MARCADOR
        marker=dict(size=5), # <-- Definimos el tamaño del punto
        line=dict(color="#00A2FF", width=2), # NARANJA
        yaxis='y1',
        fill='tozeroy',
        fillcolor='rgba(255,165,0,0.2)'
    ))
    
    # 2. Línea de Nivel Máximo (referencia)
    fig.add_shape(
        type="line",
        x0=df['FECHA'].min(),
        y0=nivel_max,
        x1=df['FECHA'].max(),
        y1=nivel_max,
        line=dict(color="#90EE90", width=1, dash="dash"),
        name='Nivel Máximo'
    )
    
    # Configuración de Layout para look SCADA
    fig.update_layout(
        title=f'Histórico 7 Días - {tanque}',
        title_font_color='white',
        plot_bgcolor='#0b1a29', 
        paper_bgcolor='#0b1a29', 
        font=dict(color='white', size=9),
        height=280, 
        margin=dict(l=40, r=40, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, traceorder="normal"),
        
        hovermode='x unified', 
        
        # Eje Y Izquierdo (Nivel)
        yaxis=dict(
            title='Nivel (m)',
            title_font=dict(color='#FFA500'), # Color de título naranja
            tickfont=dict(color='#FFA500'), # Color de ticks naranja
            range=[0, nivel_max * 1.1], # Ajustar rango visible
            showgrid=False,
            zeroline=False
        ),
        
        # Eje X (Fecha)
        xaxis=dict(
            showgrid=False,
            tickfont=dict(color='white'),
            rangeselector=None 
        )
    )

    # Convertir figura a HTML puro
    html_output = pio.to_html(
        fig, 
        full_html=False, 
        include_plotlyjs='cdn', 
        config={'displayModeBar': True}
    )
    
    return html_output # Devolvemos solo el HTML del gráfico
#-------------------------------------------------------------- 4 Grafico de los Macromedidores
def generar_grafico_consumo_macrometer(macrometro, df_historico):
    """
    Genera un gráfico Plotly interactivo del consumo diario de los últimos 30 días.
    """
    if df_historico.empty or 'Consumo' not in df_historico.columns:
        return '<div style="color: #FF4500; text-align: center; margin: 10px 0;">Gráfico: Datos de consumo histórico insuficientes o no disponibles (últimos 30 días).</div>'
        
    df = df_historico.copy()
    
    # Crear la figura Plotly
    fig = go.Figure()

    # 1. Traza para Consumo (Azul)
    fig.add_trace(go.Bar(
        x=df['Fecha'], y=df['Consumo'], 
        name='Consumo Diario (m³)', 
        marker_color='#1E90FF', # Azul Dodger Blue
        opacity=0.8
    ))
    
    # Configuración de Layout para look SCADA
    fig.update_layout(
        title=f'Consumo Diario (Últimos 30 Días) - {macrometro}',
        title_font_color='white',
        plot_bgcolor='#0b1a29', 
        paper_bgcolor='#0b1a29', 
        font=dict(color='white', size=9),
        height=280, 
        margin=dict(l=40, r=40, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        
        hovermode='x unified', 
        
        # Eje Y (Consumo Diario)
        yaxis=dict(
            title='Consumo Diario (m³)',
            title_font=dict(color='#1E90FF'),
            tickfont=dict(color='#1E90FF'),
            showgrid=False,
            zeroline=False
        ),
        
        # Eje X (Fecha)
        xaxis=dict(
            type='date',
            showgrid=False,
            tickformat='%d/%m', # Mostrar solo día y mes
            tickfont=dict(color='white'),
            rangeselector=None 
        )
    )

    # Convertir figura a HTML puro
    html_output = pio.to_html(
        fig, 
        full_html=False, 
        include_plotlyjs='cdn', 
        config={'displayModeBar': True}
    )
    
    return html_output
#-------------------------------------------------------------- 5 Grafico de caudla, presion, voltajes y amperajes para el boton del popup
def generar_grafico_popup_pozos(pozo, df_historico):

    # 🟢 MODIFICACIÓN 3: Cambiar la verificación de columnas. Solo Caudal y Presion son obligatorias.
    if df_historico.empty or ('Caudal' not in df_historico.columns) or ('Presion' not in df_historico.columns):
        # Retornar un HTML simple para mostrar que no hay datos.
        return '<div style="color: #FF4500; text-align: center; margin: 10px 0;">Gráfico de C/P/V/A: Datos históricos insuficientes.</div>'
        
    df = df_historico.copy()
    
    # Redondear y limitar valores para limpiar el gráfico (opcional)
    df['Caudal'] = df['Caudal'].clip(upper=150) # Ejemplo: caudal máximo 150
    df['Presion'] = df['Presion'].clip(upper=10) # Ejemplo: presión máxima 10
   
    # Crear la figura Plotly
    fig = go.Figure()
    
    # Flags para verificar si se añadió alguna traza de Voltaje o Corriente
    voltaje_added = False
    
    corriente_added = False # <-- Nuevo flag para Corriente

    # 1. Traza para Caudal (Azul Turquesa - Eje Y Izquierdo)
    fig.add_trace(go.Scatter(
        x=df['FECHA'],
        y=df['Caudal'],
        name='Caudal (l/s)',
        mode='lines+markers', # <-- LÍNEA Y MARCADOR
        marker=dict(size=5), # <-- Definimos el tamaño del punto
        line=dict(color='#00CED1', width=2), # <-- AZUL TURQUESA (DarkTurquoise)
        yaxis='y1',
    ))

    # 2. Traza para Presión (Verde - Eje Y Derecho)
    fig.add_trace(go.Scatter(
        x=df['FECHA'],
        y=df['Presion'],
        name='Presión (Kg/cm²)',
        mode='lines+markers', # <-- LÍNEA Y MARCADOR
        marker=dict(size=5), # <-- Definimos el tamaño del punto
        line=dict(color="#09FF00", width=2),
        yaxis='y2'
    ))

    # 🟢 MODIFICACIÓN 4: Agregar las 3 trazas de Voltaje (L1-L2, L2-L3, L1-L3)
    if 'Voltaje_L1L2' in df.columns and df['Voltaje_L1L2'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L1L2'],
            name='Volt L1', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#FFD700', width=2),
            yaxis='y3'
        ))
        voltaje_added = True
        
    if 'Voltaje_L2L3' in df.columns and df['Voltaje_L2L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L2L3'],
            name='Volt L2', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#FFA07A', width=2), 
            yaxis='y3'
        ))
        voltaje_added = True
        
    if 'Voltaje_L1L3' in df.columns and df['Voltaje_L1L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Voltaje_L1L3'],
            name='Volt L3', # Nombre más específico
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='#8A2BE2', width=2), 
            yaxis='y3'
        ))
        voltaje_added = True

    # 🟢 NUEVA MODIFICACIÓN: Agregar las 3 trazas de Corriente (L1, L2, L3)
    if 'Corriente_L1' in df.columns and df['Corriente_L1'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L1'],
            name='Amp L1',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='red', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True

    if 'Corriente_L2' in df.columns and df['Corriente_L2'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L2'],
            name='Amp L2',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='orange', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True
        
    if 'Corriente_L3' in df.columns and df['Corriente_L3'].max() > 0:
        fig.add_trace(go.Scatter(
            x=df['FECHA'],
            y=df['Corriente_L3'],
            name='Amp L3',
            mode='lines+markers', # <-- LÍNEA Y MARCADOR
            marker=dict(size=5), # <-- Definimos el tamaño del punto
            line=dict(color='yellow', width=2), 
            yaxis='y3' # <-- Usar Eje Y4
        ))
        corriente_added = True
    # --- 5. TRAZA: NIVEL ESTÁTICO ---
    if 'Nivel_Estatico' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['FECHA'], y=df['Nivel_Estatico'],
            name='Nivel Estático (m)',
            mode='lines+markers',
            marker=dict(size=4),
            line=dict(color="#C300FF", width=2.5),
            yaxis='y5' 
        )) 
    # --- 6. TRAZA: SUMERGENCIA ---
    if 'Sumergencia' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['FECHA'], y=df['Sumergencia'],
            name='Sumergencia (m)',
            mode='lines+markers',
            marker=dict(size=4),
            line=dict(color="#00FFDD", width=2.5),
            yaxis='y6' 
        ))         

    # Configuración de Layout para look SCADA --------------------------------------------------------------------------------------------------------------------------------------
    fig.update_layout(
        
        plot_bgcolor='#111', # Fondo del área del gráfico
        paper_bgcolor='#0b1a29', # Fondo del contenedor
        font=dict(color='white'),
        hovermode="x unified",
        xaxis=dict(
            title='Fecha y Hora',
            showgrid=True, gridwidth=1, gridcolor='#333', zeroline=False
        ),
            yaxis=dict( # Y1: Caudal
            title='Caudal (l/s)',
            title_font=dict(color='#00CED1'),
            tickfont=dict(color='#00CED1'),
            showgrid=True, gridwidth=1, gridcolor='#333', zeroline=False,
            tickformat='.2f' # <-- AÑADIDO: Formato a dos decimales
        ),
            yaxis2=dict( # Y2: Presión (Derecha - Eje Central)
            title='Presión (Kg/cm²)',
            title_font=dict(color='lime'),
            tickfont=dict(color='lime'),
            overlaying='y',
            side='right',
            anchor='free', 
            position=1.00,
            showgrid=False,
            zeroline=False,
            tickformat='.2f' # <-- AÑADIDO: Formato a dos decimales
        ),
            yaxis5=dict( # Y5: Nivel dinamico (Derecha - Eje Central)
            title='Nivel Dinamico / Estatico (m)',
            title_font=dict(color="#C300FF"),
            tickfont=dict(color="#C300FF"),
            overlaying='y',
            side='right',
            anchor='free', 
            position=0.95,
            showgrid=False,
            zeroline=False,
            tickformat='.2f' # <-- AÑADIDO: Formato a dos decimales
        ),      
        
            yaxis6=dict( # Y5: Sumergencia (Izquierdo - Eje Central)
            title='Sumergencia (m)',
            title_font=dict(color="#C300FF"),
            tickfont=dict(color="#C300FF"),
            overlaying='y',
            side='left',
            anchor='free', 
            position=0.90,
            showgrid=False,
            zeroline=False,
            tickformat='.2f' # <-- AÑADIDO: Formato a dos decimales
        ),        
                   
        # 🟢 MODIFICACIÓN 5:  Y3 para Voltaje (Derecha, Eje Izquierdo)
        yaxis3=dict( 
            title='Voltaje y Amperaje', 
            title_font=dict(color='#FFD700'), # Oro
            tickfont=dict(color='#FFD700'),
            overlaying='y',
            side='right',
            anchor='free', # Anclaje libre
            position=0.98, # <-- CORREGIDO: Más hacia el centro del gráfico (Izquierda de Y2)
            showgrid=False, 
            zeroline=False,
            tickformat='.0f', # <-- AÑADIDO: Formato a dos decimales
            rangemode='tozero',
            range=[0, 600], 
        ) if voltaje_added else None,
        
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor='rgba(0,0,0,0)',
        ),
        margin=dict(l=40, r=40, t=60, b=40)
        
    )

    # Generar el HTML
    html_output = pio.to_html(
        fig, 
        full_html=False, 
        include_plotlyjs='cdn', 
        config={'displayModeBar': True}
    )
    # Encapsular el HTML generado para darle un estilo base. AQUI SE CONTROLA EL ALTO Y ANCHO DEL GRAFICO MOSTRADO EN EL POPUP DE LOS POZOS
    return f'<div style="margin-top: 10px; border: 2px solid #333; height: 600px; width: 1460px; overflow: hidden;">{html_output}</div>'
# ======================================================================FUNCION DE FORMATO DE FECHA (MODIFICADA) ===============================================================================
UMBRAL_ANTIGUEDAD = timedelta(hours=4)

def format_fecha_simple(fecha, es_macromedidor=False):
    """
    Formatea la fecha de la última lectura para Pozos y Macromeds (D/M/A H:M).
    Devuelve un span HTML que incluye los paréntesis y el color de obsolescencia.
    """
    if fecha:
        try:
            if isinstance(fecha, str):
                dt_obj = datetime.strptime(fecha.split('.')[0], '%Y-%m-%d %H:%M:%S')
            else:
                dt_obj = fecha
            
            ahora = datetime.now()
            diferencia_tiempo = ahora - dt_obj
            
            # Lógica de color
            if es_macromedidor:
                color_texto = '#191970' 
            else:
                # Lógica de obsolescencia para pozos
                # Rosa si es obsoleto (> 4h), Verde si es actual
                color_texto = "#FF69B4" if diferencia_tiempo >= UMBRAL_ANTIGUEDAD else '#90EE90' 
                
            # 🚨 CAMBIO CLAVE: Se incluye el día, mes y año.
            fecha_formateada = dt_obj.strftime('%d/%m/%Y %H:%M') 
            
            # Se devuelve el span con el formato completo
            return f'<span style="font-size: 8pt; color: {color_texto}; white-space: nowrap;">({fecha_formateada})</span>'
            
        except Exception:
            # Color de error.
            error_color = '#FF4500' # Naranja rojizo para error
            return f'<span style="font-size: 8pt; color: {error_color}; white-space: nowrap;"> (Sin fecha)</span>'
            
    return ""

def format_fecha_completa_tanque(fecha):
    """
    NUEVA FUNCIÓN: Formatea la fecha y hora completa (DD/MM/YYYY HH:MM) 
    y aplica color de obsolescencia (verde/rosa) para los Tanques.
    """
    if fecha:
        try:
            if isinstance(fecha, str):
                dt_obj = datetime.strptime(fecha.split('.')[0], '%Y-%m-%d %H:%M:%S')
            else:
                dt_obj = fecha
            
            ahora = datetime.now()
            diferencia_tiempo = ahora - dt_obj
            
            # Color: Verde (#90EE90) si es actual, Rosa (#FF69B4) si es obsoleto (> 4h)
            color_texto = "#FF69B4" if diferencia_tiempo >= UMBRAL_ANTIGUEDAD else '#90EE90' 
            
            # Formato de salida: DD/MM/YYYY HH:MM
            fecha_formateada = dt_obj.strftime('%d/%m/%Y %H:%M') 
            
            # Se devuelve el span con el formato y color
            return f'<span style="font-size: 10pt; color: {color_texto}; white-space: nowrap; font-weight: bold;">({fecha_formateada})</span>'
            
        except Exception:
            error_color = '#FF4500' # Naranja rojizo para error
            return f'<span style="font-size: 10pt; color: {error_color}; white-space: nowrap;"> (Error de Fecha)</span>'
            
    return ""

# ======================================================================FUNCIÓN TRABAJADORA PARA UN POZO (ACTUALIZADA) ========================================================================
def fetch_well_data(pozo, info):
    """ Obtiene todos los datos instantáneos y históricos para un pozo. """
    
    # Función auxiliar para obtener valor y fecha
    def get_value_and_date(nombre_variable):
        """Llama a obtener_variables y retorna (valor, fecha) o (0.0, None)."""
        if not nombre_variable or nombre_variable == '0':
            return 0.0, None
        resultado = obtener_variables(nombre_variable)
        if isinstance(resultado, tuple) and len(resultado) == 2:
            valor, fecha = resultado
            # Convertimos None a 0.0 aquí para evitar errores de formato posteriores
            return (float(valor) if valor is not None else 0.0), fecha
        return 0.0, None

    # 1. Corrientes (para Estado ON/OFF)
    nombres_corrientes = info.get("corrientes_l") or ([info.get("corriente")] if info.get("corriente") else [])
    corrientes_resultados = [get_value_and_date(nombre_corr) for nombre_corr in nombres_corrientes]
    corrientes_valores = [v[0] for v in corrientes_resultados if isinstance(v, tuple)]
    corriente_total = sum(corrientes_valores)
    encendido = corriente_total > 0.10

    # 2. Caudal y Presión (Instantánea)
    caudal_tag = info.get("caudal")
    presion_tag = info.get("presion")
    
    caudal, caudal_fecha = get_value_and_date(caudal_tag)
    presion, presion_fecha = get_value_and_date(presion_tag)

    # Nivel Estático, Sumergencia y Nivel de Tanque (Instantánea)
    nivel_estatico_tag = info.get("nivel_estatico")
    sumergencia_tag = info.get("sumergencia")
    nivel_tanque_tag = info.get("nivel_tanque")
    
    nivel_estatico, nivel_estatico_fecha = get_value_and_date(nivel_estatico_tag)
    sumergencia, sumergencia_fecha = get_value_and_date(sumergencia_tag)
    nivel_tanque, nivel_tanque_fecha = get_value_and_date(nivel_tanque_tag)

    # 3. Voltajes L1-L2, L2-L3, L1-L3 (Instantánea)
    nombres_voltajes = info.get("voltajes_l") or []
    voltajes_resultados = [get_value_and_date(nombre_volt) for nombre_volt in nombres_voltajes]

    # 4. Obtener datos históricos
    tags_volt_list = info.get("voltajes_l", [])
    tags_corr_list = info.get("corrientes_l", [])
    
    # 🟢 MODIFICACIÓN: Se añade nivel_estatico_tag a la función de históricos
    # para que la gráfica pueda dibujar la línea amarilla.
    datos_historicos = obtener_datos_historicos_multiple(
        caudal_tag, 
        presion_tag, 
        nivel_estatico_tag, # <--- IMPORTANTE: Agregado aquí
        sumergencia_tag, # <--- IMPORTANTE: Agregado aquí
        tags_volt_list, 
        tags_corr_list
    )
    
    datos = {
        "encendido": encendido,
        "corriente_total": corriente_total,
        "corrientes_resultados": corrientes_resultados,
        "caudal": caudal,
        "caudal_fecha": caudal_fecha,
        "presion": presion,
        "presion_fecha": presion_fecha,
        "voltajes_resultados": voltajes_resultados, 
        "nivel_estatico": nivel_estatico, # Ya garantizado como float (0.0 si es None)
        "nivel_estatico_fecha": nivel_estatico_fecha,
        "sumergencia": sumergencia,
        "sumergencia_fecha": sumergencia_fecha,
        "nivel_tanque": nivel_tanque,
        "nivel_tanque_fecha": nivel_tanque_fecha,
        "datos_historicos": datos_historicos, 
    }
    
    return pozo, datos

# ======================================================================FUNCIÓN TRABAJADORA PARA UN TANQUE (ACTUALIZADA) ========================================================================
def fetch_tank_data(tanque, info):
    """
    Función que será ejecutada por cada hilo para obtener los datos de UN SOLO tanque, incluyendo históricos.
    """
    def get_value_and_date(nombre_variable):
        """Llama a obtener_variables y asegura el retorno de (valor, fecha_hora) o (0.0, None)."""
        if not nombre_variable or nombre_variable == '0':
            return 0.0, None 
        
        resultado = obtener_variables(nombre_variable) 
        
        if isinstance(resultado, tuple) and len(resultado) == 2:
            valor, fecha = resultado
            return (float(valor) if valor is not None else 0.0), fecha
        
        return 0.0, None
    
    nivel_tag = info.get("nivel_tag")
    nivel_max = info.get("nivel_max", 1.0) 
    
    nivel_actual, nivel_fecha = get_value_and_date(nivel_tag)
    
    # Calcular porcentaje
    porcentaje = (nivel_actual / nivel_max) * 100 if nivel_max > 0 else 0.0
    
    # Obtener histórico de nivel
    datos_historicos = obtener_datos_historicos_tanque(nivel_tag)
    
    datos = {
        "nivel_actual": nivel_actual,
        "nivel_fecha": nivel_fecha,
        "nivel_max": nivel_max,
        "porcentaje": porcentaje,
        "datos_historicos": datos_historicos # <--- AGREGADO
    }
    return tanque, datos

# ======================================================================FUNCION PRINCIPAL DE MOSTRAR MAPA (CON CAMBIOS EN MACROMEDIDORES) =============================================================================================
def mostrar_mapa_aguascalientes():
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
        
# =================================================================
# 5. LÓGICA DE INICIO Y HILOS (sin cambios)
# =================================================================

INTERVALO_EJECUCION_PYTHON = 350

def iniciar_actualizacion_continua():
    mapa_abierto = False 
    
    while True:
        try:
            filepath = mostrar_mapa_aguascalientes() 
            
            if filepath:
                if not mapa_abierto:
                    map_url = f"file://{filepath}"
                    webbrowser.open_new(map_url)
                    mapa_abierto = True
                    print("✅ Mapa abierto en el navegador. La página se refrescará automáticamente.")
                
                else:
                    print(f"Mapa regenerado con nuevos datos. Esperando {INTERVALO_EJECUCION_PYTHON} segundos.")

            time.sleep(INTERVALO_EJECUCION_PYTHON)
            
        except Exception as e:
            print(f"Error grave en el bucle principal: {e}. Reintentando en 10 segundos.")
            time.sleep(10)
    
# =============================================================================
# 1. CLASES AUXILIARES (LOADING Y CONSOLA)
# =============================================================================
class CircularProgress(tk.Canvas):
    """Crea un loading circular estilo HUD/Tecnológico"""
    def __init__(self, master, size=100, color="#00CED1"):
        super().__init__(master, width=size, height=size, bg="#0b1a29", highlightthickness=0)
        self.size = size
        self.color = color
        self.angle = 0
        self.running = False
        # Arco animado
        self.arc = self.create_arc(10, 10, size-10, size-10, outline=color, 
                                   width=5, style=tk.ARC, start=0, extent=60)
        # Círculo decorativo de fondo
        self.create_oval(10, 10, size-10, size-10, outline="#1a3a5a", width=2)

    def start(self):
        self.running = True
        self.rotate()

    def rotate(self):
        if self.running:
            self.angle = (self.angle + 10) % 360
            self.itemconfig(self.arc, start=self.angle)
            self.after(30, self.rotate)

    def stop(self):
        self.running = False

class ConsoleToTkinter:
    """Redirige el texto de print() al widget de ScrolledText de la interfaz"""
    def __init__(self, text_widget):
        self.text_widget = text_widget
    def write(self, string):
        self.text_widget.configure(state='normal')
        self.text_widget.insert(tk.END, string)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state='disabled')
    def flush(self):
        pass 

# =============================================================================
# 2. VARIABLES GLOBALES Y FUNCIONES DE CONTROL DE UI
# =============================================================================
loading_canvas = None
lbl_status = None
root_ref = None

def mostrar_loading(mensaje):
    if loading_canvas and lbl_status:
        lbl_status.config(text=mensaje)
        lbl_status.pack(pady=5)
        loading_canvas.pack(pady=10)
        loading_canvas.start()

def ocultar_loading():
    if loading_canvas:
        loading_canvas.stop()
        loading_canvas.pack_forget()
        lbl_status.pack_forget()

def ejecutar_proceso_con_loading():
    """Ejecuta la lógica del mapa y asegura apagar el loading al terminar"""
    try:
        # Llama a la función original de tu script
        iniciar_actualizacion_continua() 
    except Exception as e:
        print(f"❌ Error en el proceso: {e}")
    finally:
        # Volver al hilo principal para ocultar la UI
        if root_ref:
            root_ref.after(0, ocultar_loading)

def iniciar_hilo_mapa():
    global MAPA_EN_EJECUCION
    if not MAPA_EN_EJECUCION:
        MAPA_EN_EJECUCION = True
        mostrar_loading("⏳ Conectando con Telemetría...")
        # Iniciar hilo secundario para no congelar la ventana
        threading.Thread(target=ejecutar_proceso_con_loading, daemon=True).start()
        print("🚀 Monitoreo iniciado.")
    else:
        messagebox.showinfo("Aviso", "El monitoreo ya se encuentra activo.")

# =============================================================================
# 3. INTERFAZ PRINCIPAL
st.title("🛰️ Monitoreo de Pozos - Aguascalientes")

# --- FRAME SUPERIOR (CONTROLES) ---
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    if st.button("Iniciar Monitoreo de Mapa"):
        # Esto sustituye al 'CircularProgress' y al 'loading_canvas'
        with st.spinner("🔄 Procesando datos de ingeniería y generando mapa..."):
            try:
                # Ejecuta tu lógica pesada
                ruta_archivo = ejecutar_logica_mapa()
                
                # Leemos el archivo para crear el botón de apertura
                with open(ruta_archivo, "rb") as f:
                    html_bytes = f.read()
                    b64 = base64.b64encode(html_bytes).decode()
                
                st.success("✅ Mapa generado correctamente")
                
                # Link para abrir en pestaña nueva (target="_blank")
                # Esto es lo que pedías para que se abra el HTML
                href = f'''
                    <a href="data:text/html;base64,{b64}" target="_blank" style="text-decoration: none;">
                        <div style="text-align: center; padding: 15px; background-color: #00CED1; color: #0b1a29; font-weight: bold; border-radius: 5px;">
                            🚀 CLIC AQUÍ PARA ABRIR MAPA EN PESTAÑA NUEVA
                        </div>
                    </a>
                '''
                st.markdown(href, unsafe_allow_html=True)
                
            except Exception as e:
                st.error(f"Error: {str(e)}")

# --- FRAME INFERIOR (CONSOLA) ---
st.write("### 📜 Registro de Eventos (Consola)")
