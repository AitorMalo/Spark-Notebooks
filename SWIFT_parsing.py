# -*- coding: utf-8 -*-
"""
SWIFT_parsing.py (multi-archivo)
--------------------------------
Adaptado para procesar en UNA sola ejecución múltiples pares ZIP+CSV presentes en DAT_DIR.
- Empareja cada ZIP con su CSV por nombre base (sin extensión).
- Reinicia acumuladores por cada ZIP.
- Gestiona TEMP_DIR por ZIP (crea antes, borra después).
- Ejecuta la carga a HDFS por ZIP, usando el nombre del propio ZIP para resolver la carpeta YYYYMM.
El resto de la lógica de parseo/normalización permanece igual a la original.
"""

# LIBRERIAS
import os
import sys
import zipfile
import shutil
import glob
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

import initialFunctions
import SWIFTParserLogger
import MTFunctions
import New_MT_Names
import New_MX_Names
import General_properties
import HDFS_process
from General_properties import (
    lista_payments_message_type, lista_trade_message_type,
    lista_payments, lista_trade
)

##########################################################################################################
## STEP 1: Input parameters and environment variables
##########################################################################################################
# Setting log name
CURRENT_TIMESTAMP = datetime.now()
MASKED_TIMESTAMP = CURRENT_TIMESTAMP.strftime("%Y%m%d%H%M%S%f")
os.environ['LOG_NAME'] = f'SWIFT_parsing_{MASKED_TIMESTAMP}.log'

# Environment variables
DAT_DIR = os.environ.get('DAT_DIR')  # directorio de datos (dropzone)
LOG_DIR = os.environ.get('LOG_DIR')  # directorio de logs
TEMP_DIR = os.environ.get('TEMP_DIR')  # carpeta temporal de trabajo por ZIP
LOG_NAME = os.environ.get('LOG_NAME')
LOG_LEVEL = os.environ.get('LOG_LEVEL')
ANTIGUEDAD_LOGS = os.environ.get('LOG_DEPTH')

# Derivados
LOG_FILE = f'{LOG_DIR}/{LOG_NAME}'

# Descubrimiento de ficheros en dropzone
archivos_zip = sorted(glob.glob(f'{DAT_DIR}/*.zip'))
archivos_csv = sorted(glob.glob(f'{DAT_DIR}/*.csv'))

# Índice CSV por nombre base (sin extensión) para emparejar 1–1 con ZIP
csv_index = {os.path.splitext(os.path.basename(p))[0]: p for p in archivos_csv}

##########################################################################################################
## STEP 2: Log record
##########################################################################################################
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
PARSER_LOGGER = SWIFTParserLogger.getSWIFTParserLogger(LOG_FILE, LOG_LEVEL)
print(f'Log de ejecucion generado en: {LOG_FILE}')

##########################################################################################################
## STEP 3: Execution initial report
##########################################################################################################
PARSER_LOGGER.info(f'Nombre del fichero de log: {LOG_NAME}')
PARSER_LOGGER.info(f'Nivel de log: {LOG_LEVEL}')
PARSER_LOGGER.info(f'Antiguedad logs: {ANTIGUEDAD_LOGS} dias (ANTIGUEDAD_LOGS).')
PARSER_LOGGER.info(f'Directorio de datos: {DAT_DIR}')
PARSER_LOGGER.info(f'Directorio de log: {LOG_DIR}')
PARSER_LOGGER.info(f'Directorio temporal: {TEMP_DIR}')
PARSER_LOGGER.info(f'Columnas definidas en el data frame: Payments: {lista_payments}')
PARSER_LOGGER.info(f'Columnas definidas en el data frame: Trade: {lista_trade}')

PARSER_LOGGER.info(f'Se buscaran mensajes en ZIPs: {archivos_zip}')
PARSER_LOGGER.info(f'Se buscaran datos adicionales en CSVs: {archivos_csv}')

##########################################################################################################
## Sanity inicial: al menos 1 ZIP y 1 CSV
##########################################################################################################
if len(archivos_zip) == 0:
    PARSER_LOGGER.info(f'No hay ficheros ZIP en la carpeta: {DAT_DIR}')
    PARSER_LOGGER.handlers.clear()
    sys.exit(0)

if len(archivos_csv) == 0:
    PARSER_LOGGER.info(f'No hay ficheros CSV en la carpeta: {DAT_DIR}')
    PARSER_LOGGER.handlers.clear()
    sys.exit(0)

##########################################################################################################
## STEP 4: Data parsing (multi-archivo)
##########################################################################################################
PARSER_LOGGER.info('Inicio de: STEP 4: Data parsing (multi-archivo)')

# Recorremos cada ZIP y emparejamos con su CSV
for archivo_zip in archivos_zip:
    inicio_tiempo = time.time()

    zip_base = os.path.basename(archivo_zip)         # p.ej. "report_202501_1.zip"
    zip_key = os.path.splitext(zip_base)[0]          # p.ej. "report_202501_1"
    PARSER_LOGGER.info(f'Procesando ZIP: {zip_base}')

    # Emparejar CSV
    if zip_key not in csv_index:
        PARSER_LOGGER.error(f'No se encontro CSV emparejado para {zip_base}. Esperado: {zip_key}.csv')
        # Pasamos al siguiente ZIP (no abortamos toda la ejecucion)
        continue
    csv_path = csv_index[zip_key]
    csv_base = os.path.basename(csv_path)
    PARSER_LOGGER.info(f'CSV emparejado: {csv_base}')

    # Cargar CSV de fechas/metadatos
    try:
        fechas = pd.read_csv(csv_path)
        if 'Warehouse ID' in fechas.columns:
            fechas = fechas.rename(columns={'Warehouse ID': 'Filename'})
        if 'Creation date' in fechas.columns:
            fechas = fechas.rename(columns={'Creation date': 'Creation_date'})
    except Exception as e:
        PARSER_LOGGER.error(f'Error leyendo CSV {csv_base}: {e}')
        continue

    # Preparar TEMP_DIR por ZIP
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

    # Acumuladores por ZIP
    ALL_TAGS = {}
    resultados_MX = {}

    # Extraer y parsear contenidos del ZIP
    try:
        with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
            archivos_en_zip = zip_ref.namelist()
            # Extraer todo
            zip_ref.extractall(path=TEMP_DIR)

            # Pre-procesado para .unknown (MT)
            for filename in archivos_en_zip:
                if filename.endswith('.unknown'):
                    file_path = os.path.join(TEMP_DIR, filename)
                    initialFunctions.separar_secciones(file_path)

            # Parseo archivo a archivo dentro de TEMP_DIR
            for filename in os.listdir(TEMP_DIR):
                tags, resultados = initialFunctions.ProcessMessage(TEMP_DIR, filename, zip_base)
                ALL_TAGS.update(tags)
                resultados_MX.update(resultados)

    except Exception as e:
        PARSER_LOGGER.error(f'Error procesando ZIP {zip_base}: {e}')
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
        continue

    # Limpiar TEMP_DIR (se volvera a crear en el siguiente ZIP)
    shutil.rmtree(TEMP_DIR, ignore_errors=True)

    # Rutas de salida (incluyen YYYYMM derivado segun tu formato original)
    try:
        yyyy_mm_token = zip_key.split('_')[1]  # p.ej. "202501"
    except Exception:
        yyyy_mm_token = datetime.now().strftime('%Y%m')

    ts_suffix = datetime.now().strftime('%Y%m%d%H%M%S')
    ruta_parquet_mt = f'{yyyy_mm_token}_Payments_{ts_suffix}.parquet'
    ruta_parquet_mx = f'{yyyy_mm_token}_Trade_{ts_suffix}.parquet'
    ruta_parquet_all = f'{yyyy_mm_token}_All_messages_{ts_suffix}.parquet'

    # MT → DataFrame
    if ALL_TAGS:
        data_list_MT = []
        for key, value in ALL_TAGS.items():
            row = {"Filename": key}
            for subkey, subvalue in value.items():
                if isinstance(subvalue, dict):
                    for subsubkey, subsubvalue in subvalue.items():
                        new_key = f"{subkey}_{subsubkey}"
                        row[new_key] = subsubvalue
                else:
                    row[subkey] = subvalue
            data_list_MT.append(row)

        df_ALL_TAGS = pd.DataFrame(data_list_MT)
        df_ALL_TAGS = MTFunctions.separar_mensajes_101(df_ALL_TAGS)
        df_ALL_TAGS = New_MT_Names.funciones_complejas(df_ALL_TAGS)
        df_ALL_TAGS = df_ALL_TAGS.replace('\\n', ' ', regex=True)
        df_ALL_TAGS['Filename'] = df_ALL_TAGS['Filename'].str.replace('.fin', '', regex=False)

        # Merge con fechas del CSV emparejado
        if 'Filename' in fechas.columns and 'Creation_date' in fechas.columns:
            fechas_filtradas = fechas[['Filename', 'Creation_date']]
            df_merged_mt = pd.merge(df_ALL_TAGS, fechas_filtradas, on='Filename', how='inner')
        else:
            df_merged_mt = df_ALL_TAGS.copy()

        df_ALL_TAGS_final = New_MT_Names.cambiar_nombres_direccion(df_merged_mt)

    # MX → DataFrame
    if resultados_MX:
        data_list_MX = []
        for key, value in resultados_MX.items():
            row = {"Filename": key}
            for subkey, subvalue in value.items():
                if isinstance(subvalue, dict):
                    for subsubkey, subsubvalue in subvalue.items():
                        new_key = f"{subkey}_{subsubkey}"
                        row[new_key] = subsubvalue
                else:
                    row[subkey] = subvalue
            data_list_MX.append(row)

        df_resultados_MX = pd.DataFrame(data_list_MX)
        df_resultados_MX = New_MX_Names.rellenar_nan_con_prefijo(df_resultados_MX)
        df_resultados_MX = df_resultados_MX.replace('\\n', ' ', regex=True)
        df_resultados_MX['Filename'] = df_resultados_MX['Filename'].str.replace('.xml', '', regex=False)

        # Merge con CSV emparejado
        df_merged_mx = pd.merge(df_resultados_MX, fechas, on='Filename', how='left')
        df_resultados_MX_final = New_MX_Names.cambiar_nombres_MX(df_merged_mx)

    # Escrituras locales por ZIP (All / Payments / Trade)
    try:
        if 'df_ALL_TAGS_final' in locals() and 'df_resultados_MX_final' in locals():
            columnas_comunes = df_resultados_MX_final.columns.intersection(df_ALL_TAGS_final.columns).tolist()
            df_juntos = pd.merge(df_resultados_MX_final, df_ALL_TAGS_final, on=columnas_comunes, how='outer')

            representaciones_nulas_1 = ['nan', 'NaN', 'None', ':None', '<NA>', '']
            df_juntos = df_juntos.astype(str)
            df_juntos.replace(representaciones_nulas_1, np.nan, inplace=True)
            df_juntos = df_juntos.replace({'': np.nan, ' ': np.nan})
            df_juntos.to_parquet(ruta_parquet_all, index=False)

            # Payments
            payments_mask = (
                df_juntos['Message_type'].isin(lista_payments_message_type) |
                df_juntos['Message_type'].str.startswith(('pacs', 'camt'), na=False)
            )
            payments = df_juntos[payments_mask]
            if not payments.empty:
                payments_final = payments.reindex(columns=lista_payments)
                payments_final.to_parquet(ruta_parquet_mt, index=False)
                payments_csv_name = f'payments_final_{zip_key}.csv'
                payments_final.to_csv(payments_csv_name, index=False, sep=';')

            # Trade
            trade_mask = (
                df_juntos['Message_type'].isin(lista_trade_message_type) |
                df_juntos['Message_type'].str.startswith('tsrv', na=False)
            )
            trade = df_juntos[trade_mask]
            if not trade.empty:
                trade_final = trade.reindex(columns=lista_trade)
                trade_final.to_parquet(ruta_parquet_mx, index=False)
                trade_csv_name = f'trade_final_{zip_key}.csv'
                trade_final.to_csv(trade_csv_name, index=False, sep=';')

        elif 'df_ALL_TAGS_final' in locals() and isinstance(locals()['df_ALL_TAGS_final'], pd.DataFrame) and 'df_resultados_MX_final' not in locals():
            df_ALL_TAGS_final.to_parquet(ruta_parquet_all, index=False)
            representaciones_nulas = ['nan', 'NaN', 'None', ':None', '<NA>', '']
            df_ALL_TAGS_final = df_ALL_TAGS_final.astype(str)
            df_ALL_TAGS_final.replace(representaciones_nulas, np.nan, inplace=True)

            payments_mask = (
                df_ALL_TAGS_final['Message_type'].isin(lista_payments_message_type) |
                df_ALL_TAGS_final['Message_type'].str.startswith(('pacs', 'camt'), na=False)
            )
            payments = df_ALL_TAGS_final[payments_mask]
            if not payments.empty:
                payments_final = payments.reindex(columns=lista_payments)
                payments_final.to_parquet(ruta_parquet_mt, index=False)

            trade_mask = (
                df_ALL_TAGS_final['Message_type'].isin(lista_trade_message_type) |
                df_ALL_TAGS_final['Message_type'].str.startswith('tsrv', na=False)
            )
            trade = df_ALL_TAGS_final[trade_mask]
            if not trade.empty:
                trade_final = trade.reindex(columns=lista_trade)
                trade_final.to_parquet(ruta_parquet_mx, index=False)

        elif 'df_resultados_MX_final' in locals() and isinstance(locals()['df_resultados_MX_final'], pd.DataFrame) and 'df_ALL_TAGS_final' not in locals():
            df_resultados_MX_final.to_parquet(ruta_parquet_all, index=False)
            representaciones_nulas = ['nan', 'NaN', 'None', ':None', '<NA>', '']
            df_resultados_MX_final = df_resultados_MX_final.astype(str)
            df_resultados_MX_final.replace(representaciones_nulas, np.nan, inplace=True)

            payments = df_resultados_MX_final[df_resultados_MX_final['Message_type'].str.startswith(('pacs', 'camt'), na=False)]
            if not payments.empty:
                payments_final = payments.reindex(columns=lista_payments)
                payments_final.to_parquet(ruta_parquet_mt, index=False)

            trade = df_resultados_MX_final[df_resultados_MX_final['Message_type'].str.startswith('tsrv', na=False)]
            if not trade.empty:
                trade_final = trade.reindex(columns=lista_trade)
                trade_final.to_parquet(ruta_parquet_mx, index=False)

        # Tiempo de proceso por ZIP
        fin_tiempo = time.time()
        minutos = (fin_tiempo - inicio_tiempo) / 60.0
        PARSER_LOGGER.info(f'ZIP {zip_base}: DataFrames creados en {minutos:.2f} minutos.')

    except Exception as e:
        PARSER_LOGGER.error(f'Error construyendo/escribiendo DataFrames para {zip_base}: {e}')

    ######################################################################################################
    ## STEP 6: HDFS loading (por ZIP)
    ######################################################################################################
    try:
        PARSER_LOGGER.info('Inicio de: STEP 6: HDFS loading (por ZIP)')
        # kinit y gestión de carpeta en función del ZIP actual
        HDFS_process.ejecutar_proceso_kinit()
        HDFS_process.gestionar_directorio_hdfs(zip_base)
        folders = HDFS_process.list_hdfs_folders()
        HDFS_process.borrar_carpetas_antiguas(folders)
        PARSER_LOGGER.info('Fin de: STEP 6: HDFS loading (por ZIP)')
    except Exception as e:
        PARSER_LOGGER.error(f'Error en HDFS loading para {zip_base}: {e}')

# Fin del loop de ZIPs
PARSER_LOGGER.info('Fin de: STEP 4: Data parsing (multi-archivo)')

##########################################################################################################
## STEP 7: Log maintenance
##########################################################################################################
PARSER_LOGGER.info('Inicio de: STEP 7: Log maintenance')
try:
    FECHA_UMBRAL = time.time() - (int(ANTIGUEDAD_LOGS) * 24 * 60 * 60)
    for f in os.listdir(Path(LOG_DIR)):
        path_to_log = os.path.join(Path(LOG_DIR), f)
        if os.path.isfile(path_to_log):
            last_update = os.path.getmtime(path_to_log)
            if last_update < FECHA_UMBRAL and f.endswith(".log"):
                PARSER_LOGGER.info(f'Se borrara el fichero: {path_to_log}')
                os.remove(path_to_log)
except Exception as e:
    PARSER_LOGGER.error(f'Error en mantenimiento de logs: {e}')

PARSER_LOGGER.info('Fin de: STEP 7: Log maintenance')
PARSER_LOGGER.info('Ejecucion finalizada con exito.')
PARSER_LOGGER.handlers.clear()
sys.exit(0)
