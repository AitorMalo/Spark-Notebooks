# 1:45 H PARA 150.000 REGISTROS
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

import shutil  # local fallback copy

from concurrent.futures import ThreadPoolExecutor, as_completed  # OPTIMIZACIÓN


spark = SparkSession.builder.getOrCreate().newSession()
logger = SWIFTParserLogger.getSWIFTParserLogger('INFO')


def main(adls_input_base_dir: str, adls_output_base_dir: str, date: str, local_dir: str):
    dbutils = get_dbutils(spark)

    #NEW

    def copy_to_staging(local_path: str, staging_dir: str):
            """
            Copy local_path (absolute filesystem path) to staging_dir.
            - Tries dbutils.fs.cp using file: prefix.
            - If that fails, tries to map staging_dir to local fs (/dbfs/...) and uses shutil.copy2.
            - Non-fatal: logs errors but does not raise.
            """
            if not staging_dir:
                logger.debug("No staging_dir provided; skipping staging copy.")
                return

            try:
                local_path = str(local_path)
                base_name = os.path.basename(local_path)
                # Ensure target path in staging
                staging_dir = staging_dir.rstrip('/')
                target_staging_path = f"{staging_dir}/{base_name}"

                # Build a file: URI for dbutils if needed
                local_volume_path = local_path
                if not local_dir.startswith('file:') and not local_dir.startswith('/dbfs'):
                    local_volume_path = f"file:{local_path}"

                # Try dbutils.fs.cp first (works in Databricks for many schemes)
                try:
                    dbutils.fs.cp(local_volume_path, target_staging_path)
                    logger.info(f"Copied to staging with dbutils: {local_volume_path} -> {target_staging_path}")
                    return
                except Exception as e:
                    logger.warning(f"dbutils.fs.cp failed for {local_volume_path} -> {target_staging_path}: {e}")

                # Fallback: try local filesystem mapping (for /dbfs/ or dbfs:/ mapping)
                try:
                    if staging_dir.startswith('/dbfs/'):
                        staging_local_dir = staging_dir
                    elif staging_dir.startswith('dbfs:/'):
                        staging_local_dir = '/dbfs' + staging_dir[len('dbfs:'):]
                    else:
                        staging_local_dir = staging_dir

                    staging_local_path = os.path.join(staging_local_dir, base_name)
                    os.makedirs(os.path.dirname(staging_local_path), exist_ok=True)
                    shutil.copy2(local_dir, staging_local_path)
                    logger.info(f"Copied to staging by shutil: {local_dir} -> {staging_local_path}")
                    return
                except Exception as e:
                    logger.error(f"Fallback copy failed: {local_dir} -> {staging_dir}: {e}")

            except Exception as e:
                logger.error(f"Unexpected error in copy_to_staging for {local_dir} -> {staging_dir}: {e}")

    #NEW_END


    ALL_TAGS = {}
    resultados_MX = {}

    adls_input_dir =  f'{adls_input_base_dir}/{date}'
    adls_output_dir =  f'{adls_output_base_dir}/{date}'
    local_input_dir = f'{local_dir}/swift_parsing/{date}'
    volume_input_dir = f'file:{local_input_dir}'

    local_output_dir = f'{local_dir}/swift_parsing_output/{date}'

    # copiamos los ficheros a directorio volumen
    try:
        dbutils.fs.rm(volume_input_dir, True)
        dbutils.fs.mkdirs(volume_input_dir)
    except Exception as e:
        logger.error(f'Error al borrar directorio {volume_input_dir} {e}')
    dbutils.fs.cp(adls_input_dir, volume_input_dir, recurse=True)


    ficheros_de_datos = os.listdir(Path(local_input_dir))

    if len(ficheros_de_datos) == 0:
        logger.info(f'No hay ficheros de datos en la carpeta: {volume_input_dir}')
        return
    for fichero in ficheros_de_datos:
        csv_procesado = False
        # Lee el primer archivo CSV encontrado
        if fichero.endswith(".csv") and not csv_procesado:
            fechas = pd.read_csv(f'{local_input_dir}/{fichero}')
            # Muestra las primeras filas del DataFrame
            fechas = fechas.rename(columns={'Warehouse ID': 'Filename'})
            fechas = fechas.rename(columns={'Creation date': 'Creation_date'})
        # Procesar el archivo .unknown
        if fichero.endswith(".unknown"): separar_secciones(f'{local_input_dir}/{fichero}')

    # OPTIMIZACIÓN: paralelización del parsing (misma lógica que el bucle secuencial)
    def _process_one(filename):  # OPTIMIZACIÓN
        return ProcessMessage(local_input_dir, filename, date)

    files = os.listdir(Path(local_input_dir))  # OPTIMIZACIÓN

    files_to_process = [
        filename for filename in files
        if not (filename[0] == '.' or filename.endswith(".unknown") or filename.endswith(".csv"))
    ]  # OPTIMIZACIÓN

    max_workers = min(32, (os.cpu_count() or 8) * 2)  # OPTIMIZACIÓN

    # OPTIMIZACIÓN: BATCHING
    BATCH_SIZE = 10000  # OPTIMIZACIÓN: BATCHING

    # OPTIMIZACIÓN: BATCHING
    def chunked(lst, size):  # OPTIMIZACIÓN: BATCHING
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

    os.makedirs(local_output_dir, exist_ok=True)

    # OPTIMIZACIÓN: BATCHING
    for batch_id, batch_files_to_process in enumerate(chunked(files_to_process, BATCH_SIZE)):  # OPTIMIZACIÓN: BATCHING

        with ThreadPoolExecutor(max_workers=max_workers) as executor:  # OPTIMIZACIÓN
            futures = [executor.submit(_process_one, fn) for fn in batch_files_to_process]  # OPTIMIZACIÓN

            for fut in as_completed(futures):  # OPTIMIZACIÓN
                # MISMO comportamiento que antes: si falla, peta aquí
                tags, resultados = fut.result()
                ALL_TAGS.update(tags)
                resultados_MX.update(resultados)

        os.makedirs(local_output_dir, exist_ok=True)
        ruta_archivo_parquet_MT = local_output_dir + '/Payments' + '_' + datetime.now().strftime('%Y%m%d%H%M%S') + f'_batch_{batch_id}' + '.parquet'  # OPTIMIZACIÓN: BATCHING
        ruta_archivo_parquet_MX = local_output_dir + '/Trade' + '_' + datetime.now().strftime('%Y%m%d%H%M%S') + f'_batch_{batch_id}' + '.parquet'  # OPTIMIZACIÓN: BATCHING
        ruta_archivo_parquet_completo = local_input_dir + '/All_messages' + '_' + datetime.now().strftime('%Y%m%d%H%M%S') + f'_batch_{batch_id}' + '.parquet'  # OPTIMIZACIÓN: BATCHING

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
            df_ALL_TAGS = separar_mensajes_101(df_ALL_TAGS)
            df_ALL_TAGS = funciones_complejas(df_ALL_TAGS)
            df_ALL_TAGS = df_ALL_TAGS.replace('\n', ' ', regex=True)
            df_ALL_TAGS['Filename'] = df_ALL_TAGS['Filename'].str.replace('.fin', '')
            fechas_filtradas = fechas[['Filename', 'Creation_date']]
            df_merged = pd.merge(df_ALL_TAGS, fechas_filtradas, on='Filename')
            df_ALL_TAGS_final = cambiar_nombres_direccion(df_merged)
            # df_ALL_TAGS_final.to_parquet(ruta_archivo_parquet_MT,index=False)
            # df_ALL_TAGS_final.to_csv('nombre_del_archivo.csv',index=False, sep=';')

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
            # df_resultados_MX = New_MX_Names.rellenar_nan_con_prefijo(df_resultados_MX)
            df_resultados_MX = rellenar_nan_con_prefijo(df_resultados_MX)
            df_resultados_MX = df_resultados_MX.replace('\n', ' ', regex=True)
            df_resultados_MX['Filename'] = df_resultados_MX['Filename'].str.replace('.xml', '')
            df_merged = pd.merge(df_resultados_MX, fechas, on='Filename', how='left')
            # df_resultados_MX_final = New_MX_Names.cambiar_nombres_MX(df_merged)
            df_resultados_MX_final = cambiar_nombres_MX(df_merged)
            # df_resultados_MX_final.to_parquet(ruta_archivo_parquet_MX,index=False)
            # df_resultados_MX_final.to_csv('prueba.csv',index=False,sep=';')

        if 'df_resultados_MX_final' in locals() and 'df_ALL_TAGS_final' in locals():
            columnas_comunes = df_resultados_MX_final.columns.intersection(df_ALL_TAGS_final.columns).tolist()
            df_juntos = pd.merge(df_resultados_MX_final, df_ALL_TAGS_final, on=columnas_comunes, how='outer')
            representaciones_nulas_1 = ['nan', 'NaN', 'None', ':None', '<NA>', '']
            df_juntos = df_juntos.astype(str)
            df_juntos.replace(representaciones_nulas_1, np.nan, inplace=True)
            df_juntos = df_juntos.replace({'': np.nan, ' ': np.nan})
            df_juntos.to_parquet(ruta_archivo_parquet_completo, index=False)
            # payments = df_juntos[df_juntos['Message_type'].str.startswith(('pacs','camt','1', '2'))]
            payments = df_juntos[
                df_juntos['Message_type'].isin(lista_payments_message_type) | df_juntos['Message_type'].str.startswith(
                    ('pacs', 'camt'))]
            if not payments.empty:
                # payments = payments.dropna(axis=1, how='all')
                payments_final = payments.reindex(columns=lista_payments)
                payments_final.to_parquet(ruta_archivo_parquet_MT, index=False)
                # payments_final.to_csv('payments_final.csv', index=False, sep=';')
                copy_to_staging(ruta_archivo_parquet_MT,adls_output_dir)

            # trade = df_juntos[df_juntos['Message_type'].str.startswith(('7', 'tsrv'))]
            trade = df_juntos[
                df_juntos['Message_type'].isin(lista_trade_message_type) | df_juntos['Message_type'].str.startswith('tsrv')]
            if not trade.empty:
                # trade = trade.dropna(axis=1, how='all')
                trade_final = trade.reindex(columns=lista_trade)
                trade_final.to_parquet(ruta_archivo_parquet_MX, index=False)
                # trade_final.to_csv('trade_final.csv', index=False, sep=';')
                copy_to_staging(ruta_archivo_parquet_MX,adls_output_dir)


        if 'df_ALL_TAGS_final' in locals() and isinstance(locals()['df_ALL_TAGS_final'],
                                                          pd.DataFrame) and 'df_resultados_MX_final' not in locals():
            df_ALL_TAGS_final.to_parquet(ruta_archivo_parquet_completo, index=False)
            representaciones_nulas = ['nan', 'NaN', 'None', ':None', '<NA>', '']
            df_ALL_TAGS_final = df_ALL_TAGS_final.astype(str)
            df_ALL_TAGS_final.replace(representaciones_nulas, np.nan, inplace=True)
            # payments = df_ALL_TAGS_final[df_ALL_TAGS_final['Message_type'].str.startswith(('1', '2'))]
            payments = df_ALL_TAGS_final[
                df_ALL_TAGS_final['Message_type'].isin(lista_payments_message_type) | df_ALL_TAGS_final[
                    'Message_type'].str.startswith(('pacs', 'camt'))]
            if not payments.empty:
                # payments = payments.dropna(axis=1, how='all')
                payments_final = payments.reindex(columns=lista_payments)
                payments_final.to_parquet(ruta_archivo_parquet_MT, index=False)
                copy_to_staging(ruta_archivo_parquet_MT,adls_output_dir)
            # trade = df_ALL_TAGS_final[df_ALL_TAGS_final['Message_type'].str.startswith(('7'))]
            trade = df_ALL_TAGS_final[df_ALL_TAGS_final['Message_type'].isin(lista_trade_message_type) | df_ALL_TAGS_final[
                'Message_type'].str.startswith('tsrv')]
            if not trade.empty:
                # trade = trade.dropna(axis=1, how='all')
                trade_final = trade.reindex(columns=lista_trade)
                trade_final.to_parquet(ruta_archivo_parquet_MX, index=False)
                copy_to_staging(ruta_archivo_parquet_MX,adls_output_dir)

        if 'df_resultados_MX_final' in locals() and isinstance(locals()['df_resultados_MX_final'],
                                                               pd.DataFrame) and 'df_ALL_TAGS_final' not in locals():
            df_resultados_MX_final.to_parquet(ruta_archivo_parquet_completo, index=False)
            representaciones_nulas = ['nan', 'NaN', 'None', ':None', '<NA>', '']
            df_resultados_MX_final = df_resultados_MX_final.astype(str)
            df_resultados_MX_final.replace(representaciones_nulas, np.nan, inplace=True)
            payments = df_resultados_MX_final[df_resultados_MX_final['Message_type'].str.startswith(('pacs', 'camt'))]
            if not payments.empty:
                # payments = payments.dropna(axis=1, how='all')
                payments_final = payments.reindex(columns=lista_payments)
                payments_final.to_parquet(ruta_archivo_parquet_MT, index=False)
                copy_to_staging(ruta_archivo_parquet_MT,adls_output_dir)
            trade = df_resultados_MX_final[df_resultados_MX_final['Message_type'].str.startswith(('tsrv'))]
            if not trade.empty:
                # trade = trade.dropna(axis=1, how='all')
                trade_final = trade.reindex(columns=lista_trade)
                trade_final.to_parquet(ruta_archivo_parquet_MX, index=False)
                copy_to_staging(ruta_archivo_parquet_MX,adls_output_dir)

        # OPTIMIZACIÓN: BATCHING (limpieza para no acumular en driver)
        ALL_TAGS.clear()  # OPTIMIZACIÓN: BATCHING
        resultados_MX.clear()  # OPTIMIZACIÓN: BATCHING

        # OPTIMIZACIÓN: BATCHING (evitar que el estado de locals() contamine el siguiente batch)
        if 'df_ALL_TAGS_final' in locals(): del df_ALL_TAGS_final  # OPTIMIZACIÓN: BATCHING
        if 'df_resultados_MX_final' in locals(): del df_resultados_MX_final  # OPTIMIZACIÓN: BATCHING
        if 'df_juntos' in locals(): del df_juntos  # OPTIMIZACIÓN: BATCHING
