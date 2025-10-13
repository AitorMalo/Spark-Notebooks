import numpy as np
import pandas as pd
import re 
from datetime import datetime

##############################################################################################################
## Funcion 1: FUNCIÓN PARA ORDENAR LAS COLUMNAS ADDRESS Y CONTACT
## ------------------------------------------------------------
##############################################################################################################

palabras_clave_address = ["_AdrTp", "_CareOf", "_Dept", "_SubDept", "_StrtNm", "_BldgNb", "_BldgNm", "_Flr", "_UnitNb", "_PstBx", "_Room", "_PstCd", "_TwnNm", "_TwnLctnNm", "_DstrctNm", "_CtrySubDvsn", "_Ctry"]
palabras_clave_contact = ["_NmPrfx","_Nm" ,"_PhneNb","_MobNb","_FaxNb","_URLAdr" ,"_EmailAdr","_EmailPurp","_JobTitl","_Rspnsblty","_Dept","_Othr","_PrefrdMtd"]

def ordenar_por_palabras_clave(lista, palabras_clave):
    resultado = []
    palabras_no_encontradas = []    
    for palabra in palabras_clave:
        for elemento in lista:
            if palabra in elemento:
                resultado.append(elemento)    
    for elemento in lista:
        if not any(palabra in elemento for palabra in palabras_clave):
            palabras_no_encontradas.append(elemento)    
    resultado.extend(palabras_no_encontradas)
    lista_sin_duplicados = []
    for palabra in resultado:
        if palabra not in lista_sin_duplicados:
            lista_sin_duplicados.append(palabra)
    return lista_sin_duplicados

##############################################################################################################
## Funcion 1: FUNCIÓN PARA CAMBIAR EL FORMATO DE LA HORA 
## ------------------------------------------------------------
##############################################################################################################

def convertir_formato(fecha_str):
    try:
        # Intenta convertir la fecha al formato deseado
        fecha_datetime = datetime.strptime(fecha_str, 'MX_%Y%m%d-%H%M%S%f')
        return fecha_datetime.strftime('%d-%b-%Y %H:%M:%S')
    except ValueError:
        # En caso de error, devuelve el valor original
        return fecha_str

##############################################################################################################
## Funcion 2: FUNCIÓN PARA JUNTAR EL APPHEADER CON EL DOCUMENT HACIENDOLOS UN ÚNICO MENSAJE
## ------------------------------------------------------------
## Se aplica en SWIFT_parsing.py 
##############################################################################################################

def rellenar_nan_con_prefijo(df):
    # Obtener el prefijo común
    df['Common_Prefix'] = df['Filename'].str.extract(r'(\w+)_\w+\.xml')[0]

    # Obtener un diccionario de relleno para cada columna
    fill_dict = df.groupby('Common_Prefix').first().to_dict(orient='index')

    # Rellenar NaN en cada columna usando el diccionario de relleno
    for col in df.columns:
        df[col] = df.apply(lambda row: fill_dict.get(row['Common_Prefix'], {}).get(col, np.nan) if pd.isna(row[col]) else row[col], axis=1)

    # Fusionar el contenido de 'Message_Content' por el prefijo común
    df['Message_Content'] = df.groupby('Common_Prefix')['Message_Content'].transform(lambda x: ' '.join(x.dropna()))

    # Eliminar duplicados basados en 'Common_Prefix'
    df = df.drop_duplicates(subset=['Common_Prefix'], keep='first')

    # Modificar 'Filename'
    df['Filename'] = df['Filename'].str.replace('_Document', '').str.replace('_AppHdr', '')

    # Eliminar la columna 'Common_Prefix' ya que no es necesaria
    df = df.drop('Common_Prefix', axis=1)

    print(df.shape)
    return df

##############################################################################################################
## Funcion 3: FUNCIÓN PARA RELLENAR LOS NAN POR OTRA COLUMNA DEL DATAFRAME SI ESTA COLUMNA EXISTE
## ------------------------------------------------------------
## Se aplica en la función cambiar_nombres_MX
##############################################################################################################

def fillna_if_exist(df, target_column, source_columns, default=None):
    for source_column in source_columns:
        try:
            df[target_column] = df[target_column].fillna(df[source_column])
        except KeyError:
            pass  # La columna de origen no existe, continuar con la siguiente

    if default is not None:
        df[target_column].fillna(default, inplace=True)

def cambiar_nombres_MX(df):
    # Copiar el DataFrame para evitar modificar el original
    nombres_nuevos = {
    'MsgDefIdr': 'Message_type',
    'CdtTrfTxInf_PmtId_EndToEndId': 'End2EndId',
    'CdtTrfTxInf_PmtId_TxId': 'Transaction_Reference',
    'CdtTrfTxInf_InstdAmt_Ccy':'Instructed_Currency',
    'CdtTrfTxInf_InstdAmt': 'Instructed_Amount',
    'CdtTrfTxInf_IntrBkSttlmDt':'Interbank_Settled_Date',
    'GrpHdr_SttlmInf_InstgRmbrsmntAgt_FinInstnId_BICFI': "Senders_Correspondent",
    'GrpHdr_SttlmInf_InstdRmbrsmntAgt_FinInstnId_BICFI':"Receivers_Correspondent",
    'CdtTrfTxInf_Cdtr_PstlAdr_Ctry':'Beneficiary_Address_Country'
    }
    df_copiado = df.copy()
    prefijos = ['FIToFIPmtCxlReq_','CretMmb_','GetAcct_','FICdtTrf_','RtrAcct_','GetTx_','RtrTx_','ModfyTx_','CclTx_','GetLmt_','RtrLmt_','ModfyLmt_','DelLmt_','GetMmb_','RtrMmb_','ModfyMmb_','GetCcyXchgRate_','RtrCcyXchgRate_','GetBizDayInf_','RtrBizDayInf_','GetGnlBizInf_','RtrGnlBizInf_','BckpPmt_','ModfyStgOrdr_','Rct_','GetRsvatn_','RtrRsvatn_','ModfyRsvatn_','DelRsvatn_','LqdtyCdtTrf_','LqdtyDbtTrf_','GetStgOrdr_','RtrStgOrdr_','DelStgOrdr_','CretLmt_','CretStgOrdr_','CretRsvatn_','CretMmb_','FIToFIPmtStsRpt_', 'FIToFICstmrDrctDbt_', 'PmtRtr_', 'FIToFIPmtRvsl_', 'FIToFICstmrCdtTrf_', 'FIToFIPmtStsReq_', 'FIDrctDbt_', 'FIToFIPmtStsReq_','RsltnOfInvstgtn_']
    for prefijo in prefijos:
        df_copiado.columns = df_copiado.columns.map(lambda x: x[len(prefijo):] if x.startswith(prefijo) else x)
    df_copiado = df_copiado.groupby(level=0, axis=1).first()


    df_copiado.rename(columns=nombres_nuevos, inplace=True)
    df_copiado = df_copiado.replace({None: np.nan})
    df_copiado = df_copiado.sort_index(axis=1)
    df_copiado['Direction']=df_copiado['Direction'].astype(str)

    for index, value in df_copiado['Direction'].items():
        # Verificar si el valor contiene " (to SWIFT)"
        if " (to SWIFT)" in value:
            # Realizar el reemplazo
            df_copiado.at[index, 'Direction'] = value.replace(" (to SWIFT)", "")
        elif " (from SWIFT)" in value:
            # Realizar el reemplazo
            df_copiado.at[index, 'Direction'] = value.replace(" (from SWIFT)", "")
        elif 'TransmissionReport_Message_SubFormat' in df_copiado :
            df_copiado.at[index, 'Direction'] = df_copiado.at[index, 'TransmissionReport_Message_SubFormat']
        else:
            df_copiado.at[index, 'Direction'] = np.nan

    mask = (df_copiado['Message_type'].str.startswith('pacs.009') & 
            ~df_copiado['Message_type'].str.endswith('COV') & 
            df_copiado.filter(like='CdtTrfTxInf_UndrlygCstmrCdtTrf').notnull().any(axis=1))
    df_copiado.loc[mask, 'Message_type'] = df_copiado.loc[mask, 'Message_type'] + 'COV'

    
    #df_copiado['Direction'] = df_copiado['Direction'].replace({'Outgoing': 'O', 'Incoming': 'I', 'Input': 'I', 'Output': 'O'})
    df_copiado['Direction'] = df_copiado['Direction'].replace({'Input': 'Outgoing', 'Output': 'Incoming'})
    
    df_copiado['Sender_InstructingAgent_Requestor'] = df_copiado['Fr_FIId_FinInstnId_BICFI'].str[:8]
    df_copiado['Receiver_InstructedAgent_Responder'] = df_copiado['To_FIId_FinInstnId_BICFI'].str[:8]

    df_copiado['OwnBIC8'] = df_copiado.apply(
        lambda row: row['Sender_InstructingAgent_Requestor'] if row['Direction'] == 'Outgoing' else row['Receiver_InstructedAgent_Responder'],
        axis=1
    )

    df_copiado['OwnBIC8'] = df_copiado['OwnBIC8'].astype(str)

    df_copiado['Own_Country'] = df_copiado['OwnBIC8'].str[4:6]

    df_copiado['Role'] = np.nan  # Inicializar con NaN
    df_copiado['Role'] = df_copiado['Role'].astype(object)  # Convertir a tipo object
    
    df_copiado['Ordering_Customer_Account'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_DbtrAcct_Id_IBAN' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Account", ['CdtTrfTxInf_DbtrAcct_Id_IBAN', 'CdtTrfTxInf_DbtrAcct_Id_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_IBAN','CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_Othr'])
    elif 'CdtTrfTxInf_DbtrAcct_Id_Othr_Id' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Account", ['CdtTrfTxInf_DbtrAcct_Id_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_IBAN','CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_Othr'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_IBAN' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Account", ['CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_IBAN','CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_Othr'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_Othr' in df_copiado.columns:
        df_copiado["Ordering_Customer_Account"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAcct_Id_Othr']
    
    df_copiado['Beneficiary_Id'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Cdtr_Id_OrgId_Othr_Id' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Id", ['CdtTrfTxInf_Cdtr_Id_OrgId_Othr_Id', 'CdtTrfTxInf_Cdtr_Id_PrvtId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_OrgId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_PrvtId_Othr_Id'])
    elif 'CdtTrfTxInf_Cdtr_Id_PrvtId_Othr_Id' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Id", ['CdtTrfTxInf_Cdtr_Id_PrvtId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_OrgId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_PrvtId_Othr_Id'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_OrgId_Othr_Id' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Id", ['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_OrgId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_PrvtId_Othr_Id'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_PrvtId_Othr_Id' in df_copiado.columns:
        df_copiado["Beneficiary_Id"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Id_PrvtId_Othr_Id']
    
    df_copiado['Beneficiary_Account'] = np.nan
    if 'CdtTrfTxInf_CdtrAcct_Id_IBAN' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Account", ['CdtTrfTxInf_CdtrAcct_Id_IBAN', 'CdtTrfTxInf_CdtrAcct_Id_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_IBAN','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_Othr'])
    elif 'CdtTrfTxInf_CdtrAcct_Id_Othr_Id' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Account", ['CdtTrfTxInf_CdtrAcct_Id_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_IBAN','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_Othr'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_IBAN' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Account", ['CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_IBAN','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_Othr'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_Othr' in df_copiado.columns:
        df_copiado["Beneficiary_Account"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAcct_Id_Othr']
    
    df_copiado['Ordering_Customer_Id'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Dbtr_Id_OrgId_Othr_Id' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Id", ['CdtTrfTxInf_Dbtr_Id_OrgId_Othr_Id', 'CdtTrfTxInf_Dbtr_Id_PrvtId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_OrgId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_PrvtId_Othr_Id'])
    elif 'CdtTrfTxInf_Dbtr_Id_PrvtId_Othr_Id' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Id", ['CdtTrfTxInf_Dbtr_Id_PrvtId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_OrgId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_PrvtId_Othr_Id'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_OrgId_Othr_Id' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Id", ['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_OrgId_Othr_Id','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_PrvtId_Othr_Id'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_PrvtId_Othr_Id' in df_copiado.columns:
        df_copiado["Ordering_Customer_Id"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Id_PrvtId_Othr_Id']
    
    df_copiado['Beneficiary_Name'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Cdtr_Nm' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Name", ['CdtTrfTxInf_Cdtr_Nm', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Nm'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Nm' in df_copiado.columns:
        df_copiado["Beneficiary_Name"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_Nm']
    
    df_copiado['Beneficiary_Residence_Country'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Cdtr_CtryOfRes' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Residence_Country", ['CdtTrfTxInf_Cdtr_CtryOfRes', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_CtryOfRes'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_CtryOfRes' in df_copiado.columns:
        df_copiado["Beneficiary_Residence_Country"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_CtryOfRes']
    
    df_copiado['Remittance_Information'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_RmtInf_Ustrd' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Remittance_Information", ['CdtTrfTxInf_RmtInf_Ustrd', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_RmtInf_Ustrd'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_RmtInf_Ustrd' in df_copiado.columns:
        df_copiado["Remittance_Information"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_RmtInf_Ustrd']
     
    df_copiado['Ordering_Customer_Name'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Dbtr_Nm' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Name", ['CdtTrfTxInf_Dbtr_Nm', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Nm'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Nm' in df_copiado.columns:
        df_copiado["Ordering_Customer_Name"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_Nm']
    
    df_copiado['Account_With_Institution_Name_and_Address'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_CdtrAgt_FinInstnId_Nm' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Account_With_Institution_Name_and_Address", ['CdtTrfTxInf_CdtrAgt_FinInstnId_Nm','TxInfAndSts_InstdAgt_FinInstnId_Nm','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_Nm'])
    elif 'TxInfAndSts_InstdAgt_FinInstnId_Nm' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Account_With_Institution_Name_and_Address", ['TxInfAndSts_InstdAgt_FinInstnId_Nm','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_Nm'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_Nm' in df_copiado.columns:
        df_copiado["Account_With_Institution_Name_and_Address"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_Nm']
    
    df_copiado['Ordering_Institution_Name'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAgt_FinInstnId_Nm' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Institution_Name", ['CdtTrfTxInf_UndrlygCstmrCdtTrf_DbtrAgt_FinInstnId_Nm', 'CdtTrfTxInf_DbtrAgt_FinInstnId_Nm','CdtTrfTxInf_Dbtr_FinInstnId_Nm'])
    elif 'CdtTrfTxInf_DbtrAgt_FinInstnId_Nm' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Institution_Name", ['CdtTrfTxInf_DbtrAgt_FinInstnId_Nm','CdtTrfTxInf_Dbtr_FinInstnId_Nm'])
    elif 'CdtTrfTxInf_Dbtr_FinInstnId_Nm' in df_copiado.columns:
        df_copiado["Ordering_Institution_Name"] = df_copiado['CdtTrfTxInf_Dbtr_FinInstnId_Nm']
    
    df_copiado['Ordering_Customer_Address_Country'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Dbtr_PstlAdr_Ctry' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Address_Country", ['CdtTrfTxInf_Dbtr_PstlAdr_Ctry', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Ctry'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Ctry' in df_copiado.columns:
        df_copiado["Ordering_Customer_Address_Country"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Ctry']
    
    df_copiado['Ordering_Customer_Residence_Country'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Dbtr_CtryOfRes' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Residence_Country", ['CdtTrfTxInf_Dbtr_CtryOfRes', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_CtryOfRes'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_CtryOfRes' in df_copiado.columns:
        df_copiado["Ordering_Customer_Residence_Country"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_CtryOfRes']

    df_copiado['Ordering_Institution'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_DbtrAgt_FinInstnId_BICFI' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Institution", ['CdtTrfTxInf_DbtrAgt_FinInstnId_BICFI', 'TxInf_RtrChain_DbtrAgt_FinInstnId_BICFI','TxInf_RtrChain_Dbtr_Agt_FinInstnId_BICFI','TxInf_RtrChain_Dbtr_Pty_Id_OrgId_AnyBIC','CdtTrfTxInf_Dbtr_FinInstnId_BICFI'])
    elif 'TxInf_RtrChain_DbtrAgt_FinInstnId_BICFI' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Institution", ['TxInf_RtrChain_DbtrAgt_FinInstnId_BICFI','TxInf_RtrChain_Dbtr_Agt_FinInstnId_BICFI','TxInf_RtrChain_Dbtr_Pty_Id_OrgId_AnyBIC','CdtTrfTxInf_Dbtr_FinInstnId_BICFI'])
    elif 'TxInf_RtrChain_Dbtr_Agt_FinInstnId_BICFI' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Institution", ['TxInf_RtrChain_Dbtr_Agt_FinInstnId_BICFI','TxInf_RtrChain_Dbtr_Pty_Id_OrgId_AnyBIC','CdtTrfTxInf_Dbtr_FinInstnId_BICFI'])
    elif 'TxInf_RtrChain_Dbtr_Pty_Id_OrgId_AnyBIC' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Institution", ['TxInf_RtrChain_Dbtr_Pty_Id_OrgId_AnyBIC','CdtTrfTxInf_Dbtr_FinInstnId_BICFI'])
    elif 'CdtTrfTxInf_Dbtr_FinInstnId_BICFI' in df_copiado.columns:
        df_copiado["Ordering_Institution"] = df_copiado['CdtTrfTxInf_Dbtr_FinInstnId_BICFI']
    
    df_copiado['Account_With_Institution'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_CdtrAgt_FinInstnId_BICFI' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Account_With_Institution", ['CdtTrfTxInf_CdtrAgt_FinInstnId_BICFI', 'TxInf_RtrChain_CdtrAgt_FinInstnId_BICFI','TxInf_RtrChain_Cdtr_Agt_FinInstnId_BICFI','TxInf_RtrChain_Cdtr_Pty_Id_OrgId_AnyBIC','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_BICFI','TxInfAndSts_InstdAgt_FinInstnId_BICFI'])
    elif 'TxInf_RtrChain_CdtrAgt_FinInstnId_BICFI' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Account_With_Institution", ['TxInf_RtrChain_CdtrAgt_FinInstnId_BICFI','TxInf_RtrChain_Cdtr_Agt_FinInstnId_BICFI','TxInf_RtrChain_Cdtr_Pty_Id_OrgId_AnyBIC','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_BICFI','TxInfAndSts_InstdAgt_FinInstnId_BICFI'])
    elif 'TxInf_RtrChain_Cdtr_Agt_FinInstnId_BICFI' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Account_With_Institution", ['TxInf_RtrChain_Cdtr_Agt_FinInstnId_BICFI','TxInf_RtrChain_Cdtr_Pty_Id_OrgId_AnyBIC','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_BICFI','TxInfAndSts_InstdAgt_FinInstnId_BICFI'])
    elif 'TxInf_RtrChain_Cdtr_Pty_Id_OrgId_AnyBIC' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Account_With_Institution", ['TxInf_RtrChain_Cdtr_Pty_Id_OrgId_AnyBIC','CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_BICFI','TxInfAndSts_InstdAgt_FinInstnId_BICFI'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_BICFI' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Account_With_Institution", ['CdtTrfTxInf_UndrlygCstmrCdtTrf_CdtrAgt_FinInstnId_BICFI','TxInfAndSts_InstdAgt_FinInstnId_BICFI'])
    elif 'TxInfAndSts_InstdAgt_FinInstnId_BICFI' in df_copiado.columns:
        df_copiado["Account_With_Institution"] = df_copiado['TxInfAndSts_InstdAgt_FinInstnId_BICFI']
    
    for index, row in df_copiado.iterrows():
        if pd.notna(row['OwnBIC8']):
            # Verificar si la columna 'Ordering_Institution' existe en el DataFrame
            if 'Ordering_Institution' in df_copiado.columns and pd.notna(row['Ordering_Institution']) and row['OwnBIC8'] in row['Ordering_Institution']:
                df_copiado.at[index, 'Role'] = "Debtor"
            # Verificar si la columna 'Account_With_Institution' existe en el DataFrame
            elif 'Account_With_Institution' in df_copiado.columns and pd.notna(row['Account_With_Institution']) and row['OwnBIC8'] in row['Account_With_Institution']:
                df_copiado.at[index, 'Role'] = "Creditor"
            else:
                df_copiado.at[index, 'Role'] = "Intermediary"
        else:
            df_copiado.at[index, 'Role'] = "Intermediary"
    
    df_copiado["Ordering_Customer_Address"] = np.nan  # Inicializar con NaN
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_Dbtr_PstlAdr') and col != 'CdtTrfTxInf_Dbtr_PstlAdr_AdrLine']
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_address)
        df_copiado['Ordering_Customer_Address_Combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr') and col != 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_AdrLine']
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_address)
        df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    if 'Ordering_Customer_Address_Combinada' in df_copiado.columns:
        df_copiado['Ordering_Customer_Address_Combinada'] = df_copiado['Ordering_Customer_Address_Combinada'].replace('', np.nan)
    if 'CdtTrfTxInf_Dbtr_PstlAdr_AdrLine' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Address", ['CdtTrfTxInf_Dbtr_PstlAdr_AdrLine', 'Ordering_Customer_Address_Combinada','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_AdrLine', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Combinada'])
    elif 'Ordering_Customer_Address_Combinada' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Address", ['Ordering_Customer_Address_Combinada','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_AdrLine','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Combinada'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_AdrLine' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Customer_Address", ['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_AdrLine','CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Combinada'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Combinada' in df_copiado.columns:
        df_copiado["Ordering_Customer_Address"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_PstlAdr_Combinada']
    
    df_copiado["Beneficiary_Address"] = np.nan  # Inicializar con NaN
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_Cdtr_PstlAdr') and col != 'CdtTrfTxInf_Cdtr_PstlAdr_AdrLine']
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_address)
        df_copiado['CdtTrfTxInf_Cdtr_PstlAdr_Combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr') and col != 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_AdrLine']
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_address)
        df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_Combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    if 'CdtTrfTxInf_Cdtr_PstlAdr_Combinada' in df_copiado.columns:
        df_copiado['CdtTrfTxInf_Cdtr_PstlAdr_Combinada'] = df_copiado['CdtTrfTxInf_Cdtr_PstlAdr_Combinada'].replace('', np.nan)
    if 'CdtTrfTxInf_Cdtr_PstlAdr_AdrLine' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Address", ['CdtTrfTxInf_Cdtr_PstlAdr_AdrLine', 'CdtTrfTxInf_Cdtr_PstlAdr_Combinada','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_AdrLine','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_Combinada'])
    elif 'CdtTrfTxInf_Cdtr_PstlAdr_Combinada' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Address", ['CdtTrfTxInf_Cdtr_PstlAdr_Combinada','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_AdrLine','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_Combinada'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_AdrLine' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Address", ['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_AdrLine','CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_Combinada'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_Combinada' in df_copiado.columns:
        df_copiado["Beneficiary_Address"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_PstlAdr_Combinada']
    
    df_copiado["Account_With_Institution_Address"] = np.nan  # Inicializar con NaN
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr') and col != 'CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine']
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_address)
        df_copiado['Account_With_Institution_Address_Combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    if 'CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Account_With_Institution_Address", ['CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine', 'Account_With_Institution_Address_Combinada'])
    elif 'Account_With_Institution_Address_Combinada' in df_copiado.columns:
        df_copiado["Account_With_Institution_Address"] = df_copiado['Account_With_Institution_Address_Combinada']
    
    df_copiado["Ordering_Institution_Address"] = np.nan
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_DbtrAgt_FinInstnId_PstlAdr') and col != 'CdtTrfTxInf_DbtrAgt_FinInstnId_PstlAdr_AdrLine']
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_address)
        df_copiado['Ordering_Institution_Address_combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    if 'CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Institution_Address", ['CdtTrfTxInf_DbtrAgt_FinInstnId_PstlAdr_AdrLine', 'Ordering_Institution_Address_combinada'])
    elif 'Ordering_Institution_Address_combinada' in df_copiado.columns:
        df_copiado["Ordering_Institution_Address"] = df_copiado['Ordering_Institution_Address_combinada']
        
    if 'Beneficiary_Address' in df_copiado.columns:
        if 'Beneficiary_Address_Country' not in df_copiado.columns:
            df_copiado['Beneficiary_Address_Country'] = None
        patron = r'.*\/([a-zA-Z]{2})\/.*'
        df_copiado['Beneficiary_Address_Country'] = df_copiado.apply(lambda row: re.search(patron, row['Beneficiary_Address']).group(1) if pd.isna(row['Beneficiary_Address_Country']) and pd.notna(row['Beneficiary_Address']) and re.match(patron, row['Beneficiary_Address']) else row['Beneficiary_Address_Country'], axis=1)
    
    if 'Ordering_Customer_Address' in df_copiado.columns:
        if 'Ordering_Customer_Address_Country' not in df_copiado.columns:
            df_copiado['Ordering_Customer_Address_Country'] = None
        patron = r'.*\/([a-zA-Z]{2})\/.*'
        df_copiado['Ordering_Customer_Address_Country'] = df_copiado.apply(lambda row: re.search(patron, row['Ordering_Customer_Address']).group(1) if pd.isna(row['Ordering_Customer_Address_Country']) and pd.notna(row['Ordering_Customer_Address']) and re.match(patron, row['Ordering_Customer_Address']) else row['Ordering_Customer_Address_Country'], axis=1)
    
    if 'Ordering_Customer_Account' in df_copiado.columns:
        if 'Ordering_Customer_Id'  in df_copiado.columns:
            df_copiado['Ordering_Customer_Id'] = df_copiado['Ordering_Customer_Id'].astype(str)
            condicion = df_copiado['Ordering_Customer_Id'].str.match('^[a-zA-Z]{2}\d+$', na=False) & pd.notna(df_copiado['Ordering_Customer_Id'])
            df_copiado.loc[condicion, 'Ordering_Customer_Account'] = df_copiado.loc[condicion, 'Ordering_Customer_Id']
            
    if 'Beneficiary_Account' in df_copiado.columns:
        if 'Beneficiary_Id'  in df_copiado.columns:
            df_copiado['Beneficiary_Id'] = df_copiado['Beneficiary_Id'].astype(str)
            condicion = df_copiado['Beneficiary_Id'].str.match('^[a-zA-Z]{2}\d+$', na=False) & pd.notna(df_copiado['Beneficiary_Id'])
            df_copiado.loc[condicion, 'Beneficiary_Account'] = df_copiado.loc[condicion, 'Beneficiary_Id']
    
    df_copiado['UETR'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_PmtId_UETR' in df_copiado.columns:
        fillna_if_exist(df_copiado, "UETR", ['CdtTrfTxInf_PmtId_UETR','TxInf_OrgnlUETR','TxInfAndSts_OrgnlUETR','Undrlyg_TxInf_OrgnlUETR'])
    elif 'TxInf_OrgnlUETR' in df_copiado.columns:
        fillna_if_exist(df_copiado, "UETR", ['TxInf_OrgnlUETR','TxInfAndSts_OrgnlUETR','Undrlyg_TxInf_OrgnlUETR'])
    elif 'TxInfAndSts_OrgnlUETR' in df_copiado.columns:
        fillna_if_exist(df_copiado, "UETR", ['TxInfAndSts_OrgnlUETR', 'Undrlyg_TxInf_OrgnlUETR'])
    elif 'Undrlyg_TxInf_OrgnlUETR' in df_copiado.columns:
        df_copiado["UETR"] = df_copiado['Undrlyg_TxInf_OrgnlUETR']

    df_copiado['InstructionId'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_PmtId_InstrId' in df_copiado.columns:
        fillna_if_exist(df_copiado, "InstructionId", ['CdtTrfTxInf_PmtId_InstrId','Undrlyg_TxInf_CxlId','TxInf_RtrId','CxlDtls_TxInfAndSts_CxlStsId','GrpHdr_MsgId'])
    elif 'Undrlyg_TxInf_CxlId' in df_copiado.columns:
        fillna_if_exist(df_copiado, "InstructionId", ['Undrlyg_TxInf_CxlId','TxInf_RtrId','CxlDtls_TxInfAndSts_CxlStsId','GrpHdr_MsgId'])
    elif 'TxInf_RtrId' in df_copiado.columns:
        fillna_if_exist(df_copiado, "InstructionId", ['TxInf_RtrId','CxlDtls_TxInfAndSts_CxlStsId','GrpHdr_MsgId'])
    elif 'CxlDtls_TxInfAndSts_CxlStsId' in df_copiado.columns:
        fillna_if_exist(df_copiado, "InstructionId", ['CxlDtls_TxInfAndSts_CxlStsId','GrpHdr_MsgId'])
    elif 'GrpHdr_MsgId' in df_copiado.columns:
        df_copiado["InstructionId"] = df_copiado['GrpHdr_MsgId']
        
    df_copiado['Related_Message_Id'] = np.nan  # Inicializar con NaN
    if 'TxInf_OrgnlGrpInf_OrgnlMsgId' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Related_Message_Id", ['TxInf_OrgnlGrpInf_OrgnlMsgId','Undrlyg_TxInf_OrgnlGrpInf_OrgnlMsgId','TxInfAndSts_OrgnlGrpInf_OrgnlMsgId','CxlDtls_TxInfAndSts_OrgnlGrpInf_OrgnlMsgId','TxInf_OrgnlInstrId'])
    elif 'Undrlyg_TxInf_OrgnlGrpInf_OrgnlMsgId' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Related_Message_Id", ['Undrlyg_TxInf_OrgnlGrpInf_OrgnlMsgId','TxInfAndSts_OrgnlGrpInf_OrgnlMsgId','CxlDtls_TxInfAndSts_OrgnlGrpInf_OrgnlMsgId','TxInf_OrgnlInstrId'])
    elif 'TxInfAndSts_OrgnlGrpInf_OrgnlMsgId' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Related_Message_Id", ['TxInfAndSts_OrgnlGrpInf_OrgnlMsgId','CxlDtls_TxInfAndSts_OrgnlGrpInf_OrgnlMsgId','TxInf_OrgnlInstrId'])
    elif 'CxlDtls_TxInfAndSts_OrgnlGrpInf_OrgnlMsgId' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Related_Message_Id", ['CxlDtls_TxInfAndSts_OrgnlGrpInf_OrgnlMsgId','TxInf_OrgnlInstrId'])
    elif 'TxInf_OrgnlInstrId' in df_copiado.columns:
        df_copiado["Related_Message_Id"] = df_copiado['TxInf_OrgnlInstrId']
    
    df_copiado['Interbank_Settled_Amount'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_IntrBkSttlmAmt' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Interbank_Settled_Amount", ['CdtTrfTxInf_IntrBkSttlmAmt', 'TxInf_RtrdIntrBkSttlmAmt'])
    elif 'TxInf_RtrdIntrBkSttlmAmt' in df_copiado.columns:
        df_copiado["Interbank_Settled_Amount"] = df_copiado['TxInf_RtrdIntrBkSttlmAmt']
    
    if 'CxlDtls_TxInfAndSts_CxlStsRsnInf_Rsn_Cd' in df_copiado.columns:
        df_copiado["Narrative_CxlStsRsnInf"] = df_copiado['CxlDtls_TxInfAndSts_CxlStsRsnInf_Rsn_Cd']
    else:
        df_copiado["Narrative_CxlStsRsnInf"] = np.nan
    if 'CxlDtls_TxInfAndSts_CxlStsRsnInf_AddtlInf' in df_copiado.columns:
        df_copiado["Narrative_AddtlInf"] = df_copiado['CxlDtls_TxInfAndSts_CxlStsRsnInf_AddtlInf']
    else:
        df_copiado["Narrative_AddtlInf"] = np.nan
    df_copiado['Narrative_AddtlInf_CxlStsRsnInf_combinada'] = df_copiado['Narrative_CxlStsRsnInf'].fillna('')+' '+df_copiado['Narrative_AddtlInf'].fillna('')
    df_copiado['Narrative_AddtlInf_CxlStsRsnInf_combinada'] = df_copiado['Narrative_AddtlInf_CxlStsRsnInf_combinada'].str.strip()
    df_copiado['Narrative_AddtlInf_CxlStsRsnInf_combinada'] = df_copiado['Narrative_AddtlInf_CxlStsRsnInf_combinada'].replace('', np.nan)


    df_copiado['Narrative'] = np.nan  # Inicializar con NaN
    if 'TxInf_RtrRsnInf_Rsn_Cd' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Narrative", ['TxInf_RtrRsnInf_Rsn_Cd','TxInfAndSts_TxSts','Narrative_AddtlInf_CxlStsRsnInf_combinada','Undrlyg_TxInf_CxlRsnInf_Rsn_Cd'])
    elif 'TxInfAndSts_TxSts' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Narrative", ['TxInfAndSts_TxSts','Narrative_AddtlInf_CxlStsRsnInf_combinada','Undrlyg_TxInf_CxlRsnInf_Rsn_Cd'])
    elif 'Narrative_AddtlInf_CxlStsRsnInf_combinada' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Narrative", ['Narrative_AddtlInf_CxlStsRsnInf_combinada','Undrlyg_TxInf_CxlRsnInf_Rsn_Cd'])
    elif 'Undrlyg_TxInf_CxlRsnInf_Rsn_Cd' in df_copiado.columns:
        df_copiado["Narrative"] = df_copiado['Undrlyg_TxInf_CxlRsnInf_Rsn_Cd']
    
    df_copiado['Interbank_Settled_Currency'] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_IntrBkSttlmAmt_Ccy' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Interbank_Settled_Currency", ['CdtTrfTxInf_IntrBkSttlmAmt_Ccy', 'TxInf_RtrdIntrBkSttlmAmt_Ccy'])
    elif 'TxInf_RtrdIntrBkSttlmAmt_Ccy' in df_copiado.columns:
        df_copiado["Interbank_Settled_Currency"] = df_copiado['TxInf_RtrdIntrBkSttlmAmt_Ccy']
    
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_Dbtr_CtctDtls')]
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_contact)
        df_copiado['CdtTrfTxInf_Dbtr_CtctDtls_combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_CtctDtls')]
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_contact)
        df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_CtctDtls_combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    df_copiado["Ordering_Customer_Id_Additional_Information"] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Dbtr_CtctDtls_combinada' in df_copiado.columns:
        df_copiado['CdtTrfTxInf_Dbtr_CtctDtls_combinada'] = df_copiado['CdtTrfTxInf_Dbtr_CtctDtls_combinada'].replace('', np.nan)
        fillna_if_exist(df_copiado, "Ordering_Customer_Id_Additional_Information", ['CdtTrfTxInf_Dbtr_CtctDtls_combinada', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_CtctDtls_combinada'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_CtctDtls_combinada' in df_copiado.columns:
        df_copiado["Ordering_Customer_Id_Additional_Information"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Dbtr_CtctDtls_combinada']
    
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_Cdtr_CtctDtls')]
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_contact)
        df_copiado['CdtTrfTxInf_Cdtr_CtctDtls_combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_CtctDtls')]
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_contact)
        df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_CtctDtls_combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    df_copiado["Beneficiary_Additional_Information"] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Cdtr_CtctDtls_combinada' in df_copiado.columns:
        df_copiado['CdtTrfTxInf_Cdtr_CtctDtls_combinada'] = df_copiado['CdtTrfTxInf_Cdtr_CtctDtls_combinada'].replace('', np.nan)
        fillna_if_exist(df_copiado, "Beneficiary_Additional_Information", ['CdtTrfTxInf_Cdtr_CtctDtls_combinada', 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_CtctDtls_combinada'])
    elif 'CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_CtctDtls_combinada' in df_copiado.columns:
        df_copiado["Beneficiary_Additional_Information"] = df_copiado['CdtTrfTxInf_UndrlygCstmrCdtTrf_Cdtr_CtctDtls_combinada']
    
    df_copiado["Beneficiary_Institution"] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_CdtrAgt_FinInstnId_BICFI' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Institution", ['CdtTrfTxInf_CdtrAgt_FinInstnId_BICFI', 'CdtTrfTxInf_Cdtr_FinInstnId_BICFI'])
    elif 'CdtTrfTxInf_Cdtr_FinInstnId_BICFI' in df_copiado.columns:
        df_copiado["Beneficiary_Institution"] = df_copiado['CdtTrfTxInf_Cdtr_FinInstnId_BICFI']
    
    df_copiado["Beneficiary_Institution_Name"] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_CdtrAgt_FinInstnId_Nm' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Institution_Name", ['CdtTrfTxInf_CdtrAgt_FinInstnId_Nm', 'CdtTrfTxInf_Cdtr_FinInstnId_Nm'])
    elif 'CdtTrfTxInf_Cdtr_FinInstnId_Nm' in df_copiado.columns:
        df_copiado["Beneficiary_Institution_Name"] = df_copiado['CdtTrfTxInf_Cdtr_FinInstnId_Nm']
    
    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_Cdtr_FinInstnId_PstlAdr') and col != 'CdtTrfTxInf_Cdtr_FinInstnId_PstlAdr_AdrLine']
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_address)
        df_copiado['CdtTrfTxInf_Cdtr_FinInstnId_PstlAdr_combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass

    try:
        columnas_a_combinar = [col for col in df_copiado.columns if col.startswith('CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr') and col != 'CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine']
        columnas_a_combinar = ordenar_por_palabras_clave(columnas_a_combinar, palabras_clave_address)
        df_copiado['CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_combinada'] = df_copiado[columnas_a_combinar].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    except:
        pass
    
    df_copiado["Beneficiary_Institution_Address"] = np.nan  # Inicializar con NaN
    if 'CdtTrfTxInf_Cdtr_FinInstnId_PstlAdr_AdrLine' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Institution_Address", ['CdtTrfTxInf_Cdtr_FinInstnId_PstlAdr_AdrLine', 'CdtTrfTxInf_Cdtr_FinInstnId_PstlAdr_combinada','CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine','CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_combinada'])
    elif 'CdtTrfTxInf_Cdtr_FinInstnId_PstlAdr_combinada' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Institution_Address", ['CdtTrfTxInf_Cdtr_FinInstnId_PstlAdr_combinada','CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine','CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_combinada'])
    elif 'CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Institution_Address", ['CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_AdrLine', 'CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_combinada'])
    elif 'CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_combinada' in df_copiado.columns:
        df_copiado["Beneficiary_Institution_Address"] = df_copiado['CdtTrfTxInf_CdtrAgt_FinInstnId_PstlAdr_combinada']
    
    df_copiado['Creation_date'] = df_copiado['Creation_date'].fillna(df_copiado['Filename'].apply(convertir_formato))
    
    df_copiado['Creation_date'] = df_copiado['Creation_date'].fillna(df_copiado['Filename'].apply(convertir_formato))
    mask = (df_copiado['Message_type'].str.startswith('pacs.009') & 
            ~df_copiado['Message_type'].str.endswith('COV'))
    df_copiado.loc[mask, 'Account_With_Institution'] = np.nan

    columnas_deseadas = ['Filename','Message_Date','Message_Content','Creation_date', 'Direction', 'Sender_InstructingAgent_Requestor', 'Receiver_InstructedAgent_Responder', 'OwnBIC8', 'Own_Country', 'Role', 'Format', 'Message_type', 'InstructionId', 'End2EndId', 'UETR', 'Related_Message_Id', 'Transaction_Reference','Interbank_Settled_Date','Interbank_Settled_Currency', 'Interbank_Settled_Amount', 'Instructed_Currency', 'Instructed_Amount', 'Ordering_Customer_Account', 'Ordering_Customer_Name','Ordering_Customer_Id', 'Ordering_Customer_Address','Ordering_Customer_Address_Country','Ordering_Customer_Residence_Country', 'Ordering_Institution','Ordering_Institution_Name','Ordering_Institution_Address', 'Ordering_Institution_Name_and_Address', "Senders_Correspondent", "Senders_Correspondent_Name_and_Address", "Receivers_Correspondent", "Receivers_Correspondent_Name_and_Address", 'Third_Reimbursement_Institution', 'Third_Reimbursement_Institution_Name_and_Address', 'Intermediary_Institution', 'Account_With_Institution','Account_With_Institution_Address', 'Account_With_Institution_Name_and_Address', 'Beneficiary_Account', 'Beneficiary_Name','Beneficiary_Id', 'Beneficiary_Address','Beneficiary_Address_Country','Beneficiary_Residence_Country', 'Remittance_Information', 'Sender_to_Receiver_Information','Ordering_Customer_Date_Of_Birth','Ordering_Customer_Place_Of_Birth','Ordering_Customer_ID_Number','Ordering_Customer_National_ID_Number','Ordering_Customer_Id_Additional_Information','Narrative','Beneficiary_Institution','Beneficiary_Institution_Name','Beneficiary_Institution_Address','Beneficiary_Additional_Information']


    columnas_existen = [col for col in columnas_deseadas if col in df_copiado.columns]

    df_filtrado = df_copiado[columnas_existen]
    
    columnas_a_excluir = [columna for columna in df_filtrado.columns if 'date' in columna.lower()]
    columnas_a_excluir.extend(['Message_Content','Receivers_Reference','Senders_Reference','UETR'])

    for columna in df_filtrado.columns:
        if columna not in columnas_a_excluir:
            df_filtrado[columna] = df_filtrado[columna].apply(lambda x: str(x).replace('3/', ' ').replace('2/', ' ').replace('/', ' '))



    return df_filtrado