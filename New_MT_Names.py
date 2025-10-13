import pandas as pd
import numpy as np
import re 

##############################################################################################################
## Funcion 1: FUNCIÓN PARA RELLENAR LOS NAN POR OTRA COLUMNA DEL DATAFRAME SI ESTA COLUMNA EXISTE
## ------------------------------------------------------------
## Se aplica en la función funciones_complejas y cambiar_nombres_direccion
##############################################################################################################

def fillna_if_exist(df, target_column, source_columns, default=None):
    for source_column in source_columns:
        try:
            df[target_column] = df[target_column].fillna(df[source_column])
        except KeyError:
            pass  # La columna de origen no existe, continuar con la siguiente

    if default is not None:
        df[target_column].fillna(default, inplace=True)

##############################################################################################################
## Funcion 2: FUNCIÓN PARA RELLENAR LA COLUMNA NARRATIVE
## ------------------------------------------------------------
## La función extrae y devuelve una sección del texto a partir del primer marcador encontrado.
##############################################################################################################

def extract_copy_value(row):
    content = row['Message_Content']
    start_idx_11S = content.find(':11S:')
    start_idx_75 = content.find(':75:')
    start_idx_76 = content.find(':76:') 
    start_idx_79 = content.find(':79:')
    indices = [idx for idx in [start_idx_11S, start_idx_75, start_idx_76, start_idx_79] if idx != -1]
    if not indices:
        return None
    if indices:
        min_index = min(indices)
        return content[min_index:]

##############################################################################################################
## Funcion 3: FUNCIÓN PARA DIFERENCIAR LAS COLUMNAS LOCALES CON LAS NO LOCALES EN LOS MT 760
## ------------------------------------------------------------
##############################################################################################################

def combinar_columnas(row,df):
    mensaje = row['Message_Content']
    Message_type = row['SWIFT_Message_Type']
    if Message_type== '760':
        sequence_15C = mensaje.count(":15C:")
        ocurrencias_32 = mensaje.count(":50:")
        if sequence_15C==1:
            if ocurrencias_32 == 2:
                applicant = row['Applicant_50']
                applicant_1 = row['Applicant_50_1']
                resultado_applicant = f"local:{applicant_1}"
                columna_combinada = f"{resultado_applicant} // {applicant}"
            else:
                applicant = row['Applicant_50']
                resultado_applicant = f"local:{applicant}" 
                columna_combinada = f"{resultado_applicant}"
        else:
            if ocurrencias_32 == 1:
                applicant = row['Applicant_50']
                columna_combinada = f"//{applicant}"  
            else:
                columna_combinada= np.nan
        ocurrencias_59 = mensaje.count(":59:")
        if sequence_15C==1:
            if ocurrencias_59 == 2:
                Beneficiary_name = row['Beneficiary_59_Name_and_Address']
                Beneficiary_name_1 = row['Beneficiary_59_1_Name_and_Address']
                resultado_applicant = f"local:{Beneficiary_name_1}"
                Beneficiary_name_combinada = f"{resultado_applicant} // {Beneficiary_name}"
                if 'Beneficiary_59_Account' in df.columns and 'Beneficiary_59_1_Account' in df.columns:
                    Beneficiary_account = row['Beneficiary_59_Account']
                    Beneficiary_account_1 = row['Beneficiary_59_1_Account']
                    resultado_applicant = f"local:{Beneficiary_account_1}"
                    Beneficiary_account_combinada = f"{resultado_applicant} // {Beneficiary_account}"
                if 'Beneficiary_59_1_Account' in df.columns and 'Beneficiary_59_Account' not in df.columns:
                    Beneficiary_account_1 = row['Beneficiary_59_1_Account']
                    resultado_applicant = f"local:{Beneficiary_account_1}"
                    Beneficiary_account_combinada = f"{resultado_applicant}" 
                if 'Beneficiary_59_Account' in df.columns and 'Beneficiary_59_1_Account' not in df.columns:
                    Beneficiary_account = row['Beneficiary_59_Account']
                    resultado_applicant = f"//{Beneficiary_account}"
                    Beneficiary_account_combinada = f"{resultado_applicant}" 
                else:
                    Beneficiary_account_combinada = np.nan
            else:
                Beneficiary_name = row['Beneficiary_59_Name_and_Address']
                resultado_applicant = f"local:{Beneficiary_name}" 
                Beneficiary_name_combinada = f"{resultado_applicant}"       
                if 'Beneficiary_59_Account' in df.columns: 
                    Beneficiary_account = row['Beneficiary_59_Account']
                    resultado_applicant = f"local:{Beneficiary_account}" 
                    Beneficiary_account_combinada = f"{resultado_applicant}" 
                else:
                    Beneficiary_account_combinada= np.nan
        else:
            if ocurrencias_59 == 1:
                Beneficiary_name = row['Beneficiary_59_Name_and_Address']
                Beneficiary_name_combinada = f"//:{Beneficiary_name}" 
                if 'Beneficiary_59_Account' in df.columns:
                    Beneficiary_account = row['Beneficiary_59_Account']
                    Beneficiary_account_combinada = f"//:{Beneficiary_account}" 
                else:
                    Beneficiary_account_combinada= np.nan
            else:
                Beneficiary_account_combinada= np.nan
                Beneficiary_name_combinada= np.nan
        ocurrencias_45L = mensaje.count(":45L:")
        if sequence_15C==1:
            if ocurrencias_45L == 2:
                Transaction_Details = row['Underlying_Transaction_Details_45L']
                Transaction_Details_1 = row['Underlying_Transaction_Details_45L_1']
                resultado_applicant = f"local:{Transaction_Details_1}"
                Transaction_Details_combinada = f"{resultado_applicant} // {Transaction_Details}"
            else:
                Transaction_Details = row['Underlying_Transaction_Details_45L']
                resultado_applicant = f"local:{Transaction_Details}" 
                Transaction_Details_combinada = f"{resultado_applicant}"
        else:
            if ocurrencias_45L == 1:
                Transaction_Details = row['Underlying_Transaction_Details_45L']
                Transaction_Details_combinada = f"//{Transaction_Details}" 
            else:
                Transaction_Details_combinada= np.nan
    else:
        columna_combinada=np.nan
        Beneficiary_name_combinada=np.nan
        Beneficiary_account_combinada=np.nan
        Transaction_Details_combinada=np.nan
    return columna_combinada, Beneficiary_name_combinada, Beneficiary_account_combinada,Transaction_Details_combinada

def funciones_complejas(df):
    if 'Ordering_Customer_50F_Party_Identifier' in df.columns:
        df['Ordering_Customer_50F_Party_Identifier'] = df['Ordering_Customer_50F_Party_Identifier'].str.replace('/','')
        df['Ordering_Customer_50F_Party_Identifier_Account'] = df['Ordering_Customer_50F_Party_Identifier'].apply(lambda x: x if (isinstance(x, str) and x[:2].isalpha() and x[2:].isdigit() and len(x) >= 16) else None)
        df['Ordering_Customer_50F_Party_Identifier_Id'] = df['Ordering_Customer_50F_Party_Identifier'].mask(df['Ordering_Customer_50F_Party_Identifier_Account'].notna())

    lista_Ordering_Customer_50H_10_Account=[]
    lista_Ordering_Customer_50G_10_Account=[]
    for i in range(10,0,-1):
        lista_Ordering_Customer_50H_10_Account.append(f'Ordering_Customer_50H_{i}_Account')
        lista_Ordering_Customer_50G_10_Account.append(f'Ordering_Customer_50G_{i}_Account')
    lista_Ordering_Customer_50H_10_Account=lista_Ordering_Customer_50H_10_Account+lista_Ordering_Customer_50G_10_Account
    lista_Ordering_Customer_50H_10_Account.extend(['Ordering_Customer_50A_Account', 'Ordering_Customer_50K_Account','Ordering_Customer_50F_Party_Identifier_Account','Ordering_Customer_50H_Account','Ordering_Customer_50G_Account'])
    df['Ordering_Customer_Account'] = np.nan  # Inicializar con NaN
    if 'Ordering_Customer_50H_10_Account' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_Account',lista_Ordering_Customer_50H_10_Account)
    else:
        df['Ordering_Customer_50H_10_Account'] = np.nan 
        fillna_if_exist(df, 'Ordering_Customer_Account',lista_Ordering_Customer_50H_10_Account)

    df['Ordering_Customer_Id'] = np.nan  # Inicializar con NaN
    lista_Ordering_Customer_50G_Identifier_Code=[]
    for i in range(10,0,-1):
        lista_Ordering_Customer_50G_Identifier_Code.append(f'Ordering_Customer_50G_{i}_Identifier_Code')
    lista_Ordering_Customer_50G_Identifier_Code.extend(['Ordering_Customer_50A_Identifier_Code', 'Ordering_Customer_50F_Party_Identifier_Id','Ordering_Customer_50G_Identifier_Code'])
    if 'Ordering_Customer_50G_10_Identifier_Code' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_Id',lista_Ordering_Customer_50G_Identifier_Code)
    else:
        df['Ordering_Customer_50G_10_Identifier_Code'] = np.nan
        fillna_if_exist(df, 'Ordering_Customer_Id',lista_Ordering_Customer_50G_Identifier_Code)

    if 'Ordering_Customer_50F_Number_Name_and_Address' in df.columns:
        patron = r'^1/([^\n]+)'
        df['Ordering_Customer_50F_Number_Name_and_Address_Name'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(patron,flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_Address_Name'] = df['Ordering_Customer_50F_Number_Name_and_Address_Name'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_Address_Name'] = df['Ordering_Customer_50F_Number_Name_and_Address_Name'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_Address_Name'] = df['Ordering_Customer_50F_Number_Name_and_Address_Name'].replace('', np.nan)
        df['Ordering_Customer_50F_Number_Name_and_Address_Name']=df['Ordering_Customer_50F_Number_Name_and_Address_Name'].replace('nan', np.nan)
    for i in range(1,11):
        if f'Ordering_Customer_50F_{i}_Number_Name_and_Address' in df.columns:
            patron = r'^1/([^\n]+)'
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(patron,flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name'].replace('', np.nan)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name']=df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name'].replace('nan', np.nan)

    df['Ordering_Customer_Name'] = np.nan 
    lista_Ordering_Customer_50F_6_Number_Name_and_Address_Name=[] 
    Lista_Intructing_Party_50L=[] 
    for i in range(10, 0, -1):
            lista_Ordering_Customer_50F_6_Number_Name_and_Address_Name.append(f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Name')
            lista_Ordering_Customer_50F_6_Number_Name_and_Address_Name.append(f'Ordering_Customer_50H_{i}_Name_and_Address')
            Lista_Intructing_Party_50L.append(f'Intructing_Party_50L_{i}')
    lista_Ordering_Customer_50F_6_Number_Name_and_Address_Name=lista_Ordering_Customer_50F_6_Number_Name_and_Address_Name+Lista_Intructing_Party_50L
    lista_Ordering_Customer_50F_6_Number_Name_and_Address_Name.extend(['Ordering_Customer_50F_Number_Name_and_Address_Name','Ordering_Customer_50H_Name_and_Address','Ordering_Customer_50K_Name_and_Address','Intructing_Party_50L'])
    if 'Ordering_Customer_50F_10_Number_Name_and_Address_Name' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_Name', lista_Ordering_Customer_50F_6_Number_Name_and_Address_Name)
    else:
        df['Ordering_Customer_50F_10_Number_Name_and_Address_Name'] = np.nan 
        fillna_if_exist(df, 'Ordering_Customer_Name',lista_Ordering_Customer_50F_6_Number_Name_and_Address_Name)
    
    if 'Ordering_Customer_50F_Number_Name_and_Address' in df.columns:
        df['Ordering_Customer_50F_Number_Name_and_Address_Address'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(r'^2/([^\n]+)',flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address'] = df['Ordering_Customer_50F_Number_Name_and_Address_Address'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address'] = df['Ordering_Customer_50F_Number_Name_and_Address_Address'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address'] = df['Ordering_Customer_50F_Number_Name_and_Address_Address'].replace('', np.nan)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address'].replace('nan', np.nan, inplace=True)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(r'^3/([^\n]+)',flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'] = df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'] = df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'] = df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'].replace('', np.nan)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'].replace('nan', np.nan, inplace=True)
        df['Ordering_Customer_50F_Number_Name_and_Address_Address'] = df['Ordering_Customer_50F_Number_Name_and_Address_Address'].fillna(df['Ordering_Customer_50F_Number_Name_and_Address_Address_1'])
    for i in range(1,11):
        if f'Ordering_Customer_50F_{i}_Number_Name_and_Address' in df.columns:
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^2/([^\n]+)',flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'].replace('', np.nan)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^3/([^\n]+)',flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'].replace('', np.nan)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'].replace('nan', np.nan, inplace=True)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address'].fillna(df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address_1'])
    df['Ordering_Customer_Address'] = np.nan  # Inicializar con NaN
    lista_Ordering_Customer_50F_10_Number_Name_and_Address_Address=[]
    for i in range(10,0,-1):
        lista_Ordering_Customer_50F_10_Number_Name_and_Address_Address.append(f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Address')
    lista_Ordering_Customer_50F_10_Number_Name_and_Address_Address.extend(['Ordering_Customer_50F_Number_Name_and_Address_Address', 'Ordering_Customer_50K_Name_and_Address'])
    if 'Ordering_Customer_50F_10_Number_Name_and_Address_Address' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_Address',lista_Ordering_Customer_50F_10_Number_Name_and_Address_Address)
    else:
        df['Ordering_Customer_50F_10_Number_Name_and_Address_Address'] = np.nan
        fillna_if_exist(df, 'Ordering_Customer_Address',lista_Ordering_Customer_50F_10_Number_Name_and_Address_Address)
    
    if 'Ordering_Customer_50F_Number_Name_and_Address' in df.columns:
        df['Ordering_Customer_50F_Number_Name_and_Address_Country'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(r'^3/([^\n]+)',flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_Address_Country'] = df['Ordering_Customer_50F_Number_Name_and_Address_Country'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_Address_Country'] = df['Ordering_Customer_50F_Number_Name_and_Address_Country'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_Address_Country'] = df['Ordering_Customer_50F_Number_Name_and_Address_Country'].replace('', np.nan)
        df['Ordering_Customer_50F_Number_Name_and_Date_Of_Birth'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(r'^4/([^\n]+)',flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_Date_Of_Birth'] = df['Ordering_Customer_50F_Number_Name_and_Date_Of_Birth'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_Date_Of_Birth'] = df['Ordering_Customer_50F_Number_Name_and_Date_Of_Birth'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_Date_Of_Birth'] = df['Ordering_Customer_50F_Number_Name_and_Date_Of_Birth'].replace('', np.nan)
        df['Ordering_Customer_50F_Number_Name_and_Place_Of_Birth'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(r'^5/([^\n]+)',flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_Place_Of_Birth'] = df['Ordering_Customer_50F_Number_Name_and_Place_Of_Birth'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_Place_Of_Birth'] = df['Ordering_Customer_50F_Number_Name_and_Place_Of_Birth'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_Place_Of_Birth'] = df['Ordering_Customer_50F_Number_Name_and_Place_Of_Birth'].replace('', np.nan)
        df['Ordering_Customer_50F_Number_Name_and_ID_Number'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(r'^6/([^\n]+)',flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_ID_Number'] = df['Ordering_Customer_50F_Number_Name_and_ID_Number'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_ID_Number'] = df['Ordering_Customer_50F_Number_Name_and_ID_Number'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_ID_Number'] = df['Ordering_Customer_50F_Number_Name_and_ID_Number'].replace('', np.nan)
        df['Ordering_Customer_50F_Number_Name_and_National_ID_Number'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(r'^7/([^\n]+)',flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_National_ID_Number'] = df['Ordering_Customer_50F_Number_Name_and_National_ID_Number'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_National_ID_Number'] = df['Ordering_Customer_50F_Number_Name_and_National_ID_Number'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_National_ID_Number'] = df['Ordering_Customer_50F_Number_Name_and_National_ID_Number'].replace('', np.nan)
        df['Ordering_Customer_50F_Number_Name_and_Additional_Information'] = df['Ordering_Customer_50F_Number_Name_and_Address'].astype(str).str.findall(r'^8/([^\n]+)',flags=re.MULTILINE)
        df['Ordering_Customer_50F_Number_Name_and_Additional_Information'] = df['Ordering_Customer_50F_Number_Name_and_Additional_Information'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Ordering_Customer_50F_Number_Name_and_Additional_Information'] = df['Ordering_Customer_50F_Number_Name_and_Additional_Information'].astype(str)
        df['Ordering_Customer_50F_Number_Name_and_Additional_Information'] = df['Ordering_Customer_50F_Number_Name_and_Additional_Information'].replace('', np.nan)
    for i in range(1,11):
        if f'Ordering_Customer_50F_{i}_Number_Name_and_Address' in df.columns:
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Country'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^3/([^\n]+)',flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Country'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Country'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Country'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Country'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Country'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Country'].replace('', np.nan)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Date_Of_Birth'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^4/([^\n]+)',flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Date_Of_Birth'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Date_Of_Birth'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Date_Of_Birth'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Date_Of_Birth'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Date_Of_Birth'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Date_Of_Birth'].replace('', np.nan)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Place_Of_Birth'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^5/([^\n]+)',flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Place_Of_Birth'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Place_Of_Birth'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Place_Of_Birth'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Place_Of_Birth'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Place_Of_Birth'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Place_Of_Birth'].replace('', np.nan)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_ID_Number'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^6/([^\n]+)',flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_ID_Number'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_ID_Number'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_ID_Number'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_ID_Number'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_ID_Number'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_ID_Number'].replace('', np.nan)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_National_ID_Number'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^7/([^\n]+)',flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_National_ID_Number'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_National_ID_Number'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_National_ID_Number'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_National_ID_Number'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_National_ID_Number'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_National_ID_Number'].replace('', np.nan)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Additional_Information'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^8/([^\n]+)',flags=re.MULTILINE)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Additional_Information'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Additional_Information'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Additional_Information'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Additional_Information'].astype(str)
            df[f'Ordering_Customer_50F_{i}_Number_Name_and_Additional_Information'] = df[f'Ordering_Customer_50F_{i}_Number_Name_and_Additional_Information'].replace('', np.nan)
    lista_Ordering_Customer_50F_10_Number_Name_and_Address_Country=[]
    lista_Ordering_Customer_50F_10_Number_Name_and_Date_Of_Birth=[]
    lista_Ordering_Customer_50F_10_Number_Name_and_Place_Of_Birth=[]
    lista_Ordering_Customer_50F_10_Number_Name_and_ID_Number=[]
    lista_Ordering_Customer_50F_10_Number_Name_and_National_ID_Number=[]
    lista_Ordering_Customer_50F_10_Number_Name_and_Additional_Information=[]
    for i in range(10,0,-1):
        lista_Ordering_Customer_50F_10_Number_Name_and_Address_Country.append(f'Ordering_Customer_50F_{i}_Number_Name_and_Address_Country')
        lista_Ordering_Customer_50F_10_Number_Name_and_Date_Of_Birth.append(f'Ordering_Customer_50F_{i}_Number_Name_and_Date_Of_Birth')
        lista_Ordering_Customer_50F_10_Number_Name_and_Place_Of_Birth.append(f'Ordering_Customer_50F_{i}_Number_Name_and_Place_Of_Birth')
        lista_Ordering_Customer_50F_10_Number_Name_and_ID_Number.append(f'Ordering_Customer_50F_{i}_Number_Name_and_ID_Number')
        lista_Ordering_Customer_50F_10_Number_Name_and_National_ID_Number.append(f'Ordering_Customer_50F_{i}_Number_Name_and_National_ID_Number')
        lista_Ordering_Customer_50F_10_Number_Name_and_Additional_Information.append(f'Ordering_Customer_50F_{i}_Number_Name_and_Additional_Information')
    lista_Ordering_Customer_50F_10_Number_Name_and_Address_Country.extend(['Ordering_Customer_50F_Number_Name_and_Country'])
    lista_Ordering_Customer_50F_10_Number_Name_and_Date_Of_Birth.extend(['Ordering_Customer_50F_Number_Name_and_Date_Of_Birth'])
    lista_Ordering_Customer_50F_10_Number_Name_and_Place_Of_Birth.extend(['Ordering_Customer_50F_Number_Name_and_Place_Of_Birth'])
    lista_Ordering_Customer_50F_10_Number_Name_and_ID_Number.extend(['Ordering_Customer_50F_Number_Name_and_ID_Number'])
    lista_Ordering_Customer_50F_10_Number_Name_and_National_ID_Number.extend(['Ordering_Customer_50F_Number_Name_and_National_ID_Number'])
    lista_Ordering_Customer_50F_10_Number_Name_and_Additional_Information.extend(['Ordering_Customer_50F_Number_Name_and_Additional_Information'])
    df['Ordering_Customer_Address_Country'] = np.nan  # Inicializar con NaN
    if 'Ordering_Customer_50F_10_Number_Name_and_Address_Country' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_Address_Country',lista_Ordering_Customer_50F_10_Number_Name_and_Address_Country)
    else:
        df['Ordering_Customer_50F_10_Number_Name_and_Address_Country'] = np.nan
        fillna_if_exist(df, 'Ordering_Customer_Address_Country',lista_Ordering_Customer_50F_10_Number_Name_and_Address_Country)
    df['Ordering_Customer_Date_Of_Birth'] = np.nan  # Inicializar con NaN
    if 'Ordering_Customer_50F_10_Number_Name_and_Date_Of_Birth' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_Date_Of_Birth',lista_Ordering_Customer_50F_10_Number_Name_and_Date_Of_Birth)
    else:
        df['Ordering_Customer_50F_10_Number_Name_and_Date_Of_Birth'] = np.nan
        fillna_if_exist(df, 'Ordering_Customer_Date_Of_Birth',lista_Ordering_Customer_50F_10_Number_Name_and_Date_Of_Birth)
    df['Ordering_Customer_Place_Of_Birth'] = np.nan  # Inicializar con NaN
    if 'Ordering_Customer_50F_10_Number_Name_and_Place_Of_Birth' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_Place_Of_Birth',lista_Ordering_Customer_50F_10_Number_Name_and_Place_Of_Birth)
    else:
        df['Ordering_Customer_50F_10_Number_Name_and_Place_Of_Birth'] = np.nan
        fillna_if_exist(df, 'Ordering_Customer_Place_Of_Birth',lista_Ordering_Customer_50F_10_Number_Name_and_Place_Of_Birth)
    df['Ordering_Customer_ID_Number'] = np.nan  # Inicializar con NaN
    if 'Ordering_Customer_50F_10_Number_Name_and_ID_Number' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_ID_Number',lista_Ordering_Customer_50F_10_Number_Name_and_ID_Number)
    else:
        df['Ordering_Customer_50F_10_Number_Name_and_ID_Number'] = np.nan
        fillna_if_exist(df, 'Ordering_Customer_ID_Number',lista_Ordering_Customer_50F_10_Number_Name_and_ID_Number)
    df['Ordering_Customer_National_ID_Number'] = np.nan  # Inicializar con NaN
    if 'Ordering_Customer_50F_10_Number_Name_and_National_ID_Number' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_National_ID_Number',lista_Ordering_Customer_50F_10_Number_Name_and_National_ID_Number)
    else:
        df['Ordering_Customer_50F_10_Number_Name_and_National_ID_Number'] = np.nan
        fillna_if_exist(df, 'Ordering_Customer_National_ID_Number',lista_Ordering_Customer_50F_10_Number_Name_and_National_ID_Number)
    df['Ordering_Customer_Additional_Information'] = np.nan  # Inicializar con NaN
    if 'Ordering_Customer_50F_10_Number_Name_and_Additional_Information' in df.columns:
        fillna_if_exist(df, 'Ordering_Customer_Additional_Information',lista_Ordering_Customer_50F_10_Number_Name_and_Additional_Information)
    else:
        df['Ordering_Customer_50F_10_Number_Name_and_Additional_Information'] = np.nan
        fillna_if_exist(df, 'Ordering_Customer_Additional_Information',lista_Ordering_Customer_50F_10_Number_Name_and_Additional_Information)

    if 'Beneficiary_Customer_59F_Number_Name_and_Address' in df.columns:
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Country'] = df['Beneficiary_Customer_59F_Number_Name_and_Address'].astype(str).str.findall(r'^3/([^\n]+)',flags=re.MULTILINE).apply(lambda x: x[0] if len(x) > 0 else np.nan)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Country'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Country'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Country'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Country'].astype(str)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Country'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Country'].replace('', np.nan)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Country'].replace('nan', np.nan, inplace=True)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1'] = df['Beneficiary_Customer_59F_Number_Name_and_Address'].astype(str).str.findall(r'^1/([^\n]+)',flags=re.MULTILINE).apply(lambda x: x[0] if len(x) > 0 else np.nan)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1'].astype(str)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1'].replace('', np.nan)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1'].replace('nan', np.nan, inplace=True)
    if 'Beneficiary_59F_Number_Name_and_Address' in df.columns:
        df['Beneficiary_59F_Number_Name_and_Address_Country'] = df['Beneficiary_59F_Number_Name_and_Address'].astype(str).str.findall(r'^3/([^\n]+)',flags=re.MULTILINE).apply(lambda x: x[0] if len(x) > 0 else np.nan)
        df['Beneficiary_59F_Number_Name_and_Address_Country'] = df['Beneficiary_59F_Number_Name_and_Address_Country'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Beneficiary_59F_Number_Name_and_Address_Country'] = df['Beneficiary_59F_Number_Name_and_Address_Country'].astype(str)
        df['Beneficiary_59F_Number_Name_and_Address_Country'] = df['Beneficiary_59F_Number_Name_and_Address_Country'].replace('', np.nan)
        df['Beneficiary_59F_Number_Name_and_Address_Country'].replace('nan', np.nan, inplace=True)
        df['Beneficiary_59F_Number_Name_and_Address_Name_1'] = df['Beneficiary_59F_Number_Name_and_Address'].astype(str).str.findall(r'^1/([^\n]+)',flags=re.MULTILINE).apply(lambda x: x[0] if len(x) > 0 else np.nan)
        df['Beneficiary_59F_Number_Name_and_Address_Name_1'] = df['Beneficiary_59F_Number_Name_and_Address_Name_1'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Beneficiary_59F_Number_Name_and_Address_Name_1'] = df['Beneficiary_59F_Number_Name_and_Address_Name_1'].astype(str)
        df['Beneficiary_59F_Number_Name_and_Address_Name_1'] = df['Beneficiary_59F_Number_Name_and_Address_Name_1'].replace('', np.nan)
        df['Beneficiary_59F_Number_Name_and_Address_Name_1'].replace('nan', np.nan, inplace=True)
    if f'Beneficiary_59F_{i}_Number_Name_and_Address' in df.columns:
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Country'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^3/([^\n]+)',flags=re.MULTILINE).apply(lambda x: x[0] if len(x) > 0 else np.nan)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Country'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Country'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Country'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Country'].astype(str)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Country'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Country'].replace('', np.nan)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Country'].replace('nan', np.nan, inplace=True)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address'].astype(str).str.findall(r'^1/([^\n]+)',flags=re.MULTILINE).apply(lambda x: x[0] if len(x) > 0 else np.nan)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1'].astype(str)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1'].replace('', np.nan)
        df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1'].replace('nan', np.nan, inplace=True)
    lista_Beneficiary_59F_10_Number_Name_and_Address_Country=[]
    for i in range(10,0,-1):
        lista_Beneficiary_59F_10_Number_Name_and_Address_Country.append(f'lista_Beneficiary_59F_{i}_Number_Name_and_Address_Country')
    lista_Beneficiary_59F_10_Number_Name_and_Address_Country.extend(['Beneficiary_Customer_59F_Number_Name_and_Address_Country', 'Beneficiary_59F_Number_Name_and_Address_Country'])
    df["Beneficiary_Address_Country"] = np.nan  # Inicializar con NaN
    if 'Beneficiary_59F_10_Number_Name_and_Address_Country' in df.columns:
        fillna_if_exist(df, 'Beneficiary_Address_Country',lista_Beneficiary_59F_10_Number_Name_and_Address_Country) 
    else:
        df["Beneficiary_59F_10_Number_Name_and_Address_Country"] = np.nan
        fillna_if_exist(df, 'Beneficiary_Address_Country',lista_Beneficiary_59F_10_Number_Name_and_Address_Country)

    if 'Beneficiary_Customer_59F_Number_Name_and_Address' in df.columns:
        patron_beneficiary_address = r'^2/(.*)'
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'] = df['Beneficiary_Customer_59F_Number_Name_and_Address'].astype(str).apply(lambda x: re.findall(patron_beneficiary_address, x, re.DOTALL))
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        patron_beneficiary_address_1 = r'^3/(.*)'
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address_1'] = df['Beneficiary_Customer_59F_Number_Name_and_Address'].astype(str).apply(lambda x: re.findall(patron_beneficiary_address_1, x,re.DOTALL))
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address_1'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Address_1'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address_1'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Address_1'].astype(str)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address_1'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Address_1'].replace('', np.nan)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'].astype(str)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'].replace('', np.nan)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'].replace('nan', np.nan, inplace=True)
        df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'] = df['Beneficiary_Customer_59F_Number_Name_and_Address_Address'].fillna(df['Beneficiary_Customer_59F_Number_Name_and_Address_Address_1'])
    if 'Beneficiary_59F_Number_Name_and_Address' in df.columns:
        patron_beneficiary_address = r'^2/(.*)'
        df['Beneficiary_59F_Number_Name_and_Address_Address'] = df['Beneficiary_59F_Number_Name_and_Address'].astype(str).apply(lambda x: re.findall(patron_beneficiary_address, x, re.DOTALL))
        df['Beneficiary_59F_Number_Name_and_Address_Address'] = df['Beneficiary_59F_Number_Name_and_Address_Address'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        patron_beneficiary_address_1 = r'^3/(.*)'
        df['Beneficiary_59F_Number_Name_and_Address_Address_1'] = df['Beneficiary_59F_Number_Name_and_Address'].astype(str).apply(lambda x: re.findall(patron_beneficiary_address_1, x,re.DOTALL))
        df['Beneficiary_59F_Number_Name_and_Address_Address_1'] = df['Beneficiary_59F_Number_Name_and_Address_Address_1'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Beneficiary_59F_Number_Name_and_Address_Address_1'] = df['Beneficiary_59F_Number_Name_and_Address_Address_1'].astype(str)
        df['Beneficiary_59F_Number_Name_and_Address_Address_1'] = df['Beneficiary_59F_Number_Name_and_Address_Address_1'].replace('', np.nan)
        df['Beneficiary_59F_Number_Name_and_Address_Address'] = df['Beneficiary_59F_Number_Name_and_Address_Address'].astype(str)
        df['Beneficiary_59F_Number_Name_and_Address_Address'] = df['Beneficiary_59F_Number_Name_and_Address_Address'].replace('', np.nan)
        df['Beneficiary_59F_Number_Name_and_Address_Address'].replace('nan', np.nan, inplace=True)
        df['Beneficiary_59F_Number_Name_and_Address_Address'] = df['Beneficiary_59F_Number_Name_and_Address_Address'].fillna(df['Beneficiary_59F_Number_Name_and_Address_Address_1'])
    for i in range(1,11):
        if f'Beneficiary_59F_{i}_Number_Name_and_Address' in df.columns:
            patron_beneficiary_address = r'^2/(.*)'
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address'].astype(str).apply(lambda x: re.findall(patron_beneficiary_address, x, re.DOTALL))
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            patron_beneficiary_address_1 = r'^3/(.*)'
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address_1'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address'].astype(str).apply(lambda x: re.findall(patron_beneficiary_address_1, x,re.DOTALL))
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address_1'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address_1'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address_1'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address_1'].astype(str)
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address_1'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address_1'].replace('', np.nan)
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'].astype(str)
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'].replace('', np.nan)
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'].replace('nan', np.nan, inplace=True)
            df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'] = df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address'].fillna(df[f'Beneficiary_59F_{i}_Number_Name_and_Address_Address_1'])
    
    lista_Beneficiary_59F_10_Number_Name_and_Address_Address=[]
    for i in range(10,0,-1):
        lista_Beneficiary_59F_10_Number_Name_and_Address_Address.append(f'Beneficiary_59F_{i}_Number_Name_and_Address_Address')
    lista_Beneficiary_59F_10_Number_Name_and_Address_Address.extend(['Beneficiary_Customer_59_Name_and_Address', 'Beneficiary_Customer_59F_Number_Name_and_Address_Address','Beneficiary_59_Name_and_Address','Second_Beneficiary_59_Name_and_Address'])
    df["Beneficiary_Address"] = np.nan  # Inicializar con NaN
    if 'Beneficiary_59F_10_Number_Name_and_Address_Address' in df.columns:
        fillna_if_exist(df, 'Beneficiary_Address',lista_Beneficiary_59F_10_Number_Name_and_Address_Address) 
    else:
        df["Beneficiary_59F_10_Number_Name_and_Address_Address"] = np.nan
        fillna_if_exist(df, 'Beneficiary_Address',lista_Beneficiary_59F_10_Number_Name_and_Address_Address)  
        
    df['Applicant_50_combinada'] = df.apply(lambda row: combinar_columnas(row,df)[0], axis=1)
    if 'Applicant_50_combinada' in df.columns:
        patron_Applicant_50_combinada = r'local:(.*?)(?://|$)'
        df['Local_Applicant_Name'] = df['Applicant_50_combinada'].astype(str).apply(lambda x: re.findall(patron_Applicant_50_combinada, x, re.DOTALL))
        df['Local_Applicant_Name'] = df['Local_Applicant_Name'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        patron_Applicant_50_combinada_1 = r'//(.*)$'
        df['Applicant_Name_Applicant_50_combinada'] = df['Applicant_50_combinada'].astype(str).apply(lambda x: re.findall(patron_Applicant_50_combinada_1, x, re.DOTALL))
        df['Applicant_Name_Applicant_50_combinada'] = df['Applicant_Name_Applicant_50_combinada'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Applicant_Name_Applicant_50_combinada'] = df['Applicant_Name_Applicant_50_combinada'].astype(str)
        df['Applicant_Name_Applicant_50_combinada'] = df['Applicant_Name_Applicant_50_combinada'].replace('', np.nan)
        df['Applicant_Name_Applicant_50_combinada'].replace('nan', np.nan, inplace=True)
    
    df["Applicant_Name"] = np.nan  # Inicializar con NaN
    if 'Applicant_Name_Applicant_50_combinada' in df.columns:
        fillna_if_exist(df, 'Applicant_Name',['Applicant_Name_Applicant_50_combinada','First_Beneficiary_50','Changed_Applicant_Details_50'])
    else:
        df["Applicant_Name_Applicant_50_combinada"] = np.nan
        fillna_if_exist(df, 'Applicant_Name',['Applicant_Name_Applicant_50_combinada','First_Beneficiary_50','Changed_Applicant_Details_50'])
        
    if 'Applicant_Name' in df.columns and 'Applicant_50' in df.columns:
        mask = df['SWIFT_Message_Type'] != 760
        df.loc[mask, 'Applicant_Name'] = df.loc[mask, 'Applicant_Name'].fillna(df.loc[mask, 'Applicant_50'])
    
    df['Beneficiary_59_Name_and_Address_combinada'] = df.apply(lambda row: combinar_columnas(row,df)[1], axis=1)
    if 'Beneficiary_59_Name_and_Address_combinada' in df.columns:
        patron_Applicant_50_combinada = r'local:(.*?)(?://|$)'
        df['Local_Beneficiary_Name'] = df['Beneficiary_59_Name_and_Address_combinada'].astype(str).apply(lambda x: re.findall(patron_Applicant_50_combinada, x, re.DOTALL))
        df['Local_Beneficiary_Name'] = df['Local_Beneficiary_Name'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        patron_Applicant_50_combinada_1 = r'//(.*)$'
        df['Beneficiary_Name_nolocal'] = df['Beneficiary_59_Name_and_Address_combinada'].astype(str).apply(lambda x: re.findall(patron_Applicant_50_combinada_1, x, re.DOTALL))
        df['Beneficiary_Name_nolocal'] = df['Beneficiary_Name_nolocal'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Beneficiary_Name_nolocal'] = df['Beneficiary_Name_nolocal'].astype(str)
        df['Beneficiary_Name_nolocal'] = df['Beneficiary_Name_nolocal'].replace('', np.nan)
        df['Beneficiary_Name_nolocal'].replace('nan', np.nan, inplace=True)

    lista_Beneficiary_59_10_Name_and_Address=[]
    lista_Beneficiary_59F_10_Number_Name_and_Address=[]
    for i in range(10,0,-1):
        lista_Beneficiary_59_10_Name_and_Address.append(f'Beneficiary_59_{i}_Name_and_Address')
        lista_Beneficiary_59F_10_Number_Name_and_Address.append(f'Beneficiary_59F_{i}_Number_Name_and_Address_Name_1')
    lista_Beneficiary_59_10_Name_and_Address=lista_Beneficiary_59_10_Name_and_Address+lista_Beneficiary_59F_10_Number_Name_and_Address
    lista_Beneficiary_59_10_Name_and_Address.extend(['Beneficiary_Customer_59F_Number_Name_and_Address_Name_1','Beneficiary_Customer_59_Name_and_Address','Beneficiary_Name_nolocal','Beneficiary_59_Name_and_Address','Beneficiary_Customer_59F_Number_Name_and_Address_Name_1','Second_Beneficiary_59_Name_and_Address'])
    df["Beneficiary_Name"] = np.nan
    if 'Beneficiary_59_10_Name_and_Address' in df.columns:
        fillna_if_exist(df, 'Beneficiary_Name',lista_Beneficiary_59_10_Name_and_Address)
    else:
        df["Beneficiary_59_10_Name_and_Address"] = np.nan
        fillna_if_exist(df, 'Beneficiary_Name',lista_Beneficiary_59_10_Name_and_Address)
    
    df['Beneficiary_59_Account_combinada'] = df.apply(lambda row: combinar_columnas(row,df)[2], axis=1)
    if 'Beneficiary_59_Account_combinada' in df.columns:
        patron_Applicant_50_combinada = r'local:(.*?)(?://|$)'
        df['Local_Beneficiary_Account'] = df['Beneficiary_59_Account_combinada'].astype(str).apply(lambda x: re.findall(patron_Applicant_50_combinada, x, re.DOTALL))
        df['Local_Beneficiary_Account'] = df['Local_Beneficiary_Account'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        patron_Applicant_50_combinada_1 = r'//(.*)$'
        df['Beneficiary_59_Account_nueva'] = df['Beneficiary_59_Account_combinada'].astype(str).apply(lambda x: re.findall(patron_Applicant_50_combinada_1, x, re.DOTALL))
        df['Beneficiary_59_Account_nueva'] = df['Beneficiary_59_Account_nueva'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Beneficiary_59_Account_nueva'] = df['Beneficiary_59_Account_nueva'].astype(str)
        df['Beneficiary_59_Account_nueva'] = df['Beneficiary_59_Account_nueva'].replace('', np.nan)
        df['Beneficiary_59_Account_nueva'].replace('nan', np.nan, inplace=True)


    df['Underlying_Transaction_Details_45L_combinada'] = df.apply(lambda row: combinar_columnas(row,df)[3], axis=1)
    if 'Underlying_Transaction_Details_45L_combinada' in df.columns:
        patron = r'local:(.*?)(?://|$)'
        df['Local_Transaction_Details'] = df['Underlying_Transaction_Details_45L_combinada'].astype(str).apply(lambda x: re.findall(patron, x, re.DOTALL))
        df['Local_Transaction_Details'] = df['Local_Transaction_Details'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        patron_1 = r'//(.*)$'
        df['Transaction_Details'] = df['Underlying_Transaction_Details_45L_combinada'].astype(str).apply(lambda x: re.findall(patron_1, x, re.DOTALL))
        df['Transaction_Details'] = df['Transaction_Details'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        df['Transaction_Details'] = df['Transaction_Details'].astype(str)
        df['Transaction_Details'] = df['Transaction_Details'].replace('', np.nan)
        df['Transaction_Details'].replace('nan', np.nan, inplace=True)

    return df

def cambiar_nombres_direccion(df):
    # Copiar el DataFrame para evitar modificar el original
    nombres_nuevos = {
    'SWIFT_Message_Type': 'Message_type',
    'Input_Output': 'Direction',
    'Currency/Instructed_Amount_33B_Currency':'Instructed_Currency',
    'Currency/Instructed_Amount_33B_Amount':'Instructed_Amount',
    'Beneficiary_Customer_59A_Identifier_Code': 'Beneficiary_Id',
    'Date_and_Place_of_Expiry_31D_Place':'Expiry_Place',
    'Applicant_Bank_51D_Name_and_Address':'Applicant_Bank_Name_and_Address',
    'Obligor/Instructing_Party_51':'Instructing_Party',
    'Place_of_Taking_in_Charge/Dispatch_from/Place_of_Receipt_44A':'Place_Dispatch_Receipt',
    'Port_of_Loading/Airport_of_Departure_44E':'PortLoading_AirportDeparture',
    'Place_of_Final_Destination/For_Transportation_to/Place_of_Delivery_44B':'Place_Destination_Delivery',
    'Latest_Date_of_Shipment_44C':'Latest_Date_Shipment',
    'Requested_Confirmation_Party_58D_Name_and_Address':'Requested_Confirmation_Party_Name',
    'Undertaking_Terms_and_Conditions_77U':'Undertaking_Terms',
    'Requested_Local_Undertaking_Terms_and_Conditions_77L':'Local_Undertaking_Terms',
    'Drawee_42D_Name_and_Address':'Drawee_Name',
    'Reimbursing_Bank_53D_Name_and_Address':'Reimbursing_Bank_Name',
    'Advising_Bank_56D_Name_and_Address': 'Advising_Bank_Name',
    'Type_of_Undertaking_22K_Code':'Local_Type_Purpose',
    'Beneficiary_Institution_58D_Name_and_Address':'Beneficiary_Institution_Name'
    }
    
    df_copiado = df.copy()
    df_copiado.rename(columns=nombres_nuevos, inplace=True)
    df_copiado = df_copiado.replace({None: np.nan})
    df_copiado['LTAddress'] = df_copiado['LTAddress'].astype(str)
    if 'Destination_Address' in df_copiado.columns:
        df_copiado['Destination_Address'] = df_copiado['Destination_Address'].astype(str)
    if 'UETR' in df_copiado.columns: 
        df_copiado['UETR']= df_copiado['UETR'].str.replace('121:','')

    # Cambiar los nombres de la columna 'Direction' según la lógica
    df_copiado['Direction'] = df_copiado['Direction'].apply(lambda x: 'Outgoing' if x == 'I' else 'Incoming' if x == 'O' else x)
    
    df_copiado['Sender_InstructingAgent_Requestor'] = df_copiado.apply(
    lambda row: str(row['LTAddress'])[:8] if row['Direction'] == 'Outgoing' and pd.notna(row['LTAddress']) else str(row['MIR'])[6:14] if pd.notna(row['MIR']) else None,
    axis=1
    )
    
    df_copiado['Receiver_InstructedAgent_Responder'] = df_copiado.apply(
    lambda row: str(row['LTAddress'])[:8] if row['Direction'] == 'Incoming' and pd.notna(row['LTAddress']) else str(row['Destination_Address'])[:8] if pd.notna(row['Destination_Address']) else None,
    axis=1
    )
    
    df_copiado['OwnBIC8'] = df_copiado.apply(
        lambda row: row['Sender_InstructingAgent_Requestor'] if row['Direction'] == 'Outgoing' else row['Receiver_InstructedAgent_Responder'],
        axis=1
    )

    df_copiado['Own_Country'] = df_copiado['OwnBIC8'].str[4:6]
    
    lista_Account_Servicing_Institution_52A_10_Identifier_Code=[]
    lista_Account_Servicing_Institution_52C_10=[]
    for i in range(10,0,-1):
        lista_Account_Servicing_Institution_52A_10_Identifier_Code.append(f'Account_Servicing_Institution_52A_{i}_Identifier_Code')
        lista_Account_Servicing_Institution_52C_10.append(f'Account_Servicing_Institution_52C_{i}')
    lista_Account_Servicing_Institution_52A_10_Identifier_Code=lista_Account_Servicing_Institution_52A_10_Identifier_Code+lista_Account_Servicing_Institution_52C_10
    lista_Account_Servicing_Institution_52A_10_Identifier_Code.extend(['Ordering_Institution_52A_1_Identifier_Code','Ordering_Institution_52A_Identifier_Code','Ordering_Institution_52D_1_Party_Identifier','Ordering_Institution_52D_Party_Identifier','Account_Servicing_Institution_52A_Identifier_Code','Account_Servicing_Institution_52C','Sender_InstructingAgent_Requestor'])
    df_copiado['Ordering_Institution'] = np.nan  # Inicializar con NaN
    if 'Account_Servicing_Institution_52A_10_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, 'Ordering_Institution',lista_Account_Servicing_Institution_52A_10_Identifier_Code)
    else:
        df_copiado['Account_Servicing_Institution_52A_10_Identifier_Code'] = np.nan
        fillna_if_exist(df_copiado, 'Ordering_Institution',lista_Account_Servicing_Institution_52A_10_Identifier_Code)
    
    df_copiado["Beneficiary_Institution"] = np.nan  # Inicializar con NaN
    if 'Beneficiary_Institution_58A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Institution", ['Beneficiary_Institution_58A_Identifier_Code', 'Beneficiary_Institution_58D_Party_Identifier'])
    elif 'Beneficiary_Institution_58D_Party_Identifier' in df_copiado.columns:
        df_copiado["Beneficiary_Institution"] = df_copiado['Beneficiary_Institution_58D_Party_Identifier']
        
    lista_Account_With_Institution_57A_10_Identifier_Code=[]
    lista_Account_With_Institution_57C_10=[]
    lista_Account_With_Institution_57D_10_Party_Identifier=[]
    for i in range(10,1,-1):
        lista_Account_With_Institution_57A_10_Identifier_Code.append(f'Account_With_Institution_57A_{i}_Identifier_Code')
        lista_Account_With_Institution_57C_10.append(f'Account_With_Institution_57C_{i}')
        lista_Account_With_Institution_57D_10_Party_Identifier.append(f'Account_With_Institution_57D_{i}_Party_Identifier')
    lista_Account_With_Institution_57A_10_Identifier_Code= lista_Account_With_Institution_57A_10_Identifier_Code+lista_Account_With_Institution_57C_10+lista_Account_With_Institution_57D_10_Party_Identifier
    lista_Account_With_Institution_57A_10_Identifier_Code.extend(['Account_With_Institution_57A_Identifier_Code','Account_With_Institution_57B_1_Party_Identifier' 'Account_With_Institution_57B_Party_Identifier','Account_With_Institution_57C_1_Party_Identifier','Account_With_Institution_57C_Party_Identifier','Account_With_Institution_57C_1','Account_With_Institution_57C','Account_With_Institution_57D_Party_Identifier','Beneficiary_Institution','Receiver_InstructedAgent_Responder'])
    df_copiado["Account_With_Institution"] = np.nan  # Inicializar con NaN
    if 'Account_With_Institution_57A_10_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, 'Account_With_Institution',lista_Account_With_Institution_57A_10_Identifier_Code )
    else:
        df_copiado["Account_With_Institution_57A_10_Identifier_Code"]=np.nan
        fillna_if_exist(df_copiado, 'Account_With_Institution',lista_Account_With_Institution_57A_10_Identifier_Code )
    
    df_copiado["Issuing_Bank_Name"] = np.nan  # Inicializar con NaN
    if 'Issuing_Bank_52D_Name_and_Address' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Issuing_Bank_Name", ['Issuing_Bank_52D_Name_and_Address', 'Issuer_52D_Name_and_Address','Issuing_Bank_of_the_Original_Documentary_Credit_52D_Name_and_Address'])
    else:
        df_copiado["Issuing_Bank_52D_Name_and_Address"] = np.nan 
        fillna_if_exist(df_copiado, "Issuing_Bank_Name", ['Issuing_Bank_52D_Name_and_Address', 'Issuer_52D_Name_and_Address','Issuing_Bank_of_the_Original_Documentary_Credit_52D_Name_and_Address'])
    
    lista_Remittance_Information_70=[]
    for i in range(10,0,-1):
        lista_Remittance_Information_70.append(f'Remittance_Information_70_{i}')
    lista_Remittance_Information_70.extend(['Remittance_Information_70'])
    df_copiado["Remittance_Information"] = np.nan  # Inicializar con NaN
    if 'Remittance_Information_70_10' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Remittance_Information",lista_Remittance_Information_70 )
    else:
        df_copiado["Remittance_Information_70_10"] = np.nan
        fillna_if_exist(df_copiado, "Remittance_Information",lista_Remittance_Information_70 )
        
    df_copiado["Advise_Through_Bank"] = np.nan  # Inicializar con NaN
    if 'Advise_Through_Bank_57A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Advise_Through_Bank", ['Advise_Through_Bank_57A_Identifier_Code', 'Advise_Through_Bank_57B_Party_Idendifier', 'Advise_Through_Bank_57D_Party_Idendifier','Receiver_InstructedAgent_Responder'])
    elif 'Advise_Through_Bank_57B_Party_Idendifier' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Advise_Through_Bank", ['Advise_Through_Bank_57B_Party_Idendifier', 'Advise_Through_Bank_57D_Party_Idendifier','Receiver_InstructedAgent_Responder'])
    elif 'Advise_Through_Bank_57D_Party_Idendifier' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Advise_Through_Bank", ['Advise_Through_Bank_57D_Party_Idendifier','Receiver_InstructedAgent_Responder'])
    elif 'Receiver_InstructedAgent_Responder' in df_copiado.columns:
        df_copiado["Advise_Through_Bank"] = df_copiado['Receiver_InstructedAgent_Responder']
    
    df_copiado["Issuing_Bank"] = np.nan  # Inicializar con NaN
    if 'Issuer_52A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Issuing_Bank", ['Issuer_52A_Identifier_Code', 'Issuer_52D_Party_Identifier','Issuing_Bank_52A_Identifier_Code','Issuing_Bank_52D_Party_Identifier','Issuing_Bank_of_the_Original_Documentary_Credit_52A_Identifier_Code','Issuing_Bank_of_the_Original_Documentary_Credit_52D_Party_Identifier','Sender_InstructingAgent_Requestor'])
    else:
        df_copiado["Issuer_52A_Identifier_Code"] = np.nan
        fillna_if_exist(df_copiado, "Issuing_Bank", ['Issuer_52A_Identifier_Code', 'Issuer_52D_Party_Identifier','Issuing_Bank_52A_Identifier_Code','Issuing_Bank_52D_Party_Identifier','Issuing_Bank_of_the_Original_Documentary_Credit_52A_Identifier_Code','Issuing_Bank_of_the_Original_Documentary_Credit_52D_Party_Identifier','Sender_InstructingAgent_Requestor'])
    

    df_copiado['Role'] = np.nan  # Inicializar con NaN
    df_copiado['Role'] = df_copiado['Role'].astype(object)  # Convertir a tipo object
    for index, row in df_copiado.iterrows():
        if not str(row['Message_type']).startswith('7'):
            if 'Ordering_Institution' in df_copiado.columns and pd.notna(row['OwnBIC8']) and pd.notna(row['Ordering_Institution']) and row['OwnBIC8'] in row['Ordering_Institution']:
                df_copiado.at[index, 'Role'] = "Debtor"
            elif 'Account_With_Institution' in df_copiado.columns and pd.notna(row['OwnBIC8']) and pd.notna(row['Account_With_Institution']) and row['OwnBIC8'] in row['Account_With_Institution']:
                df_copiado.at[index, 'Role'] = "Creditor"
            elif 'Beneficiary_Institution' in df_copiado.columns and pd.notna(row['OwnBIC8']) and pd.notna(row['Beneficiary_Institution']) and row['OwnBIC8'] in row['Beneficiary_Institution']:
                df_copiado.at[index, 'Role'] = "Creditor"
            else:
                df_copiado.at[index, 'Role'] = "Intermediary"
        elif pd.notna(row['Message_type']) and str(row['Message_type']).startswith('7'):
            if 'Advise_Through_Bank' in df_copiado.columns and pd.notna(row['OwnBIC8']) and pd.notna(row['Advise_Through_Bank']) and row['OwnBIC8'] in row['Advise_Through_Bank']:
                df_copiado.at[index, 'Role'] = "Advisor"
            elif 'Issuing_Bank' in df_copiado.columns and pd.notna(row['OwnBIC8']) and pd.notna(row['Issuing_Bank']) and row['OwnBIC8'] in row['Issuing_Bank']:
                df_copiado.at[index, 'Role'] = "Issuer"
            else:
                df_copiado.at[index, 'Role'] = "Intermediary"
        else:
            df_copiado.at[index, 'Role'] = np.nan
            
    df_copiado["Ordering_Institution_Name"] = np.nan  # Inicializar con NaN
    if 'Ordering_Institution_52D_1_Name_and_Address' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Ordering_Institution_Name", ['Ordering_Institution_52D_1_Name_and_Address','Ordering_Institution_52D_Name_and_Address'])
    elif 'Ordering_Institution_52D_Name_and_Address' in df_copiado.columns:
        df_copiado["Ordering_Institution_Name"] = df_copiado['Ordering_Institution_52D_Name_and_Address']
    
    df_copiado["Description_Goods"] = np.nan  # Inicializar con NaN
    if 'Description_of_Goods_and/or_Services_45A' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Description_Goods", ['Description_of_Goods_and/or_Services_45A','Description_of_Goods_and/or_Services_45B'])
    elif 'Description_of_Goods_and/or_Services_45B' in df_copiado.columns:
        df_copiado["Description_Goods"] = df_copiado['Description_of_Goods_and/or_Services_45B']
        
    #Columnas repetidas porque no se pueden separar
    if 'Ordering_Institution_Name' in df_copiado.columns:
        df_copiado["Ordering_Institution_Address"] = df_copiado['Ordering_Institution_Name']
    if 'Applicant_Name' in df_copiado.columns:
        df_copiado["Applicant_Address"] = df_copiado['Applicant_Name']
    if 'Local_Applicant_Name' in df_copiado.columns:
        df_copiado["Local_Applicant_Address"] = df_copiado['Local_Applicant_Name']
    if 'Issuing_Bank_Name' in df_copiado.columns:
        df_copiado["Issuing_Bank_Address"] = df_copiado['Issuing_Bank_Name']
    if 'Requested_Confirmation_Party_Name' in df_copiado.columns:
        df_copiado["Requested_Confirmation_Party_Address"] = df_copiado['Requested_Confirmation_Party_Name']
    if 'Drawee_Name' in df_copiado.columns:
        df_copiado["Drawee_Address"] = df_copiado['Drawee_Name']
    if 'Reimbursing_Bank_Name' in df_copiado.columns:
        df_copiado["Reimbursing_Bank_Address"] = df_copiado['Reimbursing_Bank_Name']
    if 'Advising_Bank_Name' in df_copiado.columns:
        df_copiado["Advising_Bank_Address"] = df_copiado['Advising_Bank_Name']
    if 'Local_Beneficiary_Name' in df_copiado.columns:
        df_copiado["Local_Beneficiary_Address"] = df_copiado['Local_Beneficiary_Name']
    if 'Beneficiary_Institution_Name' in df_copiado.columns:
        df_copiado["Beneficiary_Institution_Address"] = df_copiado['Beneficiary_Institution_Name']

    df_copiado["Senders_Correspondent"] = np.nan  # Inicializar con NaN
    if 'Senders_Correspondent_53A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Senders_Correspondent", ['Senders_Correspondent_53A_Identifier_Code', 'Senders_Correspondent_53D_Party_Identifier', 'Senders_Correspondent_53B_Party_Identifier'])
    elif 'Senders_Correspondent_53D_Party_Identifier' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Senders_Correspondent", ['Senders_Correspondent_53D_Party_Identifier', 'Senders_Correspondent_53B_Party_Identifier'])
    elif 'Senders_Correspondent_53B_Party_Identifier' in df_copiado.columns:
        df_copiado["Senders_Correspondent"] = df_copiado['Senders_Correspondent_53B_Party_Identifier']
    
    df_copiado["InstructionId"] = np.nan  # Inicializar con NaN
    if 'Senders_Reference_20' in df_copiado.columns:
        fillna_if_exist(df_copiado, "InstructionId", ['Senders_Reference_20','Transaction_Reference_Number_20'])
    elif 'Transaction_Reference_Number_20' in df_copiado.columns:
        df_copiado["InstructionId"] = df_copiado['Transaction_Reference_Number_20']
    
    df_copiado["Senders_Correspondent_Name_and_Address"] = np.nan  # Inicializar con NaN
    if 'Senders_Correspondent_53D_Name_and_Address' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Senders_Correspondent_Name_and_Address", ['Senders_Correspondent_53D_Name_and_Address', 'Senders_Correspondent_53B_Location'])
    elif 'Sender_Correspondent_53B_Location' in df_copiado.columns:
        df_copiado["Senders_Correspondent_Name_and_Address"] = df_copiado['Senders_Correspondent_53B_Location']

    df_copiado["Receivers_Correspondent"] = np.nan  # Inicializar con NaN
    if 'Receivers_Correspondent_54A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Receivers_Correspondent", ['Receivers_Correspondent_54A_Identifier_Code', 'Receivers_Correspondent_54D_Party_Identifier', 'Receivers_Correspondent_54B_Party_Identifier'])
    elif 'Receivers_Correspondent_54D_Party_Identifier' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Receivers_Correspondent", ['Receivers_Correspondent_54D_Party_Identifier', 'Receivers_Correspondent_54B_Party_Identifier'])
    elif 'Receivers_Correspondent_54B_Party_Identifier' in df_copiado.columns:
        df_copiado["Receivers_Correspondent"] = df_copiado['Receivers_Correspondent_54B_Party_Identifier']

    df_copiado["Receivers_Correspondent_Name_and_Address"] = np.nan  # Inicializar con NaN
    if 'Receivers_Correspondent_53D_Name_and_Address' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Receivers_Correspondent_Name_and_Address", ['Receivers_Correspondent_53D_Name_and_Address', 'Receivers_Correspondent_53B_Location'])
    elif 'Receivers_Correspondent_53B_Location' in df_copiado.columns:
        df_copiado["Receivers_Correspondent_Name_and_Address"] = df_copiado['Receivers_Correspondent_53B_Location']
    
    df_copiado["Documents_Required"] = np.nan  # Inicializar con NaN
    if 'Documents_Required_46A' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Documents_Required", ['Documents_Required_46A', 'Documents_Required_46B'])
    elif 'Sender_Correspondent_53B_Location' in df_copiado.columns:
        df_copiado["Documents_Required"] = df_copiado['Documents_Required_46B']
    
    df_copiado["Instructions"] = np.nan  # Inicializar con NaN
    if 'Instructions_to_the_Paying/Accepting/Negotiating_Bank_78' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Instructions", ['Instructions_to_the_Paying/Accepting/Negotiating_Bank_78', 'Document_and_Presentation_Instructions_45C'])
    elif 'Sender_Correspondent_53B_Location' in df_copiado.columns:
        df_copiado["Instructions"] = df_copiado['Document_and_Presentation_Instructions_45C']
    
    df_copiado["Additional_Conditions"] = np.nan  # Inicializar con NaN
    if 'Additional_Conditions_47A' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Additional_Conditions", ['Additional_Conditions_47A', 'Additional_Conditions_47B'])
    elif 'Additional_Conditions_47B' in df_copiado.columns:
        df_copiado["Additional_Conditions"] = df_copiado['Additional_Conditions_47B']

    df_copiado["PortDischarge_AirportDestination"] = np.nan  # Inicializar con NaN
    if 'Port_of_Discharge/Airport_of_Departure_44F' in df_copiado.columns:
        fillna_if_exist(df_copiado, "PortDischarge_AirportDestination", ['Port_of_Discharge/Airport_of_Departure_44F', 'Port_of_Discharge/Airport_of_Destination_44F'])
    elif 'Port_of_Discharge/Airport_of_Destination_44F' in df_copiado.columns:
        df_copiado["PortDischarge_AirportDestination"] = df_copiado['Port_of_Discharge/Airport_of_Destination_44F']
    
    df_copiado["Third_Reimbursement_Institution"] = np.nan  # Inicializar con NaN
    if 'Third_Reimbursement_Institution_55A_Party_Identifier' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Third_Reimbursement_Institution", ['Third_Reimbursement_Institution_55A_Party_Identifier', 'Third_Reimbursement_Institution_55B_Party_Identifier', 'Third_Reimbursement_Institution_55D_Party_Identifier'])
    elif 'Third_Reimbursement_Institution_55B_Party_Identifier' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Third_Reimbursement_Institution", ['Third_Reimbursement_Institution_55B_Party_Identifier', 'Third_Reimbursement_Institution_55D_Party_Identifier'])
    elif 'Third_Reimbursement_Institution_55D_Party_Identifier' in df_copiado.columns:
        df_copiado["Third_Reimbursement_Institution"] = df_copiado['Third_Reimbursement_Institution_55D_Party_Identifier']

    df_copiado["Third_Reimbursement_Institution_Name_and_Address"] = np.nan  # Inicializar con NaN
    if 'Third_Reimbursement_Institution_55D_Name_and_Address' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Third_Reimbursement_Institution_Name_and_Address", ['Third_Reimbursement_Institution_55D_Name_and_Address', 'Third_Reimbursement_Institution_55B_Location'])
    elif 'Third_Reimbursement_Institution_55B_Location' in df_copiado.columns:
        df_copiado["Third_Reimbursement_Institution_Name_and_Address"] = df_copiado['Third_Reimbursement_Institution_55B_Location']
                                                                              
    lista_Beneficiary_59_Account_nueva=[]
    for i in range(10,0,-1):
        lista_Beneficiary_59_Account_nueva.append(f'Beneficiary_59_Account_{i}')
    lista_Beneficiary_59_Account_nueva.extend( ['Beneficiary_59_Account_nueva','Beneficiary_Customer_59_Account', 'Beneficiary_Customer_59A_Account','Beneficiary_59_Account','Beneficiary_Customer_59F_Account'])
    df_copiado["Beneficiary_Account"] = np.nan  # Inicializar con NaN
    if 'Beneficiary_59_Account_10' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Beneficiary_Account",lista_Beneficiary_59_Account_nueva)
    else:
        df_copiado["Beneficiary_59_Account_10"] = np.nan 
        fillna_if_exist(df_copiado, "Beneficiary_Account",lista_Beneficiary_59_Account_nueva)
    
    lista_Account_With_Institution_57D_1_Name_and_Address=[]
    for i in range(10,0,-1):
        lista_Account_With_Institution_57D_1_Name_and_Address.append(f'Account_With_Institution_57D_{i}_Name_and_Address')
    lista_Account_With_Institution_57D_1_Name_and_Address.extend(['Account_With_Institution_57D_Name_and_Address','Account_With_Institution_57B_1_Location','Account_With_Institution_57B_Location'])
    df_copiado["Account_With_Institution_Name_and_Address"] = np.nan  # Inicializar con NaN
    if 'Account_With_Institution_57D_10_Name_and_Address' in df_copiado.columns:
        fillna_if_exist(df_copiado, 'Account_With_Institution_Name_and_Address',lista_Account_With_Institution_57D_1_Name_and_Address)
    else:
        df_copiado["Account_With_Institution_57D_10_Name_and_Address"] = np.nan 
        fillna_if_exist(df_copiado, 'Account_With_Institution_Name_and_Address',lista_Account_With_Institution_57D_1_Name_and_Address)
                                                                       
    df_copiado["Type_Purpose"] = np.nan   
    if 'Form_of_Documentary_Credit_40A' in df_copiado.columns:
        fillna_if_exist(df_copiado, 'Type_Purpose', ['Form_of_Documentary_Credit_40A', 'Purpose_of_Message_22A','Form_of_Documentary_Credit_40B_Type'])
    elif 'Purpose_of_Message_22A' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Type_Purpose", ['Purpose_of_Message_22A', 'Form_of_Documentary_Credit_40B_Type'])
    elif 'Form_of_Documentary_Credit_40B_Type' in df_copiado.columns:
        df_copiado["Type_Purpose"] = df_copiado['Form_of_Documentary_Credit_40B_Type']
        
    df_copiado["Issuing_Banks_Reference"] = np.nan  # Inicializar con NaN
    if 'Issuing_Banks_Reference_23' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Issuing_Banks_Reference", ['Issuing_Banks_Reference_23', 'Documentary_Credit_Number_20','Documentary_Credit_Number_21','Undertaking_Number_20','Senders_TRN_20'])
    elif 'Documentary_Credit_Number_20' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Issuing_Banks_Reference", ['Documentary_Credit_Number_20','Documentary_Credit_Number_21','Undertaking_Number_20','Senders_TRN_20'])
    elif 'Documentary_Credit_Number_21' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Issuing_Banks_Reference", ['Documentary_Credit_Number_21','Undertaking_Number_20','Senders_TRN_20'])
    elif 'Undertaking_Number_20' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Issuing_Banks_Reference", ['Undertaking_Number_20','Senders_TRN_20'])
    elif 'Senders_TRN_20' in df_copiado.columns:
        df_copiado["Issuing_Banks_Reference"] = df_copiado['Senders_TRN_20']
    
    
    df_copiado["Senders_Reference"] = np.nan  # Inicializar con NaN
    if 'Senders_Reference_20' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Senders_Reference", ['Senders_Reference_20', 'Documentary_Credit_Number_20','Transferring_Banks_Reference_20','Transaction_Reference_Number_20','Undertaking_Number_20'])
    elif 'Documentary_Credit_Number_20' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Senders_Reference", ['Documentary_Credit_Number_20','Transferring_Banks_Reference_20','Transaction_Reference_Number_20','Undertaking_Number_20'])
    elif 'Transferring_Banks_Reference_20' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Senders_Reference", ['Transferring_Banks_Reference_20','Transaction_Reference_Number_20','Undertaking_Number_20'])
    elif 'Transaction_Reference_Number_20' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Senders_Reference", ['Transaction_Reference_Number_20','Undertaking_Number_20'])
    elif 'Undertaking_Number_20' in df_copiado.columns:
        df_copiado["Senders_Reference"] = df_copiado['Undertaking_Number_20']
    
    df_copiado["Receivers_Reference"] = np.nan  # Inicializar con NaN
    if 'Receivers_Reference_21' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Receivers_Reference", ['Receivers_Reference_21','Documentary_Credit_Number_21','Presenting_Banks_Reference_21','Related_Reference_21'])
    elif 'Documentary_Credit_Number_21' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Receivers_Reference", ['Documentary_Credit_Number_21','Presenting_Banks_Reference_21','Related_Reference_21'])
    elif 'Presenting_Banks_Reference_21' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Receivers_Reference", ['Presenting_Banks_Reference_21', 'Related_Reference_21'])
    elif 'Related_Reference_21' in df_copiado.columns:
        df_copiado["Receivers_Reference"] = df_copiado['Related_Reference_21']
    
    df_copiado["Issue_Date"] = np.nan  # Inicializar con NaN
    if 'Date_of_Issue_31C' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Issue_Date", ['Date_of_Issue_31C','Date_and_Amount_of_Utilisation_32A_Date','Date_of_Issue_30'])
    elif 'Date_and_Amount_of_Utilisation_32A_Date' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Issue_Date", ['Date_and_Amount_of_Utilisation_32A_Date','Date_of_Issue_30'])
    elif 'Date_of_Issue_30' in df_copiado.columns:
        df_copiado["Issue_Date"] = df_copiado['Date_of_Issue_30']
    
    df_copiado["Expiry_Date"] = np.nan  # Inicializar con NaN
    if 'Date_and_Place_of_Expiry_31D_Date' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Expiry_Date", ['Date_and_Place_of_Expiry_31D_Date', 'Date_of_Expiry_31E'])
    elif 'Date_of_Expiry_31E' in df_copiado.columns:
        df_copiado["Expiry_Date"] = df_copiado['Date_of_Expiry_31E']

    df_copiado["Applicant_Bank"] = np.nan  # Inicializar con NaN
    if 'Applicant_Bank_51A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Applicant_Bank", ['Applicant_Bank_51A_Identifier_Code', 'Applicant_Bank_51D_Party_Idendifier', 'Issuing_Bank'])
    elif 'Applicant_Bank_51D_Party_Idendifier' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Applicant_Bank", ['Applicant_Bank_51D_Party_Idendifier', 'Issuing_Bank'])
    elif 'Issuing_Bank' in df_copiado.columns:
        df_copiado["Applicant_Bank"] = df_copiado['Issuing_Bank']

    df_copiado["Currency"] = np.nan  # Inicializar con NaN
    if 'Currency_Code_Amount_32B_Currency' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Currency", ['Currency_Code_Amount_32B_Currency','Date_and_Amount_of_Utilisation_32A_Currency','Increase_of_Documentary_Credit_Amount_32B_Currency','Undertaking_Amount_32B_Currency'])
    elif 'Date_and_Amount_of_Utilisation_32A_Currency' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Currency", ['Date_and_Amount_of_Utilisation_32A_Currency','Increase_of_Documentary_Credit_Amount_32B_Currency','Undertaking_Amount_32B_Currency'])
    elif 'Increase_of_Documentary_Credit_Amount_32B_Currency' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Currency", ['Increase_of_Documentary_Credit_Amount_32B_Currency','Undertaking_Amount_32B_Currency'])
    elif 'Undertaking_Amount_32B_Currency' in df_copiado.columns:
        df_copiado["Currency"] = df_copiado['Undertaking_Amount_32B_Currency']
    
    df_copiado["Local_Currency"] = np.nan  # Inicializar con NaN
    if 'Undertaking_Amount_32B_1_Currency' in df_copiado.columns:
        df_copiado["Local_Currency"] = df_copiado['Undertaking_Amount_32B_1_Currency']


    df_copiado["Amount"] = np.nan  # Inicializar con NaN
    if 'Currency_Code_Amount_32B_Amount' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Amount", ['Currency_Code_Amount_32B_Amount','Date_and_Amount_of_Utilisation_32A_Amount','Increase_of_Documentary_Credit_Amount_32B_Amount','Undertaking_Amount_32B_Amount'])
    elif 'Date_and_Amount_of_Utilisation_32A_Amount' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Amount", ['Date_and_Amount_of_Utilisation_32A_Amount','Increase_of_Documentary_Credit_Amount_32B_Amount','Undertaking_Amount_32B_Amount'])
    elif 'Increase_of_Documentary_Credit_Amount_32B_Amount' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Amount", ['Increase_of_Documentary_Credit_Amount_32B_Amount','Undertaking_Amount_32B_Amount'])
    elif 'Undertaking_Amount_32B_Amount' in df_copiado.columns:
        df_copiado["Amount"] = df_copiado['Undertaking_Amount_32B_Amount']

    df_copiado["Local_Amount"] = np.nan  # Inicializar con NaN
    if 'Undertaking_Amount_32B_1_Amount' in df_copiado.columns:
        df_copiado["Local_Amount"] = df_copiado['Undertaking_Amount_32B_1_Amount']
    
    df_copiado["Available_Bank"] = np.nan  # Inicializar con NaN
    if 'Available_With_By_41A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Available_Bank", ['Available_With_By_41A_Identifier_Code', 'Available_With_41F'])
    elif 'Available_With_41F' in df_copiado.columns:
        df_copiado["Available_Bank"] = df_copiado['Available_With_41F']

    df_copiado["Available_Bank_Name"] = np.nan  # Inicializar con NaN
    if 'Available_With_By_41D_Name_and_Address' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Available_Bank_Name", ['Available_With_By_41D_Name_and_Address', 'Available_With_41G'])
    elif 'Available_With_41G' in df_copiado.columns:
        df_copiado["Available_Bank_Name"] = df_copiado['Available_With_41G']
    df_copiado["Available_Bank_Address"] = df_copiado['Available_Bank_Name']
                                                                       
    df_copiado["Drawee"] = np.nan  # Inicializar con NaN
    if 'Drawee_42A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Drawee", ['Drawee_42A_Identifier_Code', 'Drawee_42D_Party_Identifier'])
    elif 'Drawee_42D_Party_Identifier' in df_copiado.columns:
        df_copiado["Drawee"] = df_copiado['Drawee_42D_Party_Identifier']

    df_copiado["Requested_Confirmation_Party"] = np.nan  # Inicializar con NaN
    if 'Requested_Confirmation_Party_58A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Requested_Confirmation_Party", ['Requested_Confirmation_Party_58A_Identifier_Code', 'Requested_Confirmation_Party_58D_Party_Identifier'])
    elif 'Requested_Confirmation_Party_58D_Party_Identifier' in df_copiado.columns:
        df_copiado["Requested_Confirmation_Party"] = df_copiado['Requested_Confirmation_Party_58D_Party_Identifier']

    df_copiado["Reimbursing_Bank"] = np.nan  # Inicializar con NaN
    if 'Reimbursing_Bank_53A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Reimbursing_Bank", ['Reimbursing_Bank_53A_Identifier_Code', 'Reimbursing_Bank_53D_Party_Identifier'])
    elif 'Reimbursing_Bank_53D_Party_Identifier' in df_copiado.columns:
        df_copiado["Reimbursing_Bank"] = df_copiado['Reimbursing_Bank_53D_Party_Identifier']

    df_copiado["Advising_Bank"] = np.nan  # Inicializar con NaN
    if 'Advising_Bank_56A_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Advising_Bank", ['Advising_Bank_56A_Identifier_Code','Advising_Bank_56A_Party_Identifier','Advising_Bank_56D_Party_Identifier'])
    elif 'Advising_Bank_56A_Party_Identifier' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Advising_Bank", ['Advising_Bank_56A_Party_Identifier', 'Advising_Bank_56D_Party_Identifier'])
    elif 'Advising_Bank_56D_Party_Identifier' in df_copiado.columns:
        df_copiado["Advising_Bank"] = df_copiado['Advising_Bank_56D_Party_Identifier']
    
    df_copiado["Advise_Through_Bank_Name_and_Address"] = np.nan  # Inicializar con NaN
    if 'Advise_Through_Bank_57B_Location' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Advise_Through_Bank_Name_and_Address", ['Advise_Through_Bank_57B_Location', 'Advise_Through_Bank_57D_Name_and_Address'])
    elif 'Advise_Through_Bank_57D_Name_and_Address' in df_copiado.columns:
        df_copiado["Advise_Through_Bank_Name_and_Address"] = df_copiado['Advise_Through_Bank_57D_Name_and_Address']
    
    df_copiado["Related_Message_Id"] = np.nan
    lista_Transaction_Reference_21_10=[]
    for i in range(10,0,-1):
        lista_Transaction_Reference_21_10.append(f'Transaction_Reference_21_{i}')
    lista_Transaction_Reference_21_10.extend(['Transaction_Reference_21','Related_Reference_21'])
    if 'Transaction_Reference_21_10' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Related_Message_Id",lista_Transaction_Reference_21_10)
    else:
        df_copiado["Transaction_Reference_21_10"] = np.nan
        fillna_if_exist(df_copiado, "Related_Message_Id",lista_Transaction_Reference_21_10)
    
    if 'Sender_to_Receiver_Information_72_1' in df_copiado.columns and not df_copiado['Sender_to_Receiver_Information_72_1'].isnull().all():
        df_copiado['Sender_to_Receiver_Information_72_1'] = df_copiado['Sender_to_Receiver_Information_72_1'].apply(lambda x: f'72 Sender to Receiver Information: {x}' if pd.notnull(x) else x)
    if 'Sender_to_Receiver_Information_72' in df_copiado.columns and not df_copiado['Sender_to_Receiver_Information_72'].isnull().all():
        df_copiado['Sender_to_Receiver_Information_72'] = df_copiado['Sender_to_Receiver_Information_72'].apply(lambda x: f'72 Sender to Receiver Information: {x}' if pd.notnull(x) else x)
    if 'Sender_to_Receiver_Information_72Z' in df_copiado.columns and not df_copiado['Sender_to_Receiver_Information_72Z'].isnull().all():
        df_copiado['Sender_to_Receiver_Information_72Z'] = df_copiado['Sender_to_Receiver_Information_72Z'].apply(lambda x: f'72Z Sender to Receiver Information: {x}' if pd.notnull(x) else x)
    if 'Discrepancies_77J' in df_copiado.columns and not df_copiado['Discrepancies_77J'].isnull().all():
        df_copiado['Discrepancies_77J'] = df_copiado['Discrepancies_77J'].apply(lambda x: f'77J Discrepancies: {x}' if pd.notnull(x) else x)
    if 'Sender_to_Receiver_Information_72Z' in df_copiado.columns and 'Discrepancies_77J' in df_copiado.columns:  
        df_copiado['Combinada_72Z_77J'] = df_copiado.apply(lambda row: str(row['Sender_to_Receiver_Information_72Z']) + ' ' + str(row['Discrepancies_77J']) if pd.notna(row['Sender_to_Receiver_Information_72Z']) and pd.notna(row['Discrepancies_77J']) else None, axis=1)
    
    df_copiado["Sender_to_Receiver_Information"] = np.nan  # Inicializar con NaN
    if 'Sender_to_Receiver_Information_72_1' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Sender_to_Receiver_Information",['Sender_to_Receiver_Information_72_1','Sender_to_Receiver_Information_72','Combinada_72Z_77J','Sender_to_Receiver_Information_72Z','Discrepancies_77J'])
    elif 'Sender_to_Receiver_Information_72' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Sender_to_Receiver_Information",['Sender_to_Receiver_Information_72','Combinada_72Z_77J','Sender_to_Receiver_Information_72Z','Discrepancies_77J'])
    elif 'Combinada_72Z_77J' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Sender_to_Receiver_Information",['Combinada_72Z_77J','Sender_to_Receiver_Information_72Z','Discrepancies_77J'])
    elif 'Sender_to_Receiver_Information_72Z' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Sender_to_Receiver_Information",['Sender_to_Receiver_Information_72Z','Discrepancies_77J'])
    elif 'Discrepancies_77J' in df_copiado.columns:
        df_copiado["Sender_to_Receiver_Information"] = df_copiado['Discrepancies_77J']
    
    df_copiado["Interbank_Settled_Date"] = np.nan  # Inicializar con NaN
    if 'Value_Date/Currency/Interbank_Settled_Amount_32A_Date' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Interbank_Settled_Date", ['Value_Date/Currency/Interbank_Settled_Amount_32A_Date','Requested_Execution_Date_30','Value_Date_Currency_Code_Amount_32A_Date'])
    else:
        df_copiado["Value_Date/Currency/Interbank_Settled_Amount_32A_Date"] = np.nan
        fillna_if_exist(df_copiado, "Interbank_Settled_Date", ['Value_Date/Currency/Interbank_Settled_Amount_32A_Date','Requested_Execution_Date_30','Value_Date_Currency_Code_Amount_32A_Date'])
    if 'Interbank_Settled_Date' in df_copiado.columns:
        df_copiado['Interbank_Settled_Date'] = df_copiado['Interbank_Settled_Date'].astype(str)
        df_copiado['Interbank_Settled_Date'] = df_copiado['Interbank_Settled_Date'].str.replace(' ', '')
        mask = df_copiado['Interbank_Settled_Date'].str.len() == 6  # Suponiendo que el formato anterior tiene 6 caracteres
        df_copiado.loc[mask, 'Interbank_Settled_Date'] = df_copiado.loc[mask, 'Interbank_Settled_Date'].apply(lambda x: pd.to_datetime(x, format='%y%m%d').strftime('%Y/%m/%d'))
    
    df_copiado["Interbank_Settled_Currency"] = np.nan  # Inicializar con NaN
    lista_Currency_Transaction_Amount_32B_10_Currency=[]
    lista_Currency_Transaction_Amount_32B_10_Amount=[]
    for i in range(10,0,-1):
        lista_Currency_Transaction_Amount_32B_10_Currency.append(f'Currency/Transaction_Amount_32B_{i}_Currency')
        lista_Currency_Transaction_Amount_32B_10_Amount.append(f'Currency/Transaction_Amount_32B_{i}_Amount')
    lista_Currency_Transaction_Amount_32B_10_Currency.extend(['Currency/Transaction_Amount_32B_Currency','Value_Date/Currency/Interbank_Settled_Amount_32A_Currency', 'Value_Date_Currency_Code_Amount_32A_Currency'])
    lista_Currency_Transaction_Amount_32B_10_Amount.extend(['Currency/Transaction_Amount_32B_Amount','Value_Date/Currency/Interbank_Settled_Amount_32A_Amount', 'Value_Date_Currency_Code_Amount_32A_Amount'])
    if 'Currency/Transaction_Amount_32B_10_Currency' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Interbank_Settled_Currency",lista_Currency_Transaction_Amount_32B_10_Currency )
    else:
        df_copiado["Currency/Transaction_Amount_32B_10_Currency"] = np.nan  # Inicializar con NaN
        fillna_if_exist(df_copiado, "Interbank_Settled_Currency", lista_Currency_Transaction_Amount_32B_10_Currency)

    df_copiado["Interbank_Settled_Amount"] = np.nan  # Inicializar con NaN
    if 'Currency/Transaction_Amount_32B_10_Amount' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Interbank_Settled_Amount", lista_Currency_Transaction_Amount_32B_10_Amount)
    else:
        df_copiado["Currency/Transaction_Amount_32B_10_Amount"] = np.nan  # Inicializar con NaN
        fillna_if_exist(df_copiado, "Interbank_Settled_Amount",lista_Currency_Transaction_Amount_32B_10_Amount)
                
    lista_Intermediary_56A_10_Identifier_Code=[]
    lista_Intermediary_56D_10_Name_and_Address=[]
    for i in range(10,0,-1):
        lista_Intermediary_56A_10_Identifier_Code.append(f'Intermediary_56A_{i}_Identifier_Code')
        lista_Intermediary_56D_10_Name_and_Address.append(f'Intermediary_56D_{i}_Name_and_Address')
    lista_Intermediary_56A_10_Identifier_Code=lista_Intermediary_56A_10_Identifier_Code+lista_Intermediary_56D_10_Name_and_Address
    lista_Intermediary_56A_10_Identifier_Code.extend(['Intermediary_Institution_56A_Identifier_Code','Intermediary_56A_Identifier_Code','Intermediary_56D_Name_and_Address'])
    df_copiado["Intermediary_Institution"] = np.nan  
    if 'Intermediary_56A_10_Identifier_Code' in df_copiado.columns:
        fillna_if_exist(df_copiado, "Intermediary_Institution",lista_Intermediary_56A_10_Identifier_Code )
    else:
        df_copiado["Intermediary_56A_10_Identifier_Code"] = np.nan
        fillna_if_exist(df_copiado, "Intermediary_Institution",lista_Intermediary_56A_10_Identifier_Code)
        
    df_copiado['Narrative'] = df_copiado.apply(extract_copy_value, axis=1)
    if 'Narrative' in df_copiado.columns:
        df_copiado['Narrative'] = df_copiado['Narrative'].str.replace(r'\{.*$', '', regex=True)
        df_copiado['Narrative'] = df_copiado['Narrative'].str.replace(':79:', ':79Narrative:').str.replace(':75:', ':75Queries:').str.replace(':77A:', ':77Narrative:').str.replace(':76:', ':76Answers:')
        df_copiado['Narrative']=  df_copiado['Narrative'].str.replace(r':11S:.*?:', '', regex=True)
        df_copiado['Narrative']=  df_copiado['Narrative'].str.replace(r':11R:.*?:', '', regex=True)
    
    if 'Latest_Date_Shipment' in df_copiado.columns and not df_copiado['Latest_Date_Shipment'].isnull().all():
        df_copiado['Latest_Date_Shipment'] = df_copiado['Latest_Date_Shipment'].str.replace('-', '')
    if 'Interbank_Settled_Amount' in df_copiado.columns and not df_copiado['Interbank_Settled_Amount'].isnull().all():
        df_copiado['Interbank_Settled_Amount'] = df_copiado['Interbank_Settled_Amount'].str.replace(',', '.')
        df_copiado['Interbank_Settled_Amount'] = df_copiado['Interbank_Settled_Amount'].str.rstrip('0').str.rstrip('.')
    if 'Instructed_Amount' in df_copiado.columns and not df_copiado['Instructed_Amount'].isnull().all():
        df_copiado['Instructed_Amount'] = df_copiado['Instructed_Amount'].str.replace(',', '.')
        df_copiado['Instructed_Amount'] = df_copiado['Instructed_Amount'].str.rstrip('0').str.rstrip('.')
    if 'Amount' in df_copiado.columns and not df_copiado['Amount'].isnull().all():
        df_copiado['Amount'] = df_copiado['Amount'].str.replace(',', '.')
        df_copiado['Amount'] = df_copiado['Amount'].str.rstrip('0').str.rstrip('.')
    if 'Expiry_Date' in df_copiado.columns and not df_copiado['Expiry_Date'].isnull().all():
        df_copiado['Expiry_Date'] = df_copiado['Expiry_Date'].str.replace('-', '')
    if 'Beneficiary_Name' in df_copiado.columns and not df_copiado['Beneficiary_Name'].isnull().all():
        df_copiado['Beneficiary_Name'] = df_copiado['Beneficiary_Name'].str.lstrip(':')
    
    mask = df_copiado['Message_type'].str.endswith(('90', '91', '92', '93', '94', '95', '96', '97', '98', '99'))
    df_copiado.loc[mask, ['Role', 'Ordering_Institution','Account_With_Institution']] = np.nan
        
    columnas_deseadas = ['Filename','Message_Date','Message_Content','Creation_date','Interbank_Settled_Date', 'Direction', 'Sender_InstructingAgent_Requestor', 'Receiver_InstructedAgent_Responder', 'OwnBIC8', 'Own_Country', 'Role', 'Format', 'Message_type', 'InstructionId', 'End2EndId', 'UETR', 'Related_Message_Id', 'Transaction_Reference', 'Interbank_Settled_Currency', 'Interbank_Settled_Amount', 'Instructed_Currency', 'Instructed_Amount', 'Ordering_Customer_Account', 'Ordering_Customer_Name','Ordering_Customer_Id', 'Ordering_Customer_Address','Ordering_Institution', 'Ordering_Institution_Name','Ordering_Institution_Address', "Senders_Correspondent", "Senders_Correspondent_Name_and_Address", "Receivers_Correspondent",'Receivers_Correspondent_Name_and_Address', 'Third_Reimbursement_Institution', 'Third_Reimbursement_Institution_Name_and_Address', 'Intermediary_Institution', 'Account_With_Institution','Account_With_Institution_Name_and_Address', 'Beneficiary_Account','Beneficiary_Name', 'Beneficiary_Address','Beneficiary_Address_Country', 'Remittance_Information', 'Sender_to_Receiver_Information','Ordering_Customer_Address_Country','Ordering_Customer_Date_Of_Birth','Ordering_Customer_Place_Of_Birth','Ordering_Customer_ID_Number','Ordering_Customer_National_ID_Number','Ordering_Customer_Additional_Information','Beneficiary_Id','Type_Purpose','Senders_Reference','Receivers_Reference','Issue_Date','Expiry_Date','Expiry_Place','Applicant_Bank','Applicant_Bank_Name_and_Address','Applicant_Bank_Name','Applicant_Bank_Address','Instructing_Party','Issuing_Bank','Issuing_Bank_Name','Issuing_Bank_Address','Currency','Amount','Available_Bank','Available_Bank_Name','Available_Bank_Address','Drawee','Drawee_Name','Drawee_Address','Place_Dispatch_Receipt','PortLoading_AirportDeparture','PortDischarge_AirportDestination','Place_Destination_Delivery','Latest_Date_Shipment','Description_Goods','Documents_Required','Requested_Confirmation_Party','Requested_Confirmation_Party_Name','Requested_Confirmation_Party_Address','Reimbursing_Bank','Reimbursing_Bank_Name','Reimbursing_Bank_Address','Instructions','Advising_Bank','Advising_Bank_Name','Advising_Bank_Address','Advise_Through_Bank','Advise_Through_Bank_Name','Advise_Through_Bank_Address','Advise_Through_Bank_Name_and_Address','Undertaking_Terms','Transaction_Details','Local_Transaction_Details','Local_Type_Purpose','Applicant_Name','Applicant_Address','Local_Applicant_Name','Local_Applicant_Address','Local_Beneficiary_Account','Local_Beneficiary_Name','Local_Beneficiary_Address','Local_Currency','Local_Amount','Local_Undertaking_Terms','Narrative','Issuing_Banks_Reference','Additional_Conditions','Beneficiary_Institution','Beneficiary_Institution_Name','Beneficiary_Institution_Address']    
    
    columnas_existen = [col for col in columnas_deseadas if col in df_copiado.columns]

    df_filtrado = df_copiado[columnas_existen]
    df_filtrado = df_filtrado.drop_duplicates()
    condicion = (df_filtrado['Message_type'].isin(['700','701', '707','708','710','711','720','721','760','761']))
    No_700_df = df_filtrado[~condicion]
    df_filtrado = df_filtrado[condicion]
    condicion = (df_filtrado['Message_type'].isin(['700', '707', '710', '720','760'])) & (df_filtrado.duplicated(subset=['Senders_Reference', 'Message_type'], keep='first'))
    df_primeras_repeticiones = df_filtrado[~condicion]
    df_primeras_repeticiones = df_primeras_repeticiones.sort_values(by='Message_type')
    condicion_segundas_repeticiones = (df_filtrado['Message_type'].isin(['700', '707', '710', '720','760'])) & (df_filtrado.duplicated(subset=['Senders_Reference', 'Message_type'], keep='last'))
    df_segundas_repeticiones = df_filtrado[~condicion_segundas_repeticiones]
    df_segundas_repeticiones = df_segundas_repeticiones.sort_values(by='Message_type')
    columnas_combinar = ['Description_Goods', 'Documents_Required', 'Additional_Conditions','Message_Content','Local_Undertaking_Terms','Undertaking_Terms']
    columnas_presentes_1 = [col for col in columnas_combinar if col in df_filtrado.columns]
    agg_functions_1 = {col: ' '.join if col in columnas_presentes_1 else 'first' for col in df_primeras_repeticiones.columns if col != 'Senders_Reference'}
    agg_functions_2 = {col: ' '.join if col in columnas_presentes_1 else 'first' for col in df_segundas_repeticiones.columns if col != 'Senders_Reference'}
    for col in columnas_presentes_1:
        df_primeras_repeticiones[col] = df_primeras_repeticiones[col].fillna('')
        df_segundas_repeticiones[col] = df_segundas_repeticiones[col].fillna('')
    mask_700 = df_primeras_repeticiones['Message_type'].isin(['700', '701'])
    mask_707 = df_primeras_repeticiones['Message_type'].isin(['707', '708'])
    mask_710 = df_primeras_repeticiones['Message_type'].isin(['710', '711'])
    mask_720 = df_primeras_repeticiones['Message_type'].isin(['720', '721'])
    mask_760 = df_primeras_repeticiones['Message_type'].isin(['760', '761'])
    combined_row_1 = df_primeras_repeticiones[mask_700].groupby('Senders_Reference').agg(agg_functions_1).reset_index()
    combined_row_2 = df_primeras_repeticiones[mask_707].groupby('Senders_Reference').agg(agg_functions_1).reset_index()
    combined_row_3 = df_primeras_repeticiones[mask_710].groupby('Senders_Reference').agg(agg_functions_1).reset_index()
    combined_row_4 = df_primeras_repeticiones[mask_720].groupby('Senders_Reference').agg(agg_functions_1).reset_index()
    combined_row_5 = df_primeras_repeticiones[mask_760].groupby('Senders_Reference').agg(agg_functions_1).reset_index()
    df_primeras_repeticiones = pd.concat([combined_row_1, combined_row_2, combined_row_3, combined_row_4, combined_row_5], ignore_index=True)
    mask_700 = df_segundas_repeticiones['Message_type'].isin(['700', '701'])
    mask_707 = df_segundas_repeticiones['Message_type'].isin(['707', '708'])
    mask_710 = df_segundas_repeticiones['Message_type'].isin(['710', '711'])
    mask_720 = df_segundas_repeticiones['Message_type'].isin(['720', '721'])
    mask_760 = df_segundas_repeticiones['Message_type'].isin(['760', '761'])
    combined_row_1_1 = df_segundas_repeticiones[mask_700].groupby('Senders_Reference').agg(agg_functions_2).reset_index()
    combined_row_2_1 = df_segundas_repeticiones[mask_707].groupby('Senders_Reference').agg(agg_functions_2).reset_index()
    combined_row_3_1 = df_segundas_repeticiones[mask_710].groupby('Senders_Reference').agg(agg_functions_2).reset_index()
    combined_row_4_1 = df_segundas_repeticiones[mask_720].groupby('Senders_Reference').agg(agg_functions_2).reset_index()
    combined_row_5_1 = df_segundas_repeticiones[mask_760].groupby('Senders_Reference').agg(agg_functions_2).reset_index()
    df_segundas_repeticiones = pd.concat([combined_row_1_1, combined_row_2_1, combined_row_3_1, combined_row_4_1, combined_row_5_1], ignore_index=True)
    df_filtrado = pd.concat([df_primeras_repeticiones, df_segundas_repeticiones], ignore_index=True)
    df_filtrado = df_filtrado.drop_duplicates()
    df_filtrado = pd.concat([df_filtrado, No_700_df], ignore_index=True)
    columnas_a_excluir = [columna for columna in df_filtrado.columns if 'date' in columna.lower()]
    columnas_a_excluir.extend(['Message_Content','Receivers_Reference','Senders_Reference','UETR','InstructionId','Issuing_Banks_Reference'])
    for columna in df_filtrado.columns:
        if columna not in columnas_a_excluir:
            df_filtrado[columna] = df_filtrado[columna].apply(lambda x: str(x).replace('3/', ' ').replace('2/', ' ').replace('/', ' ').replace('-', '').replace('}', ''))

    return df_filtrado