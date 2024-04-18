import os
import numpy as np
import pandas as pd
# Import Module GCS
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
# Import calendar per trovare l'ultima domenica di marzo e l'ultima di ottobre
import calendar
from zipfile import ZipFile
from app import CARTELLA, SERVICE_ACCOUNT, BUCKET_NAME

CARTELLA = CARTELLA
variabile_ambiente = SERVICE_ACCOUNT
client = bigquery.Client.from_service_account_json(variabile_ambiente)
client_storage = storage.Client.from_service_account_json(variabile_ambiente)
# TODO(developer)Inserire nome del bucket di google cloud storage in cui vogliamo inserire i dati 
bucket_name = BUCKET_NAME
#TODO indicare table id su bigquery
table_id = 'pltpg-mubi.TRADING.AnagraficheEE'


def lettura_file_anagrafiche(file):
    """
    il file viene letto e trasformato
    """
    mese = int(file[4:6])+1
    anno = int(file[0:4])
    if mese==13:
        mese = 1
        anno = anno + 1
    df_partenza = pd.read_csv(CARTELLA + '/' + file,  sep=';', header=0, low_memory=False)
    # prendo dalla prima riga della tabella le info inerenti
    # zona, p.iva, ragione_sociale     
    zona = df_partenza.columns.values[1]
    piva_distributore = df_partenza.columns.values[-1]
    ragione_sociale_distributore = df_partenza.columns.values[0]
    print(len(df_partenza.index))
    if len(df_partenza.index) > 1:
        #gli altri dati verranno presi dalla lettura della tabella generata dalla seconda riga del file csv
        df = pd.read_csv(CARTELLA + '/' + file,  sep=';', header=1, low_memory=False)
        DateUp = datetime.now()
        df['DateUp'] = DateUp    
        #trasformo i nomi delle colonne della tabella appena letta
        df.rename(columns={df.columns[4]: 'Trattamento_m1', df.columns[5]: 'Trattamento_m'},inplace=True)
        for titolo in df.columns.values:
            if 'F1' in titolo and 'CONSUMO' not in titolo:
                rename = 'F1_'+titolo[-2]+titolo[-1]            
                df.rename(columns={titolo: rename},inplace=True)
                df[rename] = df[rename].astype('float64', copy=False)
            elif 'F2' in titolo and 'CONSUMO' not in titolo:
                rename = 'F2_'+titolo[-2]+titolo[-1]            
                df.rename(columns={titolo: rename},inplace=True)
                df[rename] = df[rename].astype('float64', copy=False)
            elif 'F3' in titolo and 'CONSUMO' not in titolo:
                rename = 'F3_'+titolo[-2]+titolo[-1]            
                df.rename(columns={titolo: rename},inplace=True)
                df[rename] = df[rename].astype('float64', copy=False)
            elif 'CONSUMO' in titolo:
                df[titolo].replace(",", ".", inplace=True, regex=True)
                df[titolo] = df[titolo].astype('float64', copy=False)    
        # elimino le colonne superflue 
        df.drop(['REGIME_COMPENSAZIONE', 'DISALIMENTABILITA', 'DATA_INIZIO_BONUS', 'DATA_TERMINE_BONUS', 'MESE_RINNOVO', 'ALTRE_COMUNICAZIONI'], axis=1)
        
        # del df['REGIME_COMPENSAZIONE']
        # del df['DISALIMENTABILITA']
        # del df['DATA_INIZIO_BONUS']
        # del df['DATA_TERMINE_BONUS']
        # del df['MESE_RINNOVO']
        # del df['ALTRE_COMUNICAZIONI']
    # creo le colonne di valori costanti Anno Mese Zona P.iva_distributore e Ragione sociale
    else:
        df = df_partenza
    df['Anno'] = anno
    df['Anno'] = df['Anno'].astype('int32', copy=False)
    df['Mese'] = mese
    df['Mese'] = df['Mese'].astype('int32', copy=False)
    df['Piva_Distributore'] = piva_distributore
    df['Piva_Distributore'] = df['Piva_Distributore'].astype('string', copy=False)
    df['Ragione_Sociale_Distributore'] = ragione_sociale_distributore
    df['Ragione_Sociale_Distributore'] = df['Ragione_Sociale_Distributore'].astype('string', copy=False)
    df['Zona'] = zona
    df['Zona'] = df['Zona'].astype('string', copy=False)
    
    # limitiamo caratteri POD a 14
    for value in df.index:
        if len(df.loc[value, 'POD']) > 14:
            df.loc[value, 'POD'] = df.loc[value, 'POD'][0:14]
    # rioridino le colonne
    df = df[['Anno','Mese','Piva_Distributore','Ragione_Sociale_Distributore', 'Zona','POD','CF','PIVA','Tipo_Misuratore','Trattamento_m1',
            'Trattamento_m','F1_06','F2_06','F3_06','F1_07','F2_07','F3_07','F1_08','F2_08','F3_08','F1_09','F2_09','F3_09','F1_10',
            'F2_10','F3_10','F1_11','F2_11','F3_11','F1_12','F2_12','F3_12','F1_01','F2_01','F3_01','F1_02','F2_02','F3_02','F1_03',
            'F2_03','F3_03','F1_04','F2_04','F3_04','F1_05','F2_05','F3_05','CONSUMO_TOT','CONSUMO_F1','CONSUMO_F2','CONSUMO_F3', 'DateUp']]
    #genero il csv da inviare
    df.to_csv(CARTELLA + "/" + file, sep=';', header=1 , index=False)

def upload_to_bucket_anagrafiche(bucket_name, file_path, destination_blob_name):
    """Funzione per caricare una tabella su bucket di google cloud"""    
    storage_client = client_storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    print(
        "File {} uploaded in {}.".format(
            file_path, destination_blob_name
        )
    )
    os.remove(file_path)


def elimina_file_su_GCS_anagrafiche(bucket_name, filepassato):
    """Elimino il file da GCS"""
    storage_client = client_storage
    bucket = storage_client.bucket(bucket_name)
    filepassato = bucket.blob(filepassato)
    filepassato.delete()


def load_job_bigquery_anagrafiche(destination_blob_name, table_id):
    """Funzione per caricare una tabella da google cloud a bigquery"""
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        skip_leading_rows=1,
        field_delimiter=";",
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND #utilizza "WRITE_APPEND" per aggiungere righe/"WRITE_TRUNCATE" per sovrascriverle
    )
    uri = "gs://"+ bucket_name +"/"+ destination_blob_name
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    ) 
    # Make an API request.
    try:
        # Waits for the job to complete
        load_job.result()        
    except:        
        for e in load_job.errors:
            print('ERRORE: {}'.format(e['message']))

    destination_table = client.get_table(table_id)  # Make an API request.
    righe_caricate = load_job.output_rows
    print(f"sono state caricate {righe_caricate} righe.")
    print(f"in totale ci sono {destination_table.num_rows} righe.")
    print(f"caricato blob {destination_blob_name}")
    elimina_file_su_GCS_anagrafiche(bucket_name, destination_blob_name)   


def main_anagrafiche_EE():
    job = []
    for file in os.listdir(CARTELLA):
        if (".zip" in file) or (".ZIP" in file):            
            with ZipFile(CARTELLA + "/" + file, 'r') as zipFile:
                zipFile.extractall(CARTELLA)
            os.remove(CARTELLA + "/"+ file)
    for file in os.listdir(CARTELLA):
        lettura_file_anagrafiche(file)
        file_path = CARTELLA + "/" + file
        destination_blob_name = file[:-4]
        upload_to_bucket_anagrafiche(bucket_name, file_path, destination_blob_name)       
        load_job_bigquery_anagrafiche(destination_blob_name, table_id)
        #elimina_file_su_GCS_anagrafiche(bucket_name, destination_blob_name)
        job.extend([f'{file} aggiunto a bigquery'])
    return job


