import os
import numpy as np
import pandas as pd
# Import Module GCS
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime

from app import CARTELLA, SERVICE_ACCOUNT

# SET Cartella download e variabile ambiente 
CARTELLA = CARTELLA
variabile_ambiente = SERVICE_ACCOUNT
client = bigquery.Client.from_service_account_json(variabile_ambiente)
client_storage = storage.Client.from_service_account_json(variabile_ambiente)

# TODO(developer)Inserire nome del bucket di google cloud storage in cui vogliamo inserire i dati 
bucket_name = "dnr_logistica"
#TODO indicare table id su bigquery
table_gas_id = 'pltpg-mubi.TRADING.LibreriaOfferteGAS'
table_ee_id = 'pltpg-mubi.TRADING.LibreriaOfferteEE'


# apri file excel
def lettura_file_libreria_gas(file):
    """
    funzione per leggere file excel
    lo trasforma in csv e poi lo elimina   
    """
    df = pd.read_excel(CARTELLA + "/" + file + ".xlsx", usecols = "A:AA")
    # converto la colonna Inizio_Validit__ e Fine_Validit__
    # Elimina colonne vuote (prendi le colonne interessate)
    df['Inizio_Validit__'] = pd.to_datetime(df['Inizio_Validit__']).dt.date
    df['Fine_Validit__'] =pd.to_datetime(df['Fine_Validit__']).dt.date
    # converto le stesse colonne in fstringa col formato utilizzato
    df['Inizio_Validit__'] = pd.to_datetime(df['Inizio_Validit__']).dt.strftime('%m/%d/%Y')
    df['Fine_Validit__'] =pd.to_datetime(df['Fine_Validit__']).dt.strftime('%m/%d/%Y')
    #print(df)
    # Salvalo in csv
    df.to_csv(CARTELLA + "/" + file + ".csv", sep=';', header=1 , index=False)
    os.remove(CARTELLA + "/" + file + ".xlsx")


def lettura_file_libreria_ee(file):
    """
    funzione per leggere file excel
    lo trasforma in csv e poi lo elimina   
    """
    df = pd.read_excel(CARTELLA + "/" + file + ".xlsx", usecols = "A:AX")
    # Salvalo in csv
    df.to_csv(CARTELLA + "/" + file + ".csv", sep=';', header=1 , index=False)    
    os.remove(CARTELLA + "/" + file + ".xlsx")

def upload_to_bucket_libreria(bucket_name, file_path, file):
    """Funzione per caricare una csv su bucket di google cloud"""
    storage_client = client_storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file)
    blob.upload_from_filename(file_path)
    print(
        "File {} uploaded in {}.".format(
            file_path, file
        )
    )
    os.remove(CARTELLA + "/" + file + ".csv")

def load_job(file, table_id):
    """Funzione per caricare una tabella da google cloud a bigquery"""
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        field_delimiter=";",
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #utilizza "WRITE_APPEND" per aggiungere righe/"WRITE_TRUNCATE" per sovrascriverle
    )
    uri = "gs://"+ bucket_name +"/"+ file
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    ) 
    # Make an API request.
    try:
       # Waits for the job to complete
        load_job.result()
        destination_table = client.get_table(table_id)  # Make an API request.
        righe_caricate = load_job.output_rows
        print(f"sono state caricate {righe_caricate} righe.")
        print(f"in totale ci sono {destination_table.num_rows} righe.")
        print(f"caricato blob {file}")
    except:        
        for e in load_job.errors:
            print('ERRORE: {}'.format(e['message']))

# aggiungi in append i valori
def elimina_file_su_GCS(bucket_name, file):
    """Elimino il file da GCS"""
    storage_client = client_storage
    bucket = storage_client.bucket(bucket_name)
    filepassato = bucket.blob(file)
    filepassato.delete()


def main_upload_librerie():
    lista_file = os.listdir(CARTELLA)
    for excel in lista_file:
        file = excel
        if 'gas' in excel: 
            file = file[:-5]
            #print('gas', file)          
            lettura_file_libreria_gas(file)
            file_path = CARTELLA + "/" + file + ".csv"    
            upload_to_bucket_libreria(bucket_name, file_path, file)
            load_job(file, table_gas_id)
            elimina_file_su_GCS(bucket_name, file)
            
        else:
            file = file[:-5]
            print('ee', file)
            lettura_file_libreria_ee(file)
            file_path = CARTELLA + "/" + file + ".csv" 
            upload_to_bucket_libreria(bucket_name, file_path, file)
            load_job(file, table_ee_id)
            elimina_file_su_GCS(bucket_name, file)
        
        return(f'caricato/i {lista_file} su BigQuery')

