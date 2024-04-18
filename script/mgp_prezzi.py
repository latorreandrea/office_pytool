from http.client import BAD_REQUEST
import os
from ftplib import FTP
# Import Module GCS
from google.cloud import storage
from google.cloud import bigquery
# Time Module
import time
from datetime import datetime, date, timedelta
# import costanti da app
from app import CARTELLA, SERVICE_ACCOUNT, BUCKET_NAME
# import da pandas
import pandas as pd



CARTELLA = CARTELLA
variabile_ambiente = SERVICE_ACCOUNT
client = bigquery.Client.from_service_account_json(variabile_ambiente)
client_storage = storage.Client.from_service_account_json(variabile_ambiente) 
bucket_name = BUCKET_NAME


def selezionatore_date_me(stringadatainizio, stringadatafine, lista_file):
    """
    questa funzione crea i nomi dei file che ci 
    interessa scaricare dalla connessione ftp 
    """
    file_da_aggiornare=[]
    annoinizio=int(stringadatainizio[0:4])            
    meseinizio=int(stringadatainizio[4:6])
    giornoinizio=int(stringadatainizio[6:8])
    annofine=int(stringadatafine[0:4])
    mesefine=int(stringadatafine[4:6])
    giornofine=int(stringadatafine[6:8])    
    data_inizio = date(annoinizio, meseinizio, giornoinizio)
    data_fine = date(annofine, mesefine, giornofine)
    delta = data_fine - data_inizio   # timedelta
    for i in range(delta.days + 1):
        day = str(data_inizio + timedelta(days=i))
        file_giorno = day.replace("-","") + "MGPPrezzi.xml"
        if file_giorno in lista_file:
            file_da_aggiornare.append(file_giorno)
        else:
            pass#print('questa data non è stata trovata')
    return file_da_aggiornare


def connessione_ftp(stringadatainizio, stringadatafine):    
    """
    funzione per connettersi in ftp a MGP
    e ricavare la lista dei file da scaricare
    """
    HOSTNAME = 'download.mercatoelettrico.org'
    USERNAME = 'EMMAGODANI'
    PASSWORD = 'I15A12O4'
    PERCORSO = '/MercatiElettrici/MGP_Prezzi'
    # Connect FTP Server
    ftp = FTP(HOSTNAME, timeout=1000)
    ftp.login(USERNAME, PASSWORD)
    #entro nella cartella
    ftp.cwd(PERCORSO)
    #ottengo lista dei file all'interno
    lista_file = ftp.nlst()    
    file_da_aggiornare = selezionatore_date_me(stringadatainizio, stringadatafine, lista_file)
    # inizio download dei file
    for file in file_da_aggiornare:

        with open(CARTELLA + '/' + file, "wb") as filename:
            # use FTP's RETR command to download the file
            
            ftp.retrbinary(f"RETR {file}", filename.write)  
    ftp.quit()


def ultima_data_aggiornamento_me():
    """
        Funzione per indicare l'ultima data degli aggiornamenti
    """
    #TODO indicare l'id del dataset in BQ e l'id tabella    
    job_config = bigquery.QueryJobConfig()        
    sql = """
        SELECT MAX(Data)
        FROM `pltpg-mubi.TRADING.MGPPrezzi`;
    """
    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location="europe-west4",
        job_config=job_config,
    )  # API request - starts the query

    data = query_job.result()  # Waits for the query to finish
    for item in data:        
        return(f"{item[0]}")


def da_xml_a_csv_me(file: str):
    """
    file che trasforma il singolo xml in csv
    """
    #identifico il giorno in cui avvio il programma    
    oggi = datetime.now()
    #componiamo il dataframe:
    df = pd.read_xml(CARTELLA + '/' + file)    
    # elimino colonne superflue
    del df['id']
    #del df['{http://www.w3.org/2001/XMLSchema}element']
    del df['Mercato']    
    list_vars = list(df.columns)
    list_vars.remove('Data')
    list_vars.remove('Ora')
    df_melt=pd.melt(df, id_vars=['Data','Ora'],
                    value_vars=list_vars,
                    var_name='Zona', value_name='Prezzo',
                    )
    #elimino rows con valori Null:
    df_melt = df_melt.dropna(axis=0, how='any', inplace=False)
    #cambio il tipo di dati
    df_melt['Data']= pd.to_datetime(df_melt['Data'], format='%Y%m%d')
    df=df_melt
    df= df.astype({'Ora':'int'})
    df= df.astype({'Zona':'str'})
    df= df.astype({'Prezzo':'str'})
    df["Prezzo"]=df["Prezzo"].str.replace(',','.')
    df= df.astype({'Prezzo':'float'})
    df['DateUp'] = oggi
    #riordino le colonne    
    df.to_csv(CARTELLA + '/' + file[:-4]+".csv", sep=';', index=False, header=False)    
    os.remove(CARTELLA + '/' + file)    


def load_job_bigquery_me(destination_blob_name:str, table_id:str):
    """Funzione per caricare una tabella da google cloud a bigquery"""
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        skip_leading_rows=0,
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
    except Exception as e:
        print(f"errore: {str(e)}")


    # destination_table = client.get_table(table_id)  # Make an API request.
    # righe_caricate = load_job.output_rows
    # print(f"sono state caricate {righe_caricate} righe.")
    # print(f"in totale ci sono {destination_table.num_rows} righe.")
    # print(f"caricato blob {destination_blob_name}")


def libera_bucket_me():
    """ Funzione per eliminare i file da google storage """    
    directory_name = ''
    
    client = client_storage 
    bucket = client.get_bucket(bucket_name)
    # list all objects in the directory
    blobs = bucket.list_blobs(prefix=directory_name)
    for blob in blobs:
        blob.delete()
    


def upload_to_bucket_me(bucket_name:str, file:str, destination_blob_name:str):
    """Funzione per caricare un csv su bucket di google cloud""" 
    
    storage_client = client_storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file)


def main_MGP(stringadatainizio, stringadatafine):
    """
    gestisce logica dello script
    """
    table_id = 'pltpg-mubi.TRADING.MGPPrezzi'
    connessione_ftp(stringadatainizio, stringadatafine)
    for file in os.listdir(CARTELLA):
        da_xml_a_csv_me(file)
    for file in os.listdir(CARTELLA):
        upload_to_bucket_me(bucket_name, CARTELLA + '/' + file, file[:-4])
        load_job_bigquery_me(file[:-4], table_id)        
        os.remove(CARTELLA + '/' + file) 
    libera_bucket_me()
    return
   

    