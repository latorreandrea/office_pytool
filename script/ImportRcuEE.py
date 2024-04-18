from http.client import BAD_REQUEST
import os
from zipfile import ZipFile
from grpc import compute_engine_channel_credentials
import numpy as np
import pandas as pd
# Import Module GCS
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
from app import CARTELLA, SERVICE_ACCOUNT, BUCKET_NAME


CARTELLA = CARTELLA
variabile_ambiente = SERVICE_ACCOUNT
client = bigquery.Client.from_service_account_json(variabile_ambiente)
client_storage = storage.Client.from_service_account_json(variabile_ambiente) 
bucket_name = BUCKET_NAME

#TODO indicare table id su bigquery
table_id = 'pltpg-mubi.TRADING.RcuEE' 


def trova_AnnoMese_recuee(CARTELLA):
    "questa funzione crea un dizionario Anno:Mese"
    Anno = 0
    Mese = 0
    for file in os.listdir(CARTELLA):
        if ("REP_ELE" in file):
            Anno=int(file[23:27])
            Mese=int(file[27:29])
    return Anno, Mese


def pulizia_dati_nella_colonna_recuee(df, titolocolonna):
    """
    la funzione pulisce i dati di una colonna(titolocolnna)
    della tabella pandas indicata(df)
    """
    print("inizio modifica")
    # trasformo le "O" in 0    
    for x in df.index:
        try:                        
            if 'o' in df.loc[x, titolocolonna]:
                print("trovato")
                valorestringa=df.loc[x, titolocolonna]
                df.loc[x, titolocolonna] = valorestringa.replace('o','0')
                print(f"riga {x} del df  sostituita: {df.loc[x, titolocolonna]}")
            elif 'O' in df.loc[x, titolocolonna]:
                print("trovato")
                valorestringa=df.loc[x, titolocolonna]
                df.loc[x, titolocolonna] = valorestringa.replace('O','0')
                print(f"riga {x} del df  sostituita: {df.loc[x, titolocolonna]}")            
        except:
            print("valore null")
            continue 


def unzip_files_recuee(CARTELLA):
    """
    questa funzione continua a dare il valore unzippa fino
    a quando trova file da unzippare nella cartella
    """
    for file in os.listdir(CARTELLA):
        if (".zip" in file) or (".ZIP" in file):            
            with ZipFile(CARTELLA + "/" + file, 'r') as zipFile:
                zipFile.extractall(CARTELLA)
            os.remove(CARTELLA + "/"+ file)
    for file in os.listdir(CARTELLA):
        if (".zip" in file) or (".ZIP" in file):
            return "unzippa"
            #unzip_files(CARTELLA)
        else:
            print("non ci sono altri file zip")
            return "finito"



def lettura_file_recuee(file, Anno, Mese):
    """
    il file viene letto e trasformato
    """
    
    df = pd.read_csv(CARTELLA + "/" + file, sep=';', header=0, encoding = "ISO-8859-1", engine='c', decimal='.', low_memory=False, on_bad_lines='warn', dtype={'CF': str, 'PIVA': str, 'PIVA_DISTR': str, 'PIVA_UDD': str, 'PIVA_CC': str, 'TELEFONO_CLIENTE': str, 'MAT_MISURATORE_ATT': str, 'MAT_MISURATORE_REA': str, 'MAT_MISURATORE_POT': str, 'SETT_MERCEOLOGICO': str, 'COD_OFFERTA': str})
    df = df.fillna(np.nan).replace([np.nan], [None])
    
    df['COD_POD']=df['COD_POD'].str.slice(stop=14)    
    oggi = datetime.now()
    df['DateUp'] = oggi
    df['Anno'] = Anno
    df['Mese'] = Mese
    # trasformo tutte le date in formato compatibile per bigquery      
    colonnedflavoro = ['Anno', 'Mese', 'COD_POD', 'AREA_RIF', 'RAGIONE_SOCIALE_DISTR', 'PIVA_DISTR', 'DP', 'RAGIONE_SOCIALE_UDD',
     'PIVA_UDD', 'RAGIONE_SOCIALE_CC', 'PIVA_CC', 'TIPO_MERCATO', 'TIPO_POD', 'FINE_TIPO_POD', 'DATA_INIZIO_FORNITURA',
     'DATA_FINE_FORNITURA', 'DATA_DECORRENZA_RET', 'DATA_INIZIO_DISPACCIAMENTO', 'CF', 'PIVA', 'CF_STRANIERO', 'NOME', 
     'COGNOME', 'RAGIONE_SOCIALE_DENOMINAZIONE', 'EMAIL_REFERENTE', 'TELEFONO_CLIENTE', 'EMAIL_CLIENTE', 'RESIDENZA', 
     'SERVIZIO_TUTELA', 'SETT_MERCEOLOGICO', 'COD_OFFERTA', 'AUTOCERTIFICAZIONE', 'CODICE_UFFICIO', 'PAGAMENTO_IVA', 
     'IVA', 'IMPOSTE', 'TENSIONE', 'DISALIMENTABILITA', 'TARIFFA_DISTRIBUZIONE', 'TIPO_MISURATORE', 'DATA_MESSA_REGIME', 
     'MOTIVAZIONE', 'POTCONTRIMP', 'POTDISP', 'CONSUMO', 'TRATTAMENTO', 'TRATTAMENTO_SUCC', 'NUM_CIFRE_EA', 'NUM_CIFRE_ER', 
     'K_TRASFOR_ATT', 'K_TRASFOR_REA', 'K_TRASFOR_POT', 'MAT_MISURATORE_ATT', 'MAT_MISURATORE_REA', 'MAT_MISURATORE_POT', 
     'INST_MISURATOR_ATT', 'INST_MISURATOR_REA', 'INST_MISURATOR_POT', 'NUM_CIFRE_ATT', 'NUM_CIFRE_REA', 'NUM_CIFRE_POT', 
     'PRESENZA_MIS', 'GEST_FORFAIT', 'PMA', 'REGIME_COMPENSAZIONE', 'BF_DATA_INIZIO', 'BF_DATA_FINE', 'BF_DATA_RINNOVO', 
     'BE_ANNO_VALIDITA', 'BE_DATA_INIZIO', 'BE_DATA_FINE', 'BE_DATA_CESSAZIONE', 'DateUp']
    dfbase = df[colonnedflavoro]

    # specifico la tipologia di dati che contiene ogni colonna
    
    for titolo in colonnedflavoro:
        if 'DATA' in titolo or 'INST_MISURATOR_' in titolo:
            dfbase[titolo]=pd.to_datetime(dfbase[titolo], dayfirst=True).dt.strftime('%Y-%m-%d')            
        elif 'K_TRASFOR_' in titolo:            
            #dfbase[titolo]=dfbase[titolo].astype('Int64')         
            #dfbase[[titolo]] = dfbase[[titolo]].apply(pd.to_numeric, errors='coerce')
            #dfbase[titolo]=dfbase[titolo].astype('Int64')
            dfbase[titolo] = np.floor(pd.to_numeric(dfbase[titolo], errors='coerce')).astype('Int64')
            
        elif 'NUM_CIFRE' in titolo:            
            #dfbase[titolo]=dfbase[titolo].astype('Int64')
            #dfbase[[titolo]] = dfbase[[titolo]].apply(pd.to_numeric, errors='coerce')
            #dfbase[titolo]=dfbase[titolo].astype('Int64')
            dfbase[titolo] = np.floor(pd.to_numeric(dfbase[titolo], errors='coerce')).astype('Int64')

    dfbase['FINE_TIPO_POD']=pd.to_datetime(dfbase['FINE_TIPO_POD'], dayfirst=True).dt.strftime('%Y-%m-%d')
    dfbase['PMA']=dfbase['PMA'].astype('float')
    dfbase['PMA']=round(dfbase['PMA'],3)

    # riordino le colonne
    dfbase = dfbase[['Anno', 'Mese', 'COD_POD', 'AREA_RIF', 'RAGIONE_SOCIALE_DISTR', 'PIVA_DISTR', 'DP', 'RAGIONE_SOCIALE_UDD',
     'PIVA_UDD', 'RAGIONE_SOCIALE_CC', 'PIVA_CC', 'TIPO_MERCATO', 'TIPO_POD', 'FINE_TIPO_POD', 'DATA_INIZIO_FORNITURA',
     'DATA_FINE_FORNITURA', 'DATA_DECORRENZA_RET', 'DATA_INIZIO_DISPACCIAMENTO', 'CF', 'PIVA', 'CF_STRANIERO', 'NOME', 
     'COGNOME', 'RAGIONE_SOCIALE_DENOMINAZIONE', 'EMAIL_REFERENTE', 'TELEFONO_CLIENTE', 'EMAIL_CLIENTE', 'RESIDENZA', 
     'SERVIZIO_TUTELA', 'SETT_MERCEOLOGICO', 'COD_OFFERTA', 'AUTOCERTIFICAZIONE', 'CODICE_UFFICIO', 'PAGAMENTO_IVA', 
     'IVA', 'IMPOSTE', 'TENSIONE', 'DISALIMENTABILITA', 'TARIFFA_DISTRIBUZIONE', 'TIPO_MISURATORE', 'DATA_MESSA_REGIME', 
     'MOTIVAZIONE', 'POTCONTRIMP', 'POTDISP', 'CONSUMO', 'TRATTAMENTO', 'TRATTAMENTO_SUCC', 'NUM_CIFRE_EA', 'NUM_CIFRE_ER', 
     'K_TRASFOR_ATT', 'K_TRASFOR_REA', 'K_TRASFOR_POT', 'MAT_MISURATORE_ATT', 'MAT_MISURATORE_REA', 'MAT_MISURATORE_POT', 
     'INST_MISURATOR_ATT', 'INST_MISURATOR_REA', 'INST_MISURATOR_POT', 'NUM_CIFRE_ATT', 'NUM_CIFRE_REA', 'NUM_CIFRE_POT', 
     'PRESENZA_MIS', 'GEST_FORFAIT', 'PMA', 'REGIME_COMPENSAZIONE', 'BF_DATA_INIZIO', 'BF_DATA_FINE', 'BF_DATA_RINNOVO', 
     'BE_ANNO_VALIDITA', 'BE_DATA_INIZIO', 'BE_DATA_FINE', 'BE_DATA_CESSAZIONE', 'DateUp']]

    dfbase.to_csv(CARTELLA + "/"+ file, sep=';', header=1, index=False, encoding = "ISO-8859-1", decimal='.')
    


def upload_to_bucket_recuee(bucket_name, file_path, destination_blob_name):
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


def elimina_file_su_GCS_recuee(bucket_name, filepassato):
    """Elimino il file da GCS"""
    storage_client = client_storage
    bucket = storage_client.bucket(bucket_name)
    filepassato = bucket.blob(filepassato)
    filepassato.delete()


def load_job_bigquery_recuee(destination_blob_name, table_id):
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
    except :        
        print('ERRORE nel caricamento su bigquery')

    destination_table = client.get_table(table_id)  # Make an API request.
    righe_caricate = load_job.output_rows
    print(f"sono state caricate {righe_caricate} righe.")
    print(f"in totale ci sono {destination_table.num_rows} righe.")
    print(f"caricato blob {destination_blob_name}")
    elimina_file_su_GCS_recuee(bucket_name, destination_blob_name)


def mainRcuEE():
    job = []
    # trovovalori anno/mese, questi valori 
    # verranno utilizzati per popolare la tabella di bigquery
    Anno = trova_AnnoMese_recuee(CARTELLA)[0]
    Mese = trova_AnnoMese_recuee(CARTELLA)[1]
    while unzip_files_recuee(CARTELLA) == "unzippa":
        unzip_files_recuee(CARTELLA)
    for file in os.listdir(CARTELLA):
        print(file)
        lettura_file_recuee(file, Anno, Mese)
        file_path = CARTELLA + "/" + file
        destination_blob_name = file[:-4]
        upload_to_bucket_recuee(bucket_name, file_path, destination_blob_name)       
        load_job_bigquery_recuee(destination_blob_name, table_id)
        job.extend([f'{file} aggiunto a bigquery']) 
    return job
