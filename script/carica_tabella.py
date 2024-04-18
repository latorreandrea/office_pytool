import os
import numpy as np
import pandas as pd
import pandas_gbq
# Import Module GCS
from google.cloud import bigquery
from google.oauth2 import service_account

from app import CARTELLA, SERVICE_ACCOUNT

# SET Cartella download e variabile ambiente 
CARTELLA = CARTELLA
variabile_ambiente = SERVICE_ACCOUNT
credential = service_account.Credentials.from_service_account_file(variabile_ambiente)



def main_carica_csv(progetto, dataset, table_name, file, separatore):
    """
    funzione per caricare un file csv su una tabella bigquery:
    progetto = stringa che identifica il progetto GC
    dataset = stringa che identifica il dataset su BQ
    table_name = stringa che identifica il nome della tabella su BQ
    file = stringa nome completo del file che verrà caricato
    separatore = stringa indica con che valore sono separati i valosri nel file csv
    """
    
    df = pd.read_csv(CARTELLA + "/" + file,  sep=separatore, header=0, low_memory=False, encoding='latin-1', encoding_errors='ignore', dtype=str)    
    # creo oggetto nomi colonne
    coldict = {}
    #creo lista cambio caratteri
    cambio_caratteri = [
        (".",""),("?",""),("(",""),("/",""),#("ï",""), 
        (")",""),("à",""),("è",""),("é",""),#("»",""), 
        ("ù",""),("[",""),("]",""),("+",""),#("¿",""),
        ("#",""),("°",""),("ò",""),("\\",""),
        ('"',""),("'",""),("\xc3\xaf\xc2\xbf\xc2\xbd",""),(" ","_")]
    for col in df.columns:
        new_col = col
        # creo dizionario che associa il vecchio nome della colonna con il nuovo 
        for old, new in cambio_caratteri:
            new_col = new_col.replace(old, new)
        coldict[col] = new_col

    # # remove special character
    # df.columns=df.columns.str.replace('[#,@,&, ,?,(,),à,è,é,ù,§,+,°,ò,/,\,.]','')
    # df.columns=df.columns.str.replace("[']","")
    # df.columns=df.columns.str.replace('["]',"")
    #sostituisco il nome delle colonna
    df = df.rename(columns=coldict)
    print(df)
    # # # for col in df.columns:
    # # #     # trasformare i valori PDR in stringa
    # # #     if 'PDR' in col or 'pdr' in col:
    # # #         df[col] = df[col].astype(str)            
    # # #         # calcolare gli 0 mancanti nel codice pdr per ogni riga
    # # #         # e aggiungerli al relativo valore
    # # #         for i in range(len(df[col])):
    # # #             missing_zeroes = 14 - len(df.loc[i, col]) 
    # # #             prefix = ''      
    # # #             for x in range(missing_zeroes):                    
    # # #                 prefix = prefix + '0'
    # # #             df.loc[i, col] = prefix + df.loc[i, col]
    # invio di tabella su big query 
    try:   
        df.to_gbq(
            destination_table= dataset + "." + table_name,
            project_id=progetto, 
            chunksize=100000, 
            reauth=False, 
            if_exists='fail', 
            progress_bar=True, 
            credentials=credential
        )
        return('Caricamento riuscito')
    except Exception as e:
        return('Errore: ', str(e))
        

   


