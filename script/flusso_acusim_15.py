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


def main_load_csv():        
    files = os.listdir(CARTELLA)
    filepath = os.path.join(CARTELLA, files[0])
    try:
        # controllo nome file
        if '15' in files[0] and '.csv' in files[0]:    
            df = pd.read_csv(filepath,  sep=';', header=0, low_memory=False, encoding='latin-1', encoding_errors='ignore', dtype=str)        
        else:
            return('file Acusim 15 non trovato')
        
        df.to_gbq(
            destination_table= 'CREDITO_ACUSIM.FlussoAcusim15_PLENITUDE',
            project_id='pltpg-mubi', 
            chunksize=100000, 
            reauth=False, 
            if_exists='replace', 
            progress_bar=True, 
            credentials=credential
        )
        risposta = 'Tabella FlussoAcusim15_PLENITUDE AGGIORNATA'
        os.remove(filepath)
        return risposta
    except:
        os.remove(filepath)
    
    

