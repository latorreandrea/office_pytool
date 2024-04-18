import os
# import libreria di sharepoint
from office365.sharepoint.client_context import ClientContext 
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.files.file import File
# import interazione bigquery
from google.cloud import bigquery
import pandas as pd
from app import CARTELLA

SHAREPOINT_SITE = 'https://domain.sharepoint.com/sites/sitename'  # 'https://pltpuregreen.sharepoint.com/sites/provaapp'#  
SHAREPOINT_SITE_NAME = 'sitename' #'provaapp'
SHAREPOINT_DOC = 'Documenti condivisi/Test' #'Documenti condivisi/inviofilepython'

USERNAME = 'name.lastname@domain.com'
PASSWORD = 'P@ssw0rd3xampl3'

# Segui guida https://learn.microsoft.com/en-us/sharepoint/dev/solution-guidance/security-apponly-azureacs
CLIENT_ID = 'id-cli-ent' 
CLIENT_CREDENTIAL = 'clien-t\creden-tial='



# variabili interazione bigquery
bq_service_account = 'set/path/sa.json'
client = bigquery.Client.from_service_account_json(bq_service_account, location='europe-west4')


def vista_bq(file_path, sql):
    """
    Funzione per ricevere una vista 
    da una query dell'utente
    """
    try:
        query_job = client.query(sql)
    except:
        raise Exception("sei sucuro di avere i permessi per questi dataset?")
    finally:        
        df =  query_job.to_dataframe()  
        for colonna in df.columns.tolist():
            if 'PDR' in colonna:
                df[colonna] = df[colonna].apply(lambda x: '{0:0>14}'.format(x))   
        print(df)  
        df.to_csv(file_path, sep=';', header=1 , index=False)
    

def selezione_tabella_bq(file_path, project, dataset_id, table_name):
    """
    Funzione che seleziona i dati e li carica in un dataframe.
    Salva il dataframe come csv nel file_path indicato
    """
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)
    df = client.list_rows(table).to_dataframe()
    # esempio per gestire colonne con dati numerici:
    #pod che iniziano con lo 0
    for colonna in df.columns.tolist():
        if 'PDR' in colonna:
            df[colonna] = df[colonna].apply(lambda x: '{0:0>14}'.format(x))
    # dati decimali
    #df[['StatoUtenza', 'GPOD_____mese']] = df[['StatoUtenza', 'GPOD_____mese']].astype('float64')    
       
    df.to_csv(file_path, sep=';', header=1 , index=False)


def get_sharepoint_context_using_user():
    """
    funzione che restituisce il token di autenticazione
    accedendo come user
    """
    # Initialize the client credentials
    user_credentials = UserCredential(USERNAME , PASSWORD)
    # create client context object
    ctx = ClientContext(SHAREPOINT_SITE).with_credentials(user_credentials)       
    return ctx


def get_sharepoint_context_using_app():
    """
    fuznione che restituisce il token di autenticazione
    accedendo come applicativo
    """
    # Initialize app
    client_credentials = ClientCredential(CLIENT_ID,CLIENT_CREDENTIAL)
    # create client context object
    ctx = ClientContext(SHAREPOINT_SITE).with_credentials(client_credentials)
    return ctx


def upload_to_sharepoint(file_name):
    """
    funzione che invia il contenuto di un file su sharepoint
    """    
    # ctx = get_sharepoint_context_using_user()    
    ctx = get_sharepoint_context_using_app()
    # print(ctx)
    target_folder_url = f'/sites/{SHAREPOINT_SITE_NAME}/{SHAREPOINT_DOC}'
    target_forder = ctx.web.get_folder_by_server_relative_path(target_folder_url)
    file_path = CARTELLA + '/' + file_name

    with open(file_path, 'rb') as content_file:
        file_content = content_file.read()
    response = target_forder.upload_file(file_name, file_content).execute_query()
    return response


def main(file_name, project, dataset_id, table_name, sql):
    """
    funzione per gestire la logica dello script
    """
    file_path = CARTELLA + "/" + file_name
    if sql == '':        
        selezione_tabella_bq(file_path, project, dataset_id, table_name)
    else:
        vista_bq(file_path, sql)
    upload_to_sharepoint(file_name)
    os.remove(CARTELLA + "/" +  file_name)
    return(f"{file_name} condiviso")

