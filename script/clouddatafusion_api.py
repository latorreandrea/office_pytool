import os
import requests
import json


METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/'
METADATA_HEADERS = {'Metadata-Flavor':'Google'}
CDAP_ENDPOINT = 'https://pltpg-mubi-prod-v672-pltpg-mubi-dot-euw4.datafusion.googleusercontent.com/api'
SERVICE_ACCOUNT = 'default'


def create_token():

    """
    funzione che ricava token di accesso al google cloud project dando il service account
    """
    url = f'{METADATA_URL}instance/service-accounts/{SERVICE_ACCOUNT}/token'
    r = requests.get(url, headers=METADATA_HEADERS)
    print("Ricezione token")
    print(r.raise_for_status())
    access_token = r.json()['access_token']
    return access_token


    # output_stream = os.popen('gcloud auth activate-service-account --key-file /var/www/html/flaskapp/variabiliambiente/sapltgmubi.json')       
    # token = os.popen('gcloud auth print-access-token')
    # token = token.read()
    # # elimino il /n generato con la lettura del terminale
    # return token[:-4]
    # Request an access token from the metadata server.
	
	

   
	
def get_pipeline_status(pipeline_name, token):
    """
    funzione per ricevere lo stato dell ultimo avvio della pipeline.
    Richiede come parametri:
    pipeline_name = stringa che indica il nome della pipeline
    token = stringa ricevuta dalla funzione create_token
    """
    url = CDAP_ENDPOINT + '/v3/namespaces/default/apps/' + pipeline_name + '/workflows/DataPipelineWorkflow/runs'    
    #token = create_token()    
    headers = { 'Authorization': 'Bearer ' + token }
    response = requests.request("GET", url, headers=headers, data={})
    if response.status_code == 401:
        print(token)
        print(response)
        return('Non è possibile contattare le API DataFusion')
    response = response.json()
    return(response[0]['status'])

def start_pipeline(pipeline_name, token):
    """
    funzione per avviare una pipeline.
    Richiede come parametri:
    pipeline_name = stringa che indica il nome della pipeline
    token = stringa ricevuta dalla funzione create_token
    """
    url = CDAP_ENDPOINT + '/v3/namespaces/default/apps/' + pipeline_name + '/workflows/DataPipelineWorkflow/start'    
    headers = { 'Authorization': 'Bearer ' + token }
    response = requests.request("POST", url, headers=headers, data={})
    response = response.json()
    return(response , f"{pipeline_name} è stata avviata")


def stop_pipeline(pipeline_name, token):
    """
    funzione per avviare una pipeline.
    Richiede come parametri:
    pipeline_name = stringa che indica il nome della pipeline
    token = stringa ricevuta dalla funzione create_token
    """
    url = CDAP_ENDPOINT + '/v3/namespaces/default/apps/' + pipeline_name + '/workflows/DataPipelineWorkflow/stop'    
    headers = { 'Authorization': 'Bearer ' + token }
    response = requests.request("POST", url, headers=headers, data={})
    response = response.json()
    return(response , f"{pipeline_name} è stata avviata")




