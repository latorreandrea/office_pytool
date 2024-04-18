import os
import pandas as pd
from google.cloud import bigquery
import datetime
import random
import string
# fase 1 : leggere file excel

# fase 2 : creare per ogni cliente una riga in cui sia presente l'importo da rimborsare.

# fase 3 : rendere disponibile il file per il download
from app import CARTELLA, SERVICE_ACCOUNT


# files = os.listdir(CARTELLA)
# file_input = os.path.join(CARTELLA, files[0])
# file_output = 'RichiestaEmissioneBonificiNonImputati' + datetime.datetime.now().strftime('%y%m%d') + '.txt'
# percorso_file = os.path.join(CARTELLA, file_output) 


#---------------------------------- Funzioni per scrivere file bondom
def filler(n: str):
    """crea spazi"""
    filler = ' ' * n
    return filler


def genera_codice_numerico(lenght_code: int):
    lunghezza_codice = lenght_code
    caratteri_disponibili = string.digits  # Lettere maiuscole, minuscole e cifre  # string.ascii_uppercase + 
    codice = ''.join(random.choice(caratteri_disponibili) for _ in range(lunghezza_codice))
    return str(codice)

def genera_codice_alfanumerico(lenght_code: int):
    lunghezza_codice = lenght_code
    caratteri_disponibili = string.ascii_uppercase + string.digits  # Lettere maiuscole, minuscole e cifre  
    codice = ''.join(random.choice(caratteri_disponibili) for _ in range(lunghezza_codice))
    return str(codice)


def completa_sx(stringa:str, riempitivo:str, lunghezza_desiderata:int):
    """funzione per aggiungere riempitivo a sinistra del valore"""
    diff = lunghezza_desiderata - len(stringa)
    if diff > 0:
        zeri = riempitivo * diff
        return zeri + stringa
    else:
        return stringa

def completa_dx(stringa:str, riempitivo:str, lunghezza_desiderata:int):
    """funzione per aggiungere riempitivo a destra del valore"""
    diff = lunghezza_desiderata - len(stringa)
    if diff > 0:
        zeri = riempitivo * diff
        return  stringa + zeri
    else:
        return stringa


def record_testa(flusso:str, percorso_file):
    """funzione per creare il record di testa"""
    with open(percorso_file, 'w') as file:
        file.write(str(filler(1))) # filler
        file.write('PC14191') # ServiziFinanziari
        file.write('60199') # Ricevente
        file.write(datetime.datetime.now().strftime('%d%m%y'))
        file.write(flusso)
        file.write(str(filler(82)))
        file.write('E') # Codice divisa
        file.write(str(filler(6)))
        file.write('\n')


def conta_righe(percorso_file):
    with open(percorso_file, 'r') as file:
        righe = sum(1 for line in file)
    return righe


def record_coda(flusso:str, disposizioni:str, importo_tot:str, record_file:str, percorso_file):
    """funzione per creare il record di coda"""
    with open(percorso_file, 'a') as file:
        # file.write('\n')
        file.write(str(filler(1)))
        file.write('EF')
        file.write('14191') # Mittente
        file.write('60199') # Ricevente
        file.write(datetime.datetime.now().strftime('%d%m%y')) # data creazione
        file.write(flusso)
        file.write(str(filler(14)))
        file.write(completa_sx(str(disposizioni), '0', 7))
        file.write(str(filler(15)))
        file.write(completa_sx(importo_tot, '0', 15)) #IMPORTO TOTALE
        file.write(completa_sx(record_file, '0', 7))
        file.write(str(filler(24)))
        file.write('E')
        file.write(str(filler(6)))
        # file.write('\n')


def record_10(nr_disposizione:str, importo:str, code_line:str, cf:str, percorso_file):
    """funzione per creare record tipo 10"""
    with open(percorso_file, 'a') as file:
        # file.write('\n')
        file.write(str(filler(1)))
        file.write('10')
        file.write(str(completa_sx(nr_disposizione, '0', 7))) #numero progressivo
        file.write(str(filler(12)))
        file.write(datetime.datetime.now().strftime('%d%m%y'))
        file.write('48000') #bonifici generici
        file.write(completa_sx(importo, '0', 13))
        file.write('+')
        file.write(code_line)
        file.write('07601')
        file.write(str(filler(22)))
        file.write('3')
        file.write(cf)
        file.write(str(filler(6)))
        file.write('E') #codice divisa


def record_20(nr_disposizione:str, percorso_file):
    """funzione per creare record tipo 20"""
    with open(percorso_file, 'a') as file:
        file.write('\n')
        file.write(str(filler(1)))
        file.write('20')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write('Eni Plenitude S.p.A. S.Benefit')
        file.write(str(filler(80)))
        file.write('\n')


def record_30(nr_disposizione:str, denominazione:str, percorso_file):
    """funzione per creare record tipo 30"""
    with open(percorso_file, 'a') as file:
        # file.write('\n')
        file.write(str(filler(1)))
        file.write('30')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(completa_dx(str(denominazione), ' ', 90)))
        file.write(str(filler(20)))
        file.write('\n')


def record_40(nr_disposizione:str, percorso_file):
    """funzione per creare record tipo 40"""
    with open(percorso_file, 'a') as file:
        # file.write('\n')
        file.write(str(filler(1)))
        file.write('40')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(filler(110)))
        file.write('\n')


def record_60(nr_disposizione:str, percorso_file):
    """funzione per creare record tipo 60"""
    with open(percorso_file, 'a') as file:
        # file.write('\n')
        file.write(str(filler(1)))
        file.write('60')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write('SEG160')
        file.write(str(filler(104)))
        file.write('\n')
        file.write(str(filler(1)))
        file.write('60')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write('SEG260')
        file.write(str(filler(104)))
        file.write('\n')


def record_70(nr_disposizione:str, codice_cliente:str, percorso_file):
    """funzione per creare record tipo 70"""
    with open(percorso_file, 'a') as file:
        # file.write('\n')
        file.write(str(filler(1)))
        file.write('70')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(filler(46)))
        file.write(codice_cliente + '00' + str(genera_codice_alfanumerico(20))) # chiave di raccordo ottenuta con COD CLIENTE + CODCASUALE?
        file.write(str(filler(34)))
        file.write('\n')
#---------------------------------- BASE DATI
def cod_cliente_cod_fiscale():
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT)
    query=  """
                SELECT 
                    C.No AS Cliente
                    , COALESCE(C.FiscalCode, C.VATRegistrationNo) AS CF_PIVA
                    , REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        LEFT(UPPER(COALESCE(C.FullName, C.Name)), 90),                                    
                                    'Ù', "U'" ),
                                'Ò', "O'"),
                            'Ì', "I'"),
                        "È", "E'"),
                    "À", "A'") AS Name
                FROM
                    `pltpg-mubi.CREDITO.Customer` C                
            """
    query_job = client.query(query)
    df = query_job.to_dataframe() 
    return df


def lettura_file_excel(file):
    """
    Funzione per leggere il file excel
    """
    df = pd.read_excel(file)
    df['Codice Cliente'] = df['Codice Cliente'].str.upper()
    df = df[['Codice Cliente','Avere']]    
    df = df.groupby('Codice Cliente').agg(Avere=('Avere', 'sum')).reset_index()
    return df


def join_df(df1, df2):
    """
    effettuo il join tra due tabelle sulle colonne 'Codice Cliente' e 'Cliente'
    """
    df = df1.set_index('Codice Cliente').join(df2.set_index('Cliente'), rsuffix='cliente_')
    df = df.reset_index(drop=False)
    df = df.loc[(df['Avere'] >= 10) & (df['Avere'] <= 6000)]
    return df
    

# print(lettura_file_excel(file))
# print(cod_cliente_cod_fiscale())

def creazione_file_bonifico():
    """
    Funzione che gestisce la logica della creazione dei file bondom
    ritorna una lista di file
    """  
    files = os.listdir(CARTELLA)
    file_input = os.path.join(CARTELLA, files[0])
    file_output = 'RichiestaEmissioneBonificiNonImputati' + datetime.datetime.now().strftime('%y%m%d') + '.txt'
    percorso_file = os.path.join(CARTELLA, file_output) 
    flusso = 'BD-'+ genera_codice_numerico(9)                                     
    record_testa(flusso, percorso_file)                                                      
    df = join_df(lettura_file_excel(file_input),cod_cliente_cod_fiscale()) 
    code_line = 'BD-'+ genera_codice_alfanumerico(8) + '-' + str(datetime.datetime.now().strftime('%Y%m%d')) + '  ' 
    for index, row in df.iterrows():
        # dichiaro la disposizione \ l'importo
        nr_disposizione = str(index + 1)
        importo = '{:.2f}'.format(row['Avere']).replace('.', '').replace('-', '')
        # scrivo le rows
        record_10(nr_disposizione, importo, code_line, cf=str(row['CF_PIVA']), percorso_file=percorso_file)
        record_20(nr_disposizione, percorso_file=percorso_file)
        record_30(nr_disposizione, denominazione=row['Name'], percorso_file=percorso_file)
        record_40(nr_disposizione, percorso_file=percorso_file)
        record_60(nr_disposizione, percorso_file=percorso_file)
        record_70(nr_disposizione, codice_cliente=row['Codice Cliente'], percorso_file=percorso_file)
    disposizioni = str(len(df))
    importo_tot = '{:.2f}'.format(df['Avere'].sum()).replace('.', '').replace('-', '')
    record_file = str(conta_righe(percorso_file=percorso_file) + 1)
    record_coda(flusso, disposizioni, importo_tot, record_file, percorso_file=percorso_file)
    os.remove(os.path.join(CARTELLA, file_input))
    return [file_output]


