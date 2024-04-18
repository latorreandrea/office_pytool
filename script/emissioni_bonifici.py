# lo script serve per creare un file testo per inviare i bonifici
import datetime
import os
import random
import string
from google.cloud import bigquery
import pandas as pd
# 2) Creazione file invio bonifici:

file = 'RichiestaEmissioneBonificiOrdinari' + datetime.datetime.now().strftime('%y%m%d') + '.txt'
cartella = '/home/a.latorre/download_test'
#cartella = '/var/www/html/flaskapp/downloads'
percorso_file = os.path.join(cartella, file)
# CREAZIONE VARIABILI PER FILE TESTO

def filler(n: str):
    """crea spazi"""
    filler = ' ' * n
    return filler

def genera_codice(lenght_code: int):
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


def record_testa(flusso:str):
    """funzione per creare il record di testa"""
    with open(percorso_file, 'w') as file:
        file.write(str(filler(1))) # filler
        file.write('PC14191') # ServiziFinanziari
        file.write('60199') # Ricevente
        file.write(datetime.datetime.now().strftime('%d%m%y'))
        file.write(flusso)
        file.write(str(filler(89)))
        file.write('E') # Codice divisa
        file.write(str(filler(6)))


def conta_righe():
    with open(percorso_file, 'r') as file:
        righe = sum(1 for line in file)
    return righe


def record_coda(flusso:str, disposizioni:str, importo_tot:str, record_file:str):
    """funzione per creare il record di coda"""
    with open(percorso_file, 'a') as file:
        file.write('\n')
        file.write(str(filler(1)))
        file.write('EF')
        file.write('14191') # Mittente
        file.write('60199') # Ricevente
        file.write(datetime.datetime.now().strftime('%d%m%y')) # data creazione
        file.write(flusso)
        file.write(str(filler(21)))
        file.write(completa_sx(str(disposizioni), '0', 7))
        file.write(str(filler(15)))
        file.write(completa_sx(importo_tot, '0', 15)) #IMPORTO TOTALE
        file.write(completa_sx(record_file, '0', 7))
        file.write(str(filler(24)))
        file.write('E')
        file.write(str(filler(6)))


def record_10(nr_disposizione:str, importo:str, code_line:str, cf:str):
    """funzione per creare record tipo 10"""
    with open(percorso_file, 'a') as file:
        file.write('\n')
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


def record_20(nr_disposizione:str):
    """funzione per creare record tipo 20"""
    with open(percorso_file, 'a') as file:
        file.write('\n')
        file.write(str(filler(1)))
        file.write('20')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(filler(110)))


def record_30(nr_disposizione:str):
    """funzione per creare record tipo 30"""
    with open(percorso_file, 'a') as file:
        file.write('\n')
        file.write(str(filler(1)))
        file.write('30')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(filler(110)))


def record_40(nr_disposizione:str):
    """funzione per creare record tipo 40"""
    with open(percorso_file, 'a') as file:
        file.write('\n')
        file.write(str(filler(1)))
        file.write('40')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(filler(110)))


def record_60(nr_disposizione:str):
    """funzione per creare record tipo 60"""
    with open(percorso_file, 'a') as file:
        file.write('\n')
        file.write(str(filler(1)))
        file.write('60')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write('SEG160')
        file.write(str(filler(100)))
        file.write('\n')
        file.write(str(filler(1)))
        file.write('60')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write('SEG260')
        file.write(str(filler(100)))


def record_70(nr_disposizione:str):
    """funzione per creare record tipo 70"""
    with open(percorso_file, 'a') as file:
        file.write('\n')
        file.write(str(filler(1)))
        file.write('70')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(filler(46)))
        file.write(str(genera_codice(30)))
        file.write(str(filler(34)))


def creazione_lista_clienti():
    client = bigquery.Client.from_service_account_json(SERVICEACCOUNT.json)
    query = """
                SELECT 
                    CA.No AS Fornitura
                    , SF.SaldoFornitura
                    , CA.CustomerNo AS Cliente
                    , SC.SaldoCliente
                    , C.FiscalCode AS CF 
                    , C.FullName AS Denominazione
                FROM 
                    `pltpg-mubi.CREDITO.CustomerAccount` CA
                JOIN 
                    `pltpg-mubi.CREDITO.Customer` C 
                ON 
                    C.No = CA.CustomerNo
                LEFT JOIN
                (
                    SELECT 
                        CLE.CustomerNo AS Nrcliente
                        ,SUM((SELECT SUM(dcle.Amount)
                        FROM `pltpg-mubi.CREDITO.DetailedCustLedgEntry` dcle
                        WHERE        dcle.CustLedgerEntryNo = CLE.EntryNo)) AS SaldoCliente
                    FROM 
                        `pltpg-mubi.CREDITO.CustLedgerEntry` AS CLE 
                    GROUP BY 
                        CLE.CustomerNo
                ) SC
                ON 
                    CA.CustomerNo = SC.Nrcliente
                LEFT JOIN
                    (
                        SELECT 
                        CLE.CustomerAccountNo AS Utenza, 
                        SUM((SELECT SUM(dcle.Amount)
                            FROM `pltpg-mubi.CREDITO.DetailedCustLedgEntry` dcle
                            WHERE        dcle.CustLedgerEntryNo = CLE.EntryNo)) AS SaldoFornitura
                        FROM `pltpg-mubi.CREDITO.CustLedgerEntry` AS CLE
                        GROUP BY CLE.CustomerAccountNo
                    )SF
                ON 
                    CA.No = SF.Utenza
                WHERE 
                    CA.AccountStatus = 1 
                AND 
                    (SaldoFornitura < -10 AND SaldoFornitura >= -6000)
                AND 
                    SaldoCliente < 0
                AND 
                    (C.CreateEnrichedXML = 0 AND C.IndividualPerson = 1)
                ORDER BY CA.CustomerNo
            """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    df['SaldoCliente'] = df['SaldoCliente'].astype(float).round(2)
    df['SaldoFornitura'] = df['SaldoFornitura'].astype(float).round(2)
    
    return df


def creazione_file():
    flusso = genera_codice(5)                                      
    record_testa(flusso)  
                                                    
    df = creazione_lista_clienti()
    
    for index, row in df.iterrows():
        # dichiaro la disposizione \ l'importo
        nr_disposizione = str(index + 1)
        importo = '{:.2f}'.format(row['SaldoFornitura']).replace('.', '').replace('-', '') 

        # scrivo le rows
        record_10(nr_disposizione, importo, flusso, cf=row['CF'])
        record_20(nr_disposizione)
        record_30(nr_disposizione)
        record_40(nr_disposizione)
        record_60(nr_disposizione)
        record_70(nr_disposizione)
    disposizioni = str(len(df))
    importo_tot = '{:.2f}'.format(df['SaldoFornitura'].sum()).replace('.', '').replace('-', '')
    record_file = str(conta_righe() + 1)

    record_coda(flusso, disposizioni, importo_tot, record_file) 
                                                  
creazione_file()

