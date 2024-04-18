# lo script serve per creare un file testo per inviare i bonifici
# il funzionamento prevede più fasi:
# 1) Avere una lista di clienti e forniture a cui dobbiamo inviare i bonifici
# 2) Creare un file contenente i dati del cliente
# 3) Creare il file e restituirlo a chi lo ha creato
import datetime
import os
import random
import string
from google.cloud import bigquery
import pandas as pd
# 2) Creazione file invio bonifici:
from app import CARTELLA, SERVICE_ACCOUNT

file = 'RichiestaEmissioneBonificiDomiciliati' + datetime.datetime.now().strftime('%y%m%d') + '.txt'
file_excel = 'RichiestaEmissioneBonificiDomiciliati' + datetime.datetime.now().strftime('%y%m%d') + '.xlsx'
cartella = CARTELLA

percorso_file = os.path.join(cartella, file)
percorso_file_excel = os.path.join(cartella, file_excel)
# SERVICE_ACCOUNT 
# CREAZIONE VARIABILI PER FILE TESTO

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


def record_testa(flusso:str):
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

def conta_righe():
    with open(percorso_file, 'r') as file:
        righe = sum(1 for line in file)
    return righe


def record_coda(flusso:str, disposizioni:str, importo_tot:str, record_file:str):
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


def record_10(nr_disposizione:str, importo:str, code_line:str, cf:str):
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
        file.write('\n')


def record_20(nr_disposizione:str):
    """funzione per creare record tipo 20"""
    with open(percorso_file, 'a') as file:
        
        file.write(str(filler(1)))
        file.write('20')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write('Eni Plenitude S.p.A. S.Benefit')
        file.write(str(filler(80)))
        file.write('\n')


def record_30(nr_disposizione:str, denominazione:str):
    """funzione per creare record tipo 30"""
    with open(percorso_file, 'a') as file:
        
        file.write(str(filler(1)))
        file.write('30')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(completa_dx(denominazione, ' ', 90)))
        file.write(str(filler(20)))
        file.write('\n')


def record_40(nr_disposizione:str):
    """funzione per creare record tipo 40"""
    with open(percorso_file, 'a') as file:        
        file.write(str(filler(1)))
        file.write('40')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(filler(110)))
        file.write('\n')


def record_60(nr_disposizione:str):
    """funzione per creare record tipo 60"""
    with open(percorso_file, 'a') as file:        
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


def record_70(nr_disposizione:str, codice_cliente:str):
    """funzione per creare record tipo 70"""
    with open(percorso_file, 'a') as file:        
        file.write(str(filler(1)))
        file.write('70')
        file.write(str(completa_sx(nr_disposizione, '0', 7)))
        file.write(str(filler(46)))
        file.write(codice_cliente + '00' + str(genera_codice_alfanumerico(20))) # chiave di raccordo ottenuta con COD CLIENTE + CODCASUALE?
        file.write(str(filler(34)))
        file.write('\n')
        

def creazione_lista_clienti():
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT)
    query = """
                SELECT DISTINCT
                CA.CustomerNo AS Cliente
                , SC.SaldoCliente
                , C.FiscalCode AS CF
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
                    "À", "A'") AS Denominazione
                FROM `pltpg-mubi.CREDITO.CustomerAccount` CA
                JOIN `pltpg-mubi.CREDITO.Customer` C 
                ON C.No = CA.CustomerNo
                LEFT JOIN
                (
                SELECT 
                    CLE.CustomerNo AS Nrcliente, 
                SUM((SELECT SUM(dcle.Amount)
                    FROM `pltpg-mubi.CREDITO.DetailedCustLedgEntry` dcle
                    WHERE  dcle.CustLedgerEntryNo = CLE.EntryNo
                    AND NOT (CLE.DocumentNo LIKE '%CRP%' OR CLE.DocumentNo LIKE '%CRN%'))) AS SaldoCliente
                FROM `pltpg-mubi.CREDITO.CustLedgerEntry` AS CLE 
                GROUP BY CLE.CustomerNo
                ) SC
                ON CA.CustomerNo = SC.Nrcliente
                LEFT JOIN
                (
                SELECT 
                CLE.CustomerAccountNo AS Utenza, 
                SUM((SELECT SUM(dcle.Amount)
                    FROM `pltpg-mubi.CREDITO.DetailedCustLedgEntry` dcle
                    WHERE        dcle.CustLedgerEntryNo = CLE.EntryNo
                    AND NOT (CLE.DocumentNo LIKE '%CRP%' OR CLE.DocumentNo LIKE '%CRN%'))) AS SaldoFornitura
                FROM `pltpg-mubi.CREDITO.CustLedgerEntry` AS CLE
                GROUP BY CLE.CustomerAccountNo
                )SF
                ON CA.No = SF.Utenza
                WHERE CA.AccountStatus IN (1, 4, 5)   /* FORNITURE CESSATE, ERRATE, NON FATTURABILI */
                AND (SaldoCliente <= -10 AND SaldoCliente >= -6000)
                AND SaldoFornitura < 0
                AND LENGTH(C.FiscalCode) = 16    
                -- richiesta matteo.federico@eniplenitude.com del 24/11/2023   
                AND  CA.CustomerNo NOT IN ('CL138518','CL096449','CL208320','CL132430','CL222187','CL171368','CL129788','CL133156','CL041002','CL046804','CL127991')
                --       
                ORDER BY CA.CustomerNo
        		-- test               
		        -- limit 100
            """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    df['SaldoCliente'] = df['SaldoCliente'].astype(float).round(2) 
    return df


def creazione_lista_nrmovimenti():
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT)
    query = """
                SELECT DISTINCT
                CA.CustomerNo AS Cliente
                , SC.SaldoCliente
                , C.FiscalCode AS CF
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
                    "À", "A'") AS Denominazione
                    ,Nrdocumento
                    ,ImportoResiduoCompleto
                FROM `pltpg-mubi.CREDITO.CustomerAccount` CA
                JOIN `pltpg-mubi.CREDITO.Customer` C 
                ON C.No = CA.CustomerNo
                LEFT JOIN
                (
                SELECT 
                    CLE.CustomerNo AS Nrcliente, 
                SUM((SELECT SUM(dcle.Amount)
                    FROM `pltpg-mubi.CREDITO.DetailedCustLedgEntry` dcle
                    WHERE  dcle.CustLedgerEntryNo = CLE.EntryNo
                    AND NOT(CLE.DocumentNo LIKE '%CRP%' OR CLE.DocumentNo LIKE '%CRN%'))) AS SaldoCliente
                FROM `pltpg-mubi.CREDITO.CustLedgerEntry` AS CLE 
                GROUP BY CLE.CustomerNo
                ) SC
                ON CA.CustomerNo = SC.Nrcliente
                LEFT JOIN
                (
                SELECT 
                CLE.CustomerAccountNo AS Utenza, 
                SUM((SELECT SUM(dcle.Amount)
                    FROM `pltpg-mubi.CREDITO.DetailedCustLedgEntry` dcle
                    WHERE        dcle.CustLedgerEntryNo = CLE.EntryNo
                    AND NOT(CLE.DocumentNo LIKE '%CRP%' OR CLE.DocumentNo LIKE '%CRN%'))) AS SaldoFornitura
                FROM `pltpg-mubi.CREDITO.CustLedgerEntry` AS CLE
                GROUP BY CLE.CustomerAccountNo
                )SF
                ON CA.No = SF.Utenza
                JOIN (SELECT 
                        CustomerNo 
                        , CLE.DocumentNo AS NrDocumento
                        , SUM((SELECT SUM(DCLE.Amount)
                        FROM `pltpg-mubi.CREDITO.DetailedCustLedgEntry` DCLE
                        WHERE DCLE.CustLedgerEntryNo = CLE.EntryNo
                        AND NOT(CLE.DocumentNo LIKE '%CRP%' OR CLE.DocumentNo LIKE '%CRN%'))) AS ImportoResiduoCompleto
                        FROM `pltpg-mubi.CREDITO.CustLedgerEntry` AS CLE
                        GROUP BY CLE.CustomerNo, CLE.DocumentNo
                        HAVING ImportoResiduoCompleto != 0
                    ) MOVIMENTI
                ON MOVIMENTI.CustomerNo = CA.CustomerNo
                WHERE CA.AccountStatus IN (1, 4, 5)   /* FORNITURE CESSATE, ERRATE, NON FATTURABILI */
                AND (SaldoCliente <= -10 AND SaldoCliente >= -6000)
                AND SaldoFornitura < 0
                AND LENGTH(C.FiscalCode) = 16   
                -- richiesta matteo.federico@eniplenitude.com del 24/11/2023   
                 AND  CA.CustomerNo NOT IN ('CL138518','CL096449','CL208320','CL132430','CL222187','CL171368','CL129788','CL133156','CL041002','CL046804','CL127991')
                --                  
                ORDER BY CA.CustomerNo
                """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    df['SaldoCliente'] = df['SaldoCliente'].astype(float).round(2) 
    df['ImportoResiduoCompleto'] = df['ImportoResiduoCompleto'].astype(float).round(2)
    df['CodiceUnivoco'] = None
    return df


def read_file_bondom():
    """ 
    Funzione per leggere il file Bondom e 
    recuperare tutti i codici univoci del file
    """
    list_clienti_cod = []
    with open(percorso_file, 'r') as file:
        for line in file:
            if line.startswith(' 70'):
                # Trova l'indice iniziale della sottostringa che inizia con 'CL'
                cl_index = line.find('CL')
                # Estrai la sottostringa 'CL' seguita dai successivi 28 caratteri
                extracted_string = line[cl_index:cl_index+30] 
                list_clienti_cod.append(extracted_string)
    return list_clienti_cod


def creazione_file_bonifico():
    """
    Funzione che gestisce la logica della creazione dei file bondom
    ritorna una lista di file
    """    
    flusso = 'BD-'+ genera_codice_numerico(9)                                     
    record_testa(flusso)                                                      
    df = creazione_lista_clienti() 
    code_line = 'BD-'+ genera_codice_alfanumerico(8) + '-' + str(datetime.datetime.now().strftime('%Y%m%d')) + '  ' 
    for index, row in df.iterrows():
        # dichiaro la disposizione \ l'importo
        nr_disposizione = str(index + 1)
        importo = '{:.2f}'.format(row['SaldoCliente']).replace('.', '').replace('-', '')
        # scrivo le rows
        record_10(nr_disposizione, importo, code_line, cf=row['CF'])
        record_20(nr_disposizione)
        record_30(nr_disposizione, denominazione=row['Denominazione'])
        record_40(nr_disposizione)
        record_60(nr_disposizione)
        record_70(nr_disposizione, codice_cliente=row['Cliente'])
    disposizioni = str(len(df))
    importo_tot = '{:.2f}'.format(df['SaldoCliente'].sum()).replace('.', '').replace('-', '')
    record_file = str(conta_righe() + 1)
    record_coda(flusso, disposizioni, importo_tot, record_file)
    # --- CREAZIONE FILE EXCEL    
    list_clienti_cod = read_file_bondom()
    movimenti = creazione_lista_nrmovimenti() 
    mappa_codici = {codice[:8]: codice for codice in list_clienti_cod}
    movimenti['CodiceUnivoco'] = movimenti['Cliente'].replace(mappa_codici)    
    movimenti.to_excel(percorso_file_excel, index=False)
    return [file, file_excel]
