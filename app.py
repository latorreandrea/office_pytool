# Import di sistema
import os
from env import CARTELLA, SERVICE_ACCOUNT, BUCKET_NAME, BEARER_TOKEN_SA
# Import database
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
# Import sviluppo API
import json
# Import funzioni flask
from flask import (
        Flask, flash, render_template, make_response,
        redirect, request, session, url_for, send_file,
        jsonify
    )
from werkzeug.utils import secure_filename
from werkzeug.security import (
        generate_password_hash,
        check_password_hash
    ) 
from flask_login import (
        UserMixin, LoginManager,
        login_user, login_required,
        current_user, logout_user
    )
import logging
# import librerie google
from google.cloud import bigquery
# import funzioni da script python
from script import (
        # ImportAnagraficheEE, ImportRcuEE, carica_tabella, office365_api,
        # clouddatafusion_api, caricamento_librerie_EE_GAS, mgp_prezzi,
        emissioni_bonifici_domiciliati, flusso_acusim_15, emissioni_bonifici_doppi_pagamenti

    )
# import configurazioni/model/database
app = Flask(__name__)
app.config.from_pyfile("env.py")
# impostazione database
db = SQLAlchemy(app)

#impostazioni login
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.init_app(app)

log = logging.getLogger('app')

# Dichiarazione del model Utenti abilitati alle funzioni della webApp
class Utente(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), nullable=False, unique=True)
    password = db.Column(db.String(100), nullable=False)

# Definizione del login
@login_manager.user_loader
def load_user(utente_id):
    """
    restituisce il valore dell'utente
    """    
    return Utente.query.get(int(utente_id))

#---------- GESTIONE LOGIN ----------#


@app.route("/")
def login():
    """
    renderizza la pagina di login solo se viene effettuato 
    l'accesso da determinati ip
    """  
    # TODO blocco accesso per IP  
    ip_addr = request.remote_addr    
    if ip_addr == '127.0.0.1': 
        return render_template('login.html')
    else:
        return 'non abilitato' 
  

@app.route("/logout")
@login_required
def logout():
    """
    permette il logout
    """
    logout_user()
    return redirect(url_for('login'))


@app.route("/", methods=['POST'])
def login_post():
    """
    verifica le credenziali inserite nel login
    """    
    username = request.form['username']
    password = request.form['password']
    user_esistente = Utente.query.filter_by(username=username).first()    
    try:
        if user_esistente.password == password:
            login_user(user_esistente)
            return redirect(url_for('hello'))
        else:
            return redirect(url_for('login'))
    except:
        return redirect(url_for('login'))


# HOME PAGE
@app.route('/home')
@login_required
def hello():
    """
    renderizza la homepage
    """   
    return render_template('home.html', nome=current_user.username)

#---------- GESTIONE FILE MENSILI CARICATI DA CSV ----------#


@app.route('/upload')
@login_required
def upload_file():
    """
    renderizza la pagina di upload file
    """
    files = os.listdir(CARTELLA)
    
    return render_template('upload.html', nome=current_user.username)
    

@app.route('/uploader', methods=['GET', 'POST'])
@login_required
def uploader_file():
    """
    renderizza la pagina di upload e gestisce la logica dell'upload del file
    """   
    if request.method == 'POST':
        try:
            files = request.files.getlist("file")
            for file in files:           
                file.save(os.path.join(CARTELLA, file.filename))
            submit=True
            return render_template('upload.html', nome=current_user.username, success="è possibile avviare l'elaborazione", submit=submit)
        except:
            return render_template('upload.html', nome=current_user.username, alert="errore nel caricamento")


# @app.route('/anagrafiche', methods=['GET'])
# @login_required
# def import_anagrafiche():
#     table_id = 'pltpg-mubi.TRADING.AnagraficheEE'
#     jobs = []
    
#     if len(os.listdir(CARTELLA)) > 0:
#         try:
#             job = ImportAnagraficheEE.main_anagrafiche_EE()        
#             return render_template('success.html', jobs=job)
#         except:
#             for file in os.listdir(CARTELLA):
#                 os.remove(CARTELLA+'/'+file)
#             return render_template('upload.html', nome=current_user.username, alert="errore durante la lettura delle Anagrafiche")
#     else:
#         return render_template('upload.html', alert="il file non è stato trovato")


# @app.route('/recuee', methods=['GET'])
# @login_required
# def import_recuee():
#     """
#     gestiche i file zip con la sigla REP_ELE
#     """
#     table_id = 'pltpg-sviluppo.testdati.RcuEE'
#     jobs = []    
#     try:
#         Anno = ImportRcuEE.trova_AnnoMese_recuee(CARTELLA)[0]
#         Mese = ImportRcuEE.trova_AnnoMese_recuee(CARTELLA)[1]
#     except:
#         return render_template('upload.html', alert="il file RcuEE non è stato trovato")    
#     try:
#         if len(os.listdir(CARTELLA)) > 0:
#             try:
#                 job = ImportRcuEE.mainRcuEE()                                          
#                 return render_template('success.html', jobs=job)
#             except:
#                 return render_template('upload.html', alert="il file RcuEE non è stato elaborato")

#         else:
#             return render_template('upload.html', alert="il file non è stato trovato")
#     except:
#         for file in os.listdir(CARTELLA):
#                 os.remove(CARTELLA+'/'+file)
#         return render_template('upload.html', alert="il file RcuEE non è stato trovato ricaricare i file")   


# @app.route('/caricamentolibrerie', methods=['GET'])
# @login_required
# def upload_librerie():
#     if len(os.listdir(CARTELLA)) > 0:
#         # try:
#         job = caricamento_librerie_EE_GAS.main_upload_librerie()
#         return render_template('success.html', jobs=[job])
#     #     except:
#     #         for file in os.listdir(CARTELLA):
#     #             os.remove(CARTELLA + '/' + file)
#     #         return render_template('upload.html', alert="il file libreria non è stato trovato")
#     # else:
#     #     return render_template('upload.html', alert="nessun file è stato caricato")


# @app.route('/mercatoelettrico', methods=['POST', 'GET'])
# @login_required
# def mercato_elettrico():
#     """
#     view per avviare con le date indicate nel form il download dei
#     dati MGP
#     """
#     if request.method == 'POST':
#         stringadatainizio = request.form.get('inizio').replace("-", "")        
#         stringadatafine = request.form.get('fine').replace("-", "")        
#         if not request.form.get('inizio')  or not request.form.get('fine'):            
#             return render_template('mercatoelettrico.html', alert="inserire data d'inizio e di fine")
#         if int(stringadatafine) < int(stringadatainizio):
#             return render_template('mercatoelettrico.html', alert="la data di inizio deve essere precedente alla data di fine")
#         try:
#             mgp_prezzi.main_MGP(stringadatainizio, stringadatafine)
#             return render_template('success.html', jobs=['xml convertiti e caricati su BigQuery'])
#         except:
#             return render_template('mercatoelettrico.html', nome=current_user.username, alert="errore durante il caricamento contattare l'ufficio ICT")
        
#     return render_template('mercatoelettrico.html', nome=current_user.username, ultima_data=mgp_prezzi.ultima_data_aggiornamento_me())

        
# #---------- GESTIONE FILE CSV ----------#

# @app.route('/invia_csv')
# @login_required
# def invio_csv():
#     """
#     creo la route per l'upload del file
#     """  
#     return render_template('invio_csv_excel.html', nome=current_user.username)
    

# @app.route('/upload_csv', methods=['POST'])
# @login_required
# def upload_csv():    
#     """
#     renderizza la pagina di upload e gestisce la logica dell'upload del file
#     """   
#     if request.method == 'POST':
#         dataset = request.form['dataset']
#         separatore = request.form['separatore']
#         table_name = request.form['table_name']
#         try:
#             file = request.files.getlist("file")[0]
#             if file:
#                 file.save(os.path.join(CARTELLA, file.filename))
#                 if len(request.form['table_name']) < 1:
#                     os.remove(os.path.join(CARTELLA, file.filename))
#                     return render_template('invio_csv_excel.html', nome=current_user.username, alert="inserire il nome della tabella da creare su BigQuery")
#                 else:
#                     messaggio = carica_tabella.main_carica_csv('pltpg-mubi', dataset, table_name, file.filename, separatore)
#                     os.remove(os.path.join(CARTELLA, file.filename))
#                     return render_template('success.html', jobs=[messaggio])

#             else:
#                 return render_template('invio_csv_excel.html', nome=current_user.username, alert="nessun file presente")
#         except: 
#             for file in os.listdir(CARTELLA):
#                 os.remove(CARTELLA+'/'+file)  
#             return render_template('invio_csv_excel.html', nome=current_user.username, alert="errore nel caricamento")


#---------- GESTIONE PIPELINES ----------#

@app.route('/pipelines')
@login_required
def pipelines_list():
    """
    renderizza la pagina delle pipeline
    """
    # TODO lista delle pipeline da visionare
    pipelines = ['PLTBC_TariffBracket']
    # questa lista rappresenta tutto il flusso di controllo della pipeline billline
    pipelines_billline = ['PLTBC_BillLine', 'PLTBC_BillLine_verificaAllineamento', 'PLTBC_BillNoProvvisoriMubi', 'PLTBC_BillLine_outbound']
    # creo token per richiesta status pipeline su GOOGLE DATA FUSION
    token = clouddatafusion_api.create_token()

    list_pipeline_dict = []
    for pipeline in pipelines:
        pipeline_dict = {}
        status = clouddatafusion_api.get_pipeline_status(pipeline, token)
        pipeline_dict['name'] = pipeline
        pipeline_dict['status'] = status
        list_pipeline_dict.append(pipeline_dict)
    # GESTIONE PARTICOLARE DELLE PIPELINE PER AGGIORNAMENTO BILLLINE:    
    list_billline_status = []
    pipeline_dict = {}
    for pipeline in pipelines_billline:        
        status = clouddatafusion_api.get_pipeline_status(pipeline, token)        
        list_billline_status.append(status)        
    if 'COMPLETED' in list_billline_status and 'REJECTED' not in list_billline_status:
        pipeline_dict['name'] = 'PLTBC_BillLine'
        pipeline_dict['status'] = 'In esecuzione'
        list_pipeline_dict.append(pipeline_dict)
    elif 'RUNNING' in list_billline_status:
        pipeline_dict['name'] = 'PLTBC_BillLine'
        pipeline_dict['status'] = 'In avvio'
        list_pipeline_dict.append(pipeline_dict)
    else:
        pipeline_dict['name'] = 'PLTBC_BillLine'
        pipeline_dict['status'] = 'Rigettato'
        list_pipeline_dict.append(pipeline_dict)
    return render_template('pipelines.html', pipelines=list_pipeline_dict, nome=current_user.username)


@app.route('/<name>/start')
@login_required
def pipeline_start(name):
    token = clouddatafusion_api.create_token()
    clouddatafusion_api.start_pipeline(name, token)
    return redirect(url_for('pipelines_list'), nome=current_user.username)
    
@app.route('/<name>/stop')
@login_required
def pipeline_stop(name):
    token = clouddatafusion_api.create_token()
    clouddatafusion_api.stop_pipeline(name, token)
    return redirect(url_for('pipelines_list') , nome=current_user.username)


# #---------- CONDIVISIONE EXCEL SU SHAREPOINT -----------#
@app.route('/sharepoint')
@login_required
def share_page():
    return render_template('share_plenitude.html', nome=current_user.username)


@app.route('/sharepoint_sql')
@login_required
def share_page_sql():
    return render_template('share_plenitude_sql.html', nome=current_user.username)


@app.route('/sharepoint/send' , methods=['POST'])
@login_required
def share_page_work():
    """
    renderizza la pagina di upload e gestisce la logica dell'upload del file
    """  
    ### FUNZIONE ANCORA IN TEST 
    if request.method == 'POST':
        file_name = request.form['file_name']
        file_name = file_name.replace(' ','')
        if request.form['sql'] == '':
            dataset = request.form['dataset']
            table_name = request.form['table_name']
            sql = ''
        else:
            dataset = ''
            table_name = ''
            sql = request.form['sql']
        #TODO il progetto al momento è bloccato al eniplenidataroom
        project = 'enipleni-dataroom'
        if file_name.split('.')[-1] == 'csv':            
            pass
        elif len(file_name.split('.')) < 2:
            file_name = file_name + '.csv'
        else:
            return render_template('share_plenitude.html', nome=current_user.username, alert="nome file non valido")
        try:
            office365_api.main(file_name, project, dataset, table_name, sql)
            return render_template('success.html', jobs=[f'{file_name} condiviso su sharepoint Plenitude'])
        except:
            for file in os.listdir(CARTELLA):
                os.remove(CARTELLA+'/'+file) 
            return render_template('share_plenitude.html', nome=current_user.username, alert="errore di esecuzione \n ripetere l'operazione")

#------------- SEZIONE CREDITO --------------------------------------------#


@app.route('/bonifici-domiciliati')
@login_required
def download_bonifici_domiciliati():
    """View per creare file Bondom"""
    # try:
    files = emissioni_bonifici_domiciliati.creazione_file_bonifico()  
    return render_template('download_file.html', nome=current_user.username,  files=files)
    # except:
    #     return render_template('upload.html', nome=current_user.username, alert="errore nel caricamento")


@app.route('/download/<file_name>')
@login_required
def download_file(file_name):
    """Funzione per scaricare file"""
    try:
        file_path = f'{CARTELLA}/{file_name}'
        if os.path.exists(file_path):
            response = send_file(file_path, as_attachment=True, download_name=file_name)
            os.remove(file_path)  # Elimina il file dal server dopo il download
            return response
        else:
            return "File not found", 404
    except Exception as e:
        return "Si è verificato un errore: ", str(e)


# ------------- file acusim -------------------#
@app.route('/acusim15')
@login_required
def acusim_15():
    """
    view per avviare elaborazione
    file csv flusso 15 acusim
    """
    try:
        response = flusso_acusim_15.main_load_csv()
        return render_template('success.html', nome=current_user.username, jobs=[f'{response}'])
    except:        
        return render_template('upload.html', nome=current_user.username, alert="errore nel caricamento")


# ------------- emissione doppi pagamenti -------------------#
@app.route('/caricamento_bondoppag')
@login_required
def bonificinonimputati():
    """
    view per avviare elaborazione
    del file excel caricato
    """
    # try:
    files = emissioni_bonifici_doppi_pagamenti.creazione_file_bonifico()  
    return render_template('download_file.html', nome=current_user.username,  files=files)


if __name__ == "__main__":
    app.run()
