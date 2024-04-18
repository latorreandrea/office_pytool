from . import db

# Dichiarazione del model Utenti abilitati alle funzioni della webApp
class Utenti(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), nullable=False, unique=True)
    password = db.Column(db.String(100), nullable=False)