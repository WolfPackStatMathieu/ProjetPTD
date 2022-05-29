import os
import gzip
import shutil
from pathlib import Path
import src.package_affichage
import src.package_estimation
import src.package_chargement
import src.package_sauvegarde
from src.donnees import Donnees
from src.package_chargement.chargement_csv import ChargementCsv
from src.package_chargement.chargement_json import ChargementJson
from src.pipeline import Pipeline
import src.package_transformation
from src.package_sauvegarde.sauvegardeCsv import SauvegardeCsv


#####Déclaration des inventaires####
# global INVENTAIRE_CSV
# INVENTAIRE_CSV = []
# global INVENTAIRE_JSON
# INVENTAIRE_JSON = []


### Chargement du premier fichier csv###
# ChargementCsv("test")
# path = Path(os.getcwd()).absolute()
# cheminDossier = str(path) + "\\Fichiers de Données .csv.gz-20220405"
# nom_fichier=['synop.202203.csv.gz']
# delimiteur = ';'
# pipeline1 = Pipeline([ChargementCsv(cheminDossier, nom_fichier, delimiteur, True)])

# chemin = str(Path(os.getcwd()).absolute())
# nom2 = "synop.201301_csv"
# pipeline1.add_ope(SauvegardeCsv(chemin, nom2))
# pipeline1.execute()

##Chargement du premier fichier Json##
# path = Path(os.getcwd()).absolute()
# cheminDossier = str(path) + "\\Fichiers de Données .json.gz-20220405"
# nom_fichier=['2013-01.json.gz']
# delimiteur = ';'
# pipeline2 =  Pipeline([ChargementJson(cheminDossier, nom_fichier, delimiteur, True)])
# chemin = str(Path(os.getcwd()).absolute())
# nom2 = "2013-01_json"
# pipeline2.add_ope(SauvegardeCsv(chemin, nom2))
# pipeline2.execute()



##### Chargement des Données avec l'identifiant
# de la station et la région ###
#### 1 on le transforme en csv.gz
path = Path(os.getcwd()).absolute()
cheminDossier = str(path) + "\\fichiers stations et régions"
nom_fichier='postesSynopAvecRegions.csv'
chemin_complet = cheminDossier + "\\" + nom_fichier

## Réécriture du fichier csv en csv.gz
with open(chemin_complet, "rb") as f_in:
        with gzip.open(chemin_complet + ".gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

nom2 = ["postesSynopAvecRegions.csv.gz"]
##### 2 on crée les données
path = Path(os.getcwd()).absolute()
cheminDossier = str(path) + "\\fichiers stations et régions"
# print(cheminDossier)
nom_fichier=['postesSynopAvecRegions.csv.gz']
delimiteur = ';'

pipeline1 = Pipeline([ChargementCsv(cheminDossier, nom_fichier, delimiteur, True)]).get_res()

print(pipeline1)

#Vérification de la présence des jeux de données
# print(globals().keys())
# globals()["INVENTAIRE_JSON"]
# print(globals() INVENTAIRE_JSON)
# print(globals()['INVENTAIRE_CSV'])