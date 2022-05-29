import os
from pathlib import Path
import src.package_affichage
import src.package_estimation
import src.package_chargement
import src.package_sauvegarde
from src.donnees import Donnees
from src.package_chargement.chargement_csv import ChargementCsv
from src.pipeline import Pipeline
import src.package_transformation


#####Déclaration des inventaires####
global INVENTAIRE_CSV
INVENTAIRE_CSV = []

# ChargementCsv("test")
path = Path(os.getcwd()).absolute()
CheminDossier = str(path) + "\\Fichiers de Données .csv.gz-20220405"
nom_fichier=['all']
Delimiteur = ';'

# Inventaire_CSV = ChargementCsv(CheminDossier, nom_fichier, Delimiteur, True).charge()
# Inventaire_CSV = Inventaire_CSV.get_inventaire_csv()
pipeline1 = Pipeline([ChargementCsv(CheminDossier, nom_fichier, Delimiteur, True).charge()])
# print(pipeline1)
print(globals().keys())
# print(globals()['INVENTAIRE_CSV'])

