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
from src.package_sauvegarde.sauvegardeCsv import SauvegardeCsv


#####Déclaration des inventaires####
# global INVENTAIRE_CSV
# INVENTAIRE_CSV = []

# ChargementCsv("test")
path = Path(os.getcwd()).absolute()
cheminDossier = str(path) + "\\Fichiers de Données .csv.gz-20220405"
nom_fichier=['synop.202203.csv.gz']
delimiteur = ';'
pipeline1 = Pipeline([ChargementCsv(cheminDossier, nom_fichier, delimiteur, True)])

chemin = str(Path(os.getcwd()).absolute())
nom2 = "synop.201301_csv"
pipeline1.add_ope(SauvegardeCsv(chemin, nom2))
pipeline1.execute()

# print(pipeline1)


