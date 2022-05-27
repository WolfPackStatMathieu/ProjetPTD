

import os
from pathlib import Path

from src.donnees import Donnees
from src.pipeline import Pipeline
from src.operation import Operation
from src.package_chargement.chargement import Chargement
from src.package_chargement.chargement_csv import ChargementCsv

donnees1 = Donnees('vide', [], [])
path = Path(os.getcwd()).parent.absolute()
print(path)
chemin_dossier = str(path) + "\\Fichiers de Données .csv.gz-20220405"
nom_fichier='synop.201301.csv.gz'
delimiteur = ';'

ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True).charge()



