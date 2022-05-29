import os
from pathlib import Path


import src.package_affichage
from src.package_chargement.chargement_json import ChargementJson
import src.package_estimation
import src.package_chargement
import src.package_sauvegarde
from src.donnees import Donnees
from src.package_chargement.chargement_csv import ChargementCsv
from src.package_transformation.aggregation_spatiale import Aggregation
from src.package_transformation.concatenation import Concatenation
from src.package_transformation.jointure import Jointure
from src.pipeline import Pipeline
import src.package_transformation
from src.package_transformation.aggregation_spatiale import Aggregation
from src.package_affichage.nuage_points import Nuage_points
from src.package_affichage.serie_temporelle import Serie_temporelle
# from src.package_affichage.carte import Carte


#####Déclaration des inventaires####
global INVENTAIRE_CSV
INVENTAIRE_CSV = []

# ChargementCsv("test")
path = Path(os.getcwd()).absolute()
CheminDossier = str(path) + "\\Fichiers de Données .csv.gz-20220405"
nom_fichier=['synop.202203.csv.gz']
Delimiteur = ';'

# Inventaire_CSV = ChargementCsv(CheminDossier, nom_fichier, Delimiteur, True).charge()
# Inventaire_CSV = Inventaire_CSV.get_inventaire_csv()
pipeline1 = Pipeline([ChargementCsv(CheminDossier, nom_fichier, Delimiteur, True).charge()])
# print(pipeline1)
print(globals().keys())
print(INVENTAIRE_CSV)
path = Path(os.getcwd()).absolute()
folder_csv= str(path) + '\\Fichiers de Données .csv.gz-20220405'
folder_json=str(path) + '\\Fichiers de Données .json.gz-20220405'
file_name_csv=['synop.201301.csv.gz', 'synop.201302.csv.gz']
file_name_json=['2013-01.json.gz', '2013-02.json.gz']

question_1=Pipeline([ChargementJson(folder_json,file_name_json),ChargementCsv(folder_csv,file_name_csv),Concatenation(file_name_csv[1:]), Aggregation('numer_sta'),Jointure(Pipeline([Concatenation(file_name_json[1:]),file_name_json[0]]).get_res,'keys'), Nuage_points('vars')])

reponse_1=question_1.get_res()

question_2=Pipeline([Nuage_points('vars')],reponse_1)

question_3 = Pipeline([Serie_temporelle('vars','time_var')], reponse_1)

# question_4 = Pipeline([Carte('var')],reponse_1) #Pyplot n'affiche rien dans VScode