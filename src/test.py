import os
from pathlib import Path
import src.package_affichage
from src.package_chargement.chargement_json import ChargementJson
import src.package_estimation
import src.package_chargement
import src.package_sauvegarde
from src.donnees import Donnees
from src.package_chargement.chargement_csv import ChargementCsv
from src.package_transformation.aggregation_spatiale import Aggreg
from src.package_transformation.jointure import Jointure
from src.pipeline import Pipeline
import src.package_transformation


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

folder_csv=''
folder_json=''
file_name_csv=[]
file_name_json=[]

question_1=Pipeline([ChargementJson(folder_json,file_name_json),ChargementCsv(folder_csv,file_name_csv),Jointure('autre_donnes','keys'), Aggreg('space_var'),Jointure('autre_donnes','keys'), Nuage_de_points('vars')])

reponse_1=question_1.get_res()

question_2=Pipeline([Nuage_de_points('vars')],reponse_1)

question_3 = Pipeline([Serie_temporelle('vars','time_var')], reponse_1)

question_4 = Pipeline([])