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
print(cheminDossier)
nom_fichier=['postesSynopAvecRegions.csv.gz']
delimiteur = ';'
liste_donnees = ChargementCsv(cheminDossier, nom_fichier, delimiteur, True).charge()

# pipeline1 = Pipeline([ChargementCsv(cheminDossier, nom_fichier, delimiteur, True)]).get_res()
print(liste_donnees[0].data)


#Vérification de la présence des jeux de données
# print(globals().keys())
# globals()["INVENTAIRE_JSON"]
# print(globals() INVENTAIRE_JSON)
# print(globals()['INVENTAIRE_CSV'])



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
nom_fichier=['synop.201902.csv.gz']
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

fichiers_a_concatener =[]
for file in file_name_json:
    nom = file.split(".")[0]
    fichiers_a_concatener.append(nom)

liste_de_donnees = ChargementJson(folder_json, file_name_json[1:])


question1=Pipeline([ChargementJson(folder_json,file_name_json),ChargementCsv(folder_csv,file_name_csv), Aggregation('numer_sta'),Jointure(Pipeline([Concatenation(file_name_json[1:]),file_name_json[0]]).get_res,'code_insee_region'), Nuage_points(['temperature', 'consommation_brute_electricite_rte'])])
question1.execute()
# Nuage_points(['temperature', 'consommation_brute_electricite_rte']).ope(question1).get_res()
reponse_1 = question1.resultat
print(reponse_1.variables)


question_2=Pipeline([Nuage_points('vars')],reponse_1)

question_3 = Pipeline([Serie_temporelle('vars','time_var')], reponse_1)

# question_4 = Pipeline([Carte('var')],reponse_1) #Pyplot n'affiche rien dans VScode