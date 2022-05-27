import gzip
import json
from src.package_chargement.chargement import Chargement

#Dossier où se trouve le fichier :
folder = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD\\"
filename = "2013-01.json.gz"
with gzip.open(folder + filename, mode = 'rt') as gzfile :
    data = json.load(gzfile)

with gzip.open(folder + filename, 'rb') as f:
    line = f.readline()
    one_line = json.loads(line)
    print(one_line)

class Chargement_json(Chargement):
    """Permet le chargement de jeux de données à partir d'un dossier
    archivant des fichiers .json

    Ce module charge en mémoire autant de jeux de Données que de
    fichiers json présents dans le dossier

    Parameters
    ----------
    Chargement : _type_
        _description_
    """
    def __init__(self, chemin_dossier, noms_fichiers)