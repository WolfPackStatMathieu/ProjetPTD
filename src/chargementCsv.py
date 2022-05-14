'''
Module de chargement des donnees à partir d'un fichier cvs
'''
from chargement import Chargement
import numpy as np
from donnees import Donnees

class ChargementCsv(Chargement):
    """Permet le chargement de jeux de données à partir d'un dossier archivant des fichiers csv

    Parameters
    ----------
    chemin : str
            chemin du dossier où sont situés les fichiers à charger
    nom : list[str]
        liste des noms de fichiers à charger
        vaut 'all' par défaut pour charger tous les fichiers de type csv
        présents dans le dossier d'archivage
    """
    def __init__(self, chemin, nom, delim, header):
