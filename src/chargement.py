'''module chargement.py,
classe abstraite'''
from abc import ABC, abstractmethod

from fileinput import filename
import gzip
import csv
import numpy as np

class Chargement(ABC):
    ''' Classe abstraite pour charger les données

    '''
    def __init__(self, chemin, nom):
        """classe abstraite Chargement pour charger les données
        récupère les données d'un fichier CSV et crée le jeu de donnees afferent.
        Si il y a des valeurs manquantes, le programme affiche un message d'avertissement



        Parameters
        ----------
        chemin : str
            chemin du dossier où sont situés les fichiers à charger
        nom : list de string
            _description_
        """
        pass


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=True)