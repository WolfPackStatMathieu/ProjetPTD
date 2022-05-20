'''module chargement.py,
classe abstraite'''

import os
import sys
currentdir = os.path.dirname(os.path.realpath("chargement.py"))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from operation import Operation

class Chargement(Operation):
    ''' Classe abstraite pour charger les données

    '''
    def __init__(self, chemin_dossier, noms_fichiers):
        """classe abstraite Chargement pour charger les données
        récupère les données d'un fichier CSV et crée le jeu de donnees afferent.
        Si il y a des valeurs manquantes, le programme affiche un message d'avertissement



        Parameters
        ----------
        chemin : str
            chemin du dossier où sont situés les fichiers à charger
        nom : list[str]
            liste des noms de fichiers à charger

        """



    def ope(self):
        """opération abstraite
        """


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=True)
