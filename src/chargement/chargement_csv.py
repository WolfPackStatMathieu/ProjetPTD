'''Module de chargement des donnees à partir d'un fichier cvs
'''
import os
import gzip
import csv
from typing import Final
import numpy as np
from chargement import Chargement

class ChargementCsv(Chargement):
    """Permet le chargement de jeux de données à partir d'un dossier
    archivant des fichiers csv.

    Parameters
    ----------
    chemin : str
            chemin du dossier où sont situés les fichiers à charger
    nom : list[str]
        liste des noms de fichiers à charger
        vaut 'all' par défaut pour charger tous les fichiers de type csv
        présents dans le dossier d'archivage
    """
    def __init__(self, chemin_dossier, noms_fichiers, delim =';', header=True):
        """crée un pipeline contenant le premier fichier du dossier, et crée aussi
        les Données de chacun des autres fichiers présents.

        Attributes
        ----------

        Parameters
        ----------
        chemin_dossier : str
            chemin du dossier d'archivage
        noms_fichiers : list[str]
            liste des noms de fichiers à charger, par défaut liste vide
        delim : str
            Caractère séparant les colonnes, vaut ';' par défaut
        header : bool, optionnel
            vaut True si la première ligne contient le nom des variables, par défaut True

        Examples
        --------
        >>> import os
        >>> from pathlib import Path
        >>> path = Path(os.getcwd()).parent.parent.absolute()
        >>> chemin_dossier = path
        >>> nom_fichier='synop.201301.csv.gz'
        >>> delimiteur = ';'
        >>> chargement1 = ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True)
        synop.201301.csv.gz

        """
        Chargement.__init__(self, chemin_dossier, noms_fichiers)
        #le nom du dossier sans l'extension
        #nom_du_dossier = chemin_dossier.split('\\')[-1].split('.')[0]
        #On récupère la liste de TOUS les fichiers (avec le chemin absolu) contenus dans le
        # dossier donnés en paramètre
        fichiers_trouves = {}
        for repertoire, sous_repertoire, fichiers in os.walk(chemin_dossier):
            for fichier in fichiers:
                fichiers_trouves[fichier] = os.path.abspath(f"{repertoire}/{fichier}")
        fichiers_conserves = {}
        for key, value in fichiers_trouves.items():
            #Si le nom de fichier a une extension 'csv.gz' on le garde
            if value.split('\\')[-1].split('.')[-2:] == ['csv', 'gz']:
                fichiers_conserves[key] = value
        fichiers_conserves_2 ={}
        for key, value in fichiers_conserves.items():
            if value.split('\\')[-1]  in noms_fichiers:
                fichiers_conserves_2[key] = value

        # mon test de retour
        for key, value in fichiers_conserves_2.items():
            print(value.split('\\')[-1:][0])

        #Dossier où se trouve le fichier :

        for fichier, chemin in fichiers_conserves_2.items():
            data = []
            presence_na = False

            with gzip.open(chemin, mode='rt') as gzfile :
                #.readlines()[1:3] pour ne lire que les 3 premières lignes
                synopreader = csv.reader(gzfile.readlines()[0:3], delimiter = delim)
                for row in synopreader :
                    # début du traitement de chaque ligne
                    for i, value in enumerate(row): # on parcourt chaque ligne
                        try:
                            if value == 'mq': # C'est une valeur manquante
                                row[i] = np.NaN
                                presence_na = True # On marque la présence de valeur manquante
                            if value.isdigit(): # C'est un int
                                row[i] = int(value) # on le caste en int
                            else: #C'est donc un float
                                row[i] = float(value.replace(',', '.')) # on remplace les , par des .
                        except Exception:
                            pass # C'était une str mal formatée
                    data.append(row)

            print(data)
            #On récupère le nombre de variables
            nb_variables = max(len(row) for row in data)
            print(nb_variables)





if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose = False)
