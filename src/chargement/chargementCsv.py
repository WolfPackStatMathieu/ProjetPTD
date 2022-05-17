'''
Module de chargement des donnees à partir d'un fichier cvs
'''
from genericpath import isfile
from chargement import Chargement
import numpy as np
from donnees import Donnees
import os
from os import listdir
from os.path import isfile, join
from os import walk

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
        >>> chemin_dossier = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\Fichiers de Données .csv.gz-20220405"
        >>> nom_fichier='synop.201301.csv.gz'
        >>> delimiteur = ';'
        >>> chargement1 = ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True)

        """

        Chargement.__init__(self, chemin_dossier, noms_fichiers)
        #le nom du dossier sans l'extension
        nom_du_dossier = chemin_dossier.split('\\')[-1].split('.')[0]

        #On récupère la liste de TOUS les fichiers (avec le chemin absolu) contenus dans le dossier donnés en paramètre
        fichiers_trouves = {}
        for repertoire, sousRepertoire, fichiers in os.walk(chemin_dossier):

            for fichier in fichiers:

                fichiers_trouves[fichier] = os.path.abspath(f"{repertoire}/{fichier}")
                print(fichiers_trouves[fichier])

        # for fichier in fichiers_trouves:
        # condition surl e nom de fichier à garder
        #     if fichiers_trouves[fichier].split('csv.gz')[-1] != '':


        # print(fichiers_trouves)

        #On ne conserve que les fichiers en .csv.gz
        #fichiers_csvgz = {fichier for fichier in listeFichiers if fichier.split('.csv.gz')[-1] == ''}
# TODO retirer les noms de fichiers qui ne sont pas en csv.gz



if __name__ == '__main__':
    import doctest
    doctest.testmod()