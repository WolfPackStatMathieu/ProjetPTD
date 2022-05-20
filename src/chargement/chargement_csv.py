'''Module de chargement des donnees à partir d'un fichier cvs
'''
import os
import gzip
import csv
import sys
from datetime import date, datetime

from chargement import Chargement
from Donnees import Donnees
import numpy as np


class ChargementCsv(Chargement):
    """Permet le chargement de jeux de données à partir d'un dossier
    archivant des fichiers csv.

    Ce module charge en mémoire autant de jeux de Données que de fichiers csv.gz et

    Parameters
    ----------
    chemin_dossier : str
            chemin du dossier où sont situés les fichiers à charger
    noms_fichiers : list[str]
        liste des noms de fichiers à charger
        vaut 'all' par défaut pour charger tous les fichiers de type csv
        présents dans le dossier d'archivage
    """

    def __init__(self, chemin_dossier, noms_fichiers = 'all', delim =';', header=True):
        self.chemin_dossier = chemin_dossier
        self.noms_fichiers = noms_fichiers
        self.delim = delim
        self.header = header

    def charge(self):
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
        >>> chemin_dossier = str(path) + "\\Fichiers de Données .csv.gz-20220405"
        >>> nom_fichier='synop.201301.csv.gz'
        >>> delimiteur = ';'
        >>> ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True).charge() # doctest:+ELLIPSIS
        Attention: le jeu de données synop_201301 présente des valeurs manquantes
        ...

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
        #On conserve soit tous les fichiers, soit uniquement ceux entrés dans le paramètre noms_fichiers
        if noms_fichiers == 'all':
            fichiers_conserves_2 = fichiers_conserves
        else:
            for key, value in fichiers_conserves.items():
                if value.split('\\')[-1]  in noms_fichiers:
                    fichiers_conserves_2[key] = value

        # retour pour la doctest
        # for key, value in fichiers_conserves_2.items():
        #     print(value.split('\\')[-1:][0]) # le nom du fichier
        #     print(value.split('\\')[-1:][0].split('.')[1]) #la date du fichier

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
                                row[i] = np.NaN #transformation en type valeur manquante de numpy
                                presence_na = True # On marque la présence de valeur manquante
                            if value.isdigit(): # C'est un int
                                row[i] = int(value) # on le caste en int
                            else: #C'est donc un float
                                row[i] = float(value.replace(',', '.')) # on remplace les ,
                                #par des .
                        except Exception:
                            pass # C'était une str mal formatée
                    data.append(row) #on ajoute la ligne à nos données

            ### Gestion du nombre et des noms de variables ###
            #On récupère le nombre colonnes maximum : c'est le nombre de variables
            nb_variables = max(len(row) for row in data)


            if header: #Si le fichier fourni contient les noms de variables
                #On met à part les noms des variables
                variables = data.pop(0)
                #print(variables)
                #print(len(variables))
                if len(variables) < nb_variables: #il manque des noms de variables
                    #On rajoute des noms de variables artificiels
                    variables += [f'Var.{str(i)}' for i in range(len(variables) + 1,
                                                                 nb_variables + 1)]
            else: #Il n'y a pas de nom de variable
                #On les rajoute artificiellement
                #Var.1 Var.2 Var.3 .....
                variables = [f'Var.{str(i)}' for i in range(1, nb_variables+1)]

            ### Gestion des dates ###
            if "date" in variables:
                index = variables.index("date") #position de la colonne
                for i, row in enumerate(data):
                    #on reformatte la valeur pour en faire une date
                    row[index] = datetime.strptime(str(row[index]), "%Y%m%d%H%M%S")


            ### Gestion des lignes trop courtes ###
            #on introduit des valeurs manquantes
            introduction_nan = False
            for i, row in enumerate(data):
                if len(row) != nb_variables:
                    #Il faut autant de nan que de colonnes manquantes
                    row += [np.NaN] * (nb_variables - len(row))
                    introduction_nan = True #signale l'introduction de valeurs manquantes


            #On construit un objet Donnees par fichier qui prend en nom le début
            debut_nom = fichier.split('.')[0]
            date = fichier.split('.')[1]
            nom_donnees = debut_nom + "_" + date
            # print(nom_donnees)
            globals()[nom_donnees] = Donnees(nom= fichier ,variables= variables, data= data)
            globals()[nom_donnees].del_var(['']) #on supprime la dernière colonne qui est vide car c'était dans le CSV d'origine
            # print((globals()[nom_donnees]))


            #message pour l'introduction de valeurs manquantes
            if introduction_nan:
                print("Attention: valeurs manquantes introduites lors de la"
                      "création du jeu de données " f'{nom_donnees}')
            elif presence_na:
                print("Attention: le jeu de données "f'{nom_donnees} ' "présente des valeurs manquantes")










if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose = False)
