'''Module de chargement des donnees à partir d'un fichier cvs
'''
from msilib.schema import Error
import os
import gzip
import csv
import sys
from datetime import date, datetime
from src.package_chargement.chargement import Chargement
from src.donnees import Donnees
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
        vaut ['all'] par défaut pour charger tous les fichiers de type csv
        présents dans le dossier d'archivage
     delim : str
            Caractère séparant les colonnes, vaut ';' par défaut
    header : bool, optionnel
        vaut True si la première ligne contient le nom des variables, par défaut True

    """

    def __init__(self, chemin_dossier, noms_fichiers = ['all'], delim =';', header=True):
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
        >>> path = Path(os.getcwd()).absolute()
        >>> chemin_dossier = str(path) + "\\Fichiers de Données .csv.gz-20220405"
        >>> nom_fichier=['all']
        >>> delimiteur = ';'
        >>> ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True).charge()
        jeu de Données créé : synop_201301
        Attention: le jeu de données synop_201301 présente des valeurs manquantes
        jeu de Données créé : synop_201302
        Attention: le jeu de données synop_201302 présente des valeurs manquantes
        jeu de Données créé : synop_201303
        Attention: le jeu de données synop_201303 présente des valeurs manquantes
        jeu de Données créé : synop_201304
        Attention: le jeu de données synop_201304 présente des valeurs manquantes
        jeu de Données créé : synop_201305
        Attention: le jeu de données synop_201305 présente des valeurs manquantes
        jeu de Données créé : synop_201306
        Attention: le jeu de données synop_201306 présente des valeurs manquantes
        jeu de Données créé : synop_201307
        Attention: le jeu de données synop_201307 présente des valeurs manquantes
        jeu de Données créé : synop_201308
        Attention: le jeu de données synop_201308 présente des valeurs manquantes
        jeu de Données créé : synop_201309
        Attention: le jeu de données synop_201309 présente des valeurs manquantes
        jeu de Données créé : synop_201310
        Attention: le jeu de données synop_201310 présente des valeurs manquantes
        jeu de Données créé : synop_201311
        Attention: le jeu de données synop_201311 présente des valeurs manquantes
        jeu de Données créé : synop_201312
        Attention: le jeu de données synop_201312 présente des valeurs manquantes
        jeu de Données créé : synop_201401
        Attention: le jeu de données synop_201401 présente des valeurs manquantes
        jeu de Données créé : synop_201402
        Attention: le jeu de données synop_201402 présente des valeurs manquantes
        jeu de Données créé : synop_201403
        Attention: le jeu de données synop_201403 présente des valeurs manquantes
        jeu de Données créé : synop_201404
        Attention: le jeu de données synop_201404 présente des valeurs manquantes
        jeu de Données créé : synop_201405
        Attention: le jeu de données synop_201405 présente des valeurs manquantes
        jeu de Données créé : synop_201406
        Attention: le jeu de données synop_201406 présente des valeurs manquantes
        jeu de Données créé : synop_201407
        Attention: le jeu de données synop_201407 présente des valeurs manquantes
        jeu de Données créé : synop_201408
        Attention: le jeu de données synop_201408 présente des valeurs manquantes
        jeu de Données créé : synop_201409
        Attention: le jeu de données synop_201409 présente des valeurs manquantes
        jeu de Données créé : synop_201410
        Attention: le jeu de données synop_201410 présente des valeurs manquantes
        jeu de Données créé : synop_201411
        Attention: le jeu de données synop_201411 présente des valeurs manquantes
        jeu de Données créé : synop_201412
        Attention: le jeu de données synop_201412 présente des valeurs manquantes
        jeu de Données créé : synop_201501
        Attention: le jeu de données synop_201501 présente des valeurs manquantes
        jeu de Données créé : synop_201502
        Attention: le jeu de données synop_201502 présente des valeurs manquantes
        jeu de Données créé : synop_201503
        Attention: le jeu de données synop_201503 présente des valeurs manquantes
        jeu de Données créé : synop_201504
        Attention: le jeu de données synop_201504 présente des valeurs manquantes
        jeu de Données créé : synop_201505
        Attention: le jeu de données synop_201505 présente des valeurs manquantes
        jeu de Données créé : synop_201506
        Attention: le jeu de données synop_201506 présente des valeurs manquantes
        jeu de Données créé : synop_201507
        Attention: le jeu de données synop_201507 présente des valeurs manquantes
        jeu de Données créé : synop_201508
        Attention: le jeu de données synop_201508 présente des valeurs manquantes
        jeu de Données créé : synop_201509
        Attention: le jeu de données synop_201509 présente des valeurs manquantes
        jeu de Données créé : synop_201510
        Attention: le jeu de données synop_201510 présente des valeurs manquantes
        jeu de Données créé : synop_201511
        Attention: le jeu de données synop_201511 présente des valeurs manquantes
        jeu de Données créé : synop_201512
        Attention: le jeu de données synop_201512 présente des valeurs manquantes
        jeu de Données créé : synop_201601
        Attention: le jeu de données synop_201601 présente des valeurs manquantes
        jeu de Données créé : synop_201602
        Attention: le jeu de données synop_201602 présente des valeurs manquantes
        jeu de Données créé : synop_201603
        Attention: le jeu de données synop_201603 présente des valeurs manquantes
        jeu de Données créé : synop_201604
        Attention: le jeu de données synop_201604 présente des valeurs manquantes
        jeu de Données créé : synop_201605
        Attention: le jeu de données synop_201605 présente des valeurs manquantes
        jeu de Données créé : synop_201606
        Attention: le jeu de données synop_201606 présente des valeurs manquantes
        jeu de Données créé : synop_201607
        Attention: le jeu de données synop_201607 présente des valeurs manquantes
        jeu de Données créé : synop_201608
        Attention: le jeu de données synop_201608 présente des valeurs manquantes
        jeu de Données créé : synop_201609
        Attention: le jeu de données synop_201609 présente des valeurs manquantes
        jeu de Données créé : synop_201610
        Attention: le jeu de données synop_201610 présente des valeurs manquantes
        jeu de Données créé : synop_201611
        Attention: le jeu de données synop_201611 présente des valeurs manquantes
        jeu de Données créé : synop_201612
        Attention: le jeu de données synop_201612 présente des valeurs manquantes
        jeu de Données créé : synop_201701
        Attention: le jeu de données synop_201701 présente des valeurs manquantes
        jeu de Données créé : synop_201702
        Attention: le jeu de données synop_201702 présente des valeurs manquantes
        jeu de Données créé : synop_201703
        Attention: le jeu de données synop_201703 présente des valeurs manquantes
        jeu de Données créé : synop_201704
        Attention: le jeu de données synop_201704 présente des valeurs manquantes
        jeu de Données créé : synop_201705
        Attention: le jeu de données synop_201705 présente des valeurs manquantes
        jeu de Données créé : synop_201706
        Attention: le jeu de données synop_201706 présente des valeurs manquantes
        jeu de Données créé : synop_201707
        Attention: le jeu de données synop_201707 présente des valeurs manquantes
        jeu de Données créé : synop_201708
        Attention: le jeu de données synop_201708 présente des valeurs manquantes
        jeu de Données créé : synop_201709
        Attention: le jeu de données synop_201709 présente des valeurs manquantes
        jeu de Données créé : synop_201710
        Attention: le jeu de données synop_201710 présente des valeurs manquantes
        jeu de Données créé : synop_201711
        Attention: le jeu de données synop_201711 présente des valeurs manquantes
        jeu de Données créé : synop_201712
        Attention: le jeu de données synop_201712 présente des valeurs manquantes
        jeu de Données créé : synop_201801
        Attention: le jeu de données synop_201801 présente des valeurs manquantes
        jeu de Données créé : synop_201802
        Attention: le jeu de données synop_201802 présente des valeurs manquantes
        jeu de Données créé : synop_201803
        Attention: le jeu de données synop_201803 présente des valeurs manquantes
        jeu de Données créé : synop_201804
        Attention: le jeu de données synop_201804 présente des valeurs manquantes
        jeu de Données créé : synop_201805
        Attention: le jeu de données synop_201805 présente des valeurs manquantes
        jeu de Données créé : synop_201806
        Attention: le jeu de données synop_201806 présente des valeurs manquantes
        jeu de Données créé : synop_201807
        Attention: le jeu de données synop_201807 présente des valeurs manquantes
        jeu de Données créé : synop_201808
        Attention: le jeu de données synop_201808 présente des valeurs manquantes
        jeu de Données créé : synop_201809
        Attention: le jeu de données synop_201809 présente des valeurs manquantes
        jeu de Données créé : synop_201810
        Attention: le jeu de données synop_201810 présente des valeurs manquantes
        jeu de Données créé : synop_201811
        Attention: le jeu de données synop_201811 présente des valeurs manquantes
        jeu de Données créé : synop_201812
        Attention: le jeu de données synop_201812 présente des valeurs manquantes
        jeu de Données créé : synop_201901
        Attention: le jeu de données synop_201901 présente des valeurs manquantes
        jeu de Données créé : synop_201902
        Attention: le jeu de données synop_201902 présente des valeurs manquantes
        jeu de Données créé : synop_201903
        Attention: le jeu de données synop_201903 présente des valeurs manquantes
        jeu de Données créé : synop_201904
        Attention: le jeu de données synop_201904 présente des valeurs manquantes
        jeu de Données créé : synop_201905
        Attention: le jeu de données synop_201905 présente des valeurs manquantes
        jeu de Données créé : synop_201906
        Attention: le jeu de données synop_201906 présente des valeurs manquantes
        jeu de Données créé : synop_201907
        Attention: le jeu de données synop_201907 présente des valeurs manquantes
        jeu de Données créé : synop_201908
        Attention: le jeu de données synop_201908 présente des valeurs manquantes
        jeu de Données créé : synop_201909
        Attention: le jeu de données synop_201909 présente des valeurs manquantes
        jeu de Données créé : synop_201910
        Attention: le jeu de données synop_201910 présente des valeurs manquantes
        jeu de Données créé : synop_201911
        Attention: le jeu de données synop_201911 présente des valeurs manquantes
        jeu de Données créé : synop_201912
        Attention: le jeu de données synop_201912 présente des valeurs manquantes
        jeu de Données créé : synop_202001
        Attention: le jeu de données synop_202001 présente des valeurs manquantes
        jeu de Données créé : synop_202002
        Attention: le jeu de données synop_202002 présente des valeurs manquantes
        jeu de Données créé : synop_202003
        Attention: le jeu de données synop_202003 présente des valeurs manquantes
        jeu de Données créé : synop_202004
        Attention: le jeu de données synop_202004 présente des valeurs manquantes
        jeu de Données créé : synop_202005
        Attention: le jeu de données synop_202005 présente des valeurs manquantes
        jeu de Données créé : synop_202006
        Attention: le jeu de données synop_202006 présente des valeurs manquantes
        jeu de Données créé : synop_202007
        Attention: le jeu de données synop_202007 présente des valeurs manquantes
        jeu de Données créé : synop_202008
        Attention: le jeu de données synop_202008 présente des valeurs manquantes
        jeu de Données créé : synop_202009
        Attention: le jeu de données synop_202009 présente des valeurs manquantes
        jeu de Données créé : synop_202010
        Attention: le jeu de données synop_202010 présente des valeurs manquantes
        jeu de Données créé : synop_202011
        Attention: le jeu de données synop_202011 présente des valeurs manquantes
        jeu de Données créé : synop_202012
        Attention: le jeu de données synop_202012 présente des valeurs manquantes
        jeu de Données créé : synop_202101
        Attention: le jeu de données synop_202101 présente des valeurs manquantes
        jeu de Données créé : synop_202102
        Attention: le jeu de données synop_202102 présente des valeurs manquantes
        jeu de Données créé : synop_202103
        Attention: le jeu de données synop_202103 présente des valeurs manquantes
        jeu de Données créé : synop_202104
        Attention: le jeu de données synop_202104 présente des valeurs manquantes
        jeu de Données créé : synop_202105
        Attention: le jeu de données synop_202105 présente des valeurs manquantes
        jeu de Données créé : synop_202106
        Attention: le jeu de données synop_202106 présente des valeurs manquantes
        jeu de Données créé : synop_202107
        Attention: le jeu de données synop_202107 présente des valeurs manquantes
        jeu de Données créé : synop_202108
        Attention: le jeu de données synop_202108 présente des valeurs manquantes
        jeu de Données créé : synop_202109
        Attention: le jeu de données synop_202109 présente des valeurs manquantes
        jeu de Données créé : synop_202110
        Attention: le jeu de données synop_202110 présente des valeurs manquantes
        jeu de Données créé : synop_202111
        Attention: le jeu de données synop_202111 présente des valeurs manquantes
        jeu de Données créé : synop_202112
        Attention: le jeu de données synop_202112 présente des valeurs manquantes
        jeu de Données créé : synop_202201
        Attention: le jeu de données synop_202201 présente des valeurs manquantes
        jeu de Données créé : synop_202202
        Attention: le jeu de données synop_202202 présente des valeurs manquantes
        jeu de Données créé : synop_202203
        Attention: le jeu de données synop_202203 présente des valeurs manquantes



        """
        # >>> nom_fichier=['synop.201301.csv.gz']
        # >>> nom_fichier=['all']
        # >>> delimiteur = ';'
        # >>> ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True).charge()

        #le nom du dossier sans l'extension
        #nom_du_dossier = chemin_dossier.split('\\')[-1].split('.')[0]
        #On récupère la liste de TOUS les fichiers (avec le chemin absolu) contenus dans le
        # dossier donnés en paramètre
        fichiers_trouves = {}
        for repertoire, sous_repertoire, fichiers in os.walk(self.chemin_dossier):
            for fichier in fichiers:
                fichiers_trouves[fichier] = os.path.abspath(f"{repertoire}/{fichier}")
        fichiers_conserves = {}
        for key, value in fichiers_trouves.items():
            #Si le nom de fichier a une extension 'csv.gz' on le garde
            if value.split('\\')[-1].split('.')[-2:] == ['csv', 'gz']:

                fichiers_conserves[key] = value

        fichiers_conserves_2 ={}
        #On conserve soit tous les fichiers, soit uniquement ceux entrés dans le paramètre noms_fichiers
        if self.noms_fichiers == ['all']:
            fichiers_conserves_2 = fichiers_conserves
        else:
            # On vérifie que tous les fichiers demandés sont présents dans le dossier sélectionné
            # for key, value in fichiers_conserves.items():
            #     print(value)
            liste_fichiers_conserves = [value.split('\\')[-1] for key, value in fichiers_conserves.items()] #liste des fichiers conservés
            for fichier_demande in self.noms_fichiers:
                if fichier_demande not in liste_fichiers_conserves:
                    if fichier_demande != ['all']:
                        raise Exception("Un fichier demandé n'est pas dans le dossier sélectionné.")
            # on garde uniquement les fichiers demandés parmi l'ensemble des fichiers conservés
            for key, value in fichiers_conserves.items():
                nom_du_fichier_conserve = value.split('\\')[-1]
                if nom_du_fichier_conserve in self.noms_fichiers:
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
                synopreader = csv.reader(gzfile.readlines()[0:3], delimiter = self.delim)
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


            if self.header: #Si le fichier fourni contient les noms de variables
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
            print("jeu de Données créé : " +  nom_donnees)
            globals()[nom_donnees] = Donnees(nom= nom_donnees ,variables= variables, data= data)
            globals()[nom_donnees].del_var(['']) #on supprime la dernière colonne qui est vide car c'était dans le CSV d'origine
            # print((globals()[nom_donnees]))

            #Création d'un inventaire des jeux de données existant
            if 'inventaire' not in globals().keys():
                global inventaire
                inventaire = []
                inventaire.append(globals()[nom_donnees])
            else:
                inventaire.append(globals()[nom_donnees])
            # RESTRICTION : que se passe-t-il si l'utilisateur change le nom de données

            #message pour l'introduction de valeurs manquantes
            if introduction_nan:
                print("Attention: valeurs manquantes introduites lors de la"
                      "création du jeu de données " f'{globals()[nom_donnees].nom}')

            elif presence_na:
                print("Attention: le jeu de données "f'{globals()[nom_donnees].nom} ' "présente des valeurs manquantes")


    def ope(self, pipeline):
        """prend une pipeline et ne renvoie rien

        Examples
        --------
        >>> import os
        >>> from pathlib import Path
        >>> path = Path(os.getcwd()).parent.parent.absolute()
        >>> chemin_dossier = str(path) + "\\Fichiers de Données .csv.gz-20220405"
        >>> nom_fichier=['synop.201301.csv.gz']
        >>> delimiteur = ';'
        >>> from src.pipeline import Pipeline
        >>> pipeline1 = Pipeline([ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True)]) # doctest:+ELLIPSIS
        >>> isinstance(pipeline1, Pipeline)
        True



        """


        if self.noms_fichiers == 'all':
            raise Exception("Vous n'avez pas spécifié de Données à charger.")
        else:
            self.charge()
            jeu_de_donnees = globals()[self.noms_fichiers[0]]
            pipeline.resultat = jeu_de_donnees




if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose = False)
    # print(globals().keys())
    print([Donnees.nom for Donnees in globals()['inventaire']])


