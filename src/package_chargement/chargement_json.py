import os
import gzip
import json
import numpy as np
from src.package_chargement.chargement import Chargement

#Dossier où se trouve le fichier :
# folder = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD\\"
# filename = "2013-01.json.gz"
# with gzip.open(folder + filename, mode = 'rt') as gzfile :
#     data = json.load(gzfile)

# with gzip.open(folder + filename, 'rb') as f:
#     line = f.readline()
#     one_line = json.loads(line)
#     print(one_line)

class ChargementJson(Chargement):
    """Permet le chargement de jeux de données à partir d'un dossier
    archivant des fichiers .json

    Ce module charge en mémoire autant de jeux de Données que de
    fichiers json présents dans le dossier

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

    Examples
    --------
    >>> import os
    >>> from pathlib import Path
    >>> path = Path(os.getcwd()).parent.parent.absolute()
    >>> chemin_dossier = str(path) + "\\Fichiers de Données .json.gz-20220405"
    >>> nom_fichier='2013-01.json.gz'
    >>> delimiteur = ';'
    >>> mon_chargementJson = ChargementJson(chemin_dossier, nom_fichier, delimiteur, True)
    >>> print(mon_chargementJson.delim)
    ;
    >>> isinstance(mon_chargementJson, ChargementJson)
    True





    """
    def __init__(self, chemin_dossier, noms_fichiers, delim =';', header=True):
        """Constructeur de ChargementJson

        Parameters
        ----------
        chemin_dossier : str
            chemin du dossier où sont situés les fichiers à charger
        noms_fichiers : list[str]
            vaut ['all'] par défaut pour charger tous les fichiers de type csv
        présents dans le dossier d'archivage
        delim : str, optional
            Caractère séparant les colonnes, by default ';'
        header : bool, optional
            vaut True si la première ligne contient le nom des variables, by default True

        Example
        -------
        >>> import os
        >>> from pathlib import Path
        >>> path = Path(os.getcwd()).parent.parent.absolute()
        >>> chemin_dossier = str(path) + "\\Fichiers de Données .json.gz-20220405"
        >>> nom_fichier='2013-01.json.gz'
        >>> delimiteur = ';'
        >>> mon_chargementJson = ChargementJson(chemin_dossier, nom_fichier, delimiteur, True)
        >>> print(mon_chargementJson.delim)
        ;
        >>> print(mon_chargementJson.noms_fichiers)
        2013-01.json.gz


        """
        self.chemin_dossier = chemin_dossier
        self.noms_fichiers = noms_fichiers
        self.delim = delim
        self.header = header

    def charge(self):
        """_summary_

        Returns
        -------
        _type_
            _description_

        Raises
        ------
        Exception
            _description_
        Examples
        --------
        >>> import os
        >>> from pathlib import Path
        >>> path = Path(os.getcwd()).absolute()
        >>> chemin_dossier = str(path)+ "\\Fichiers de Données .json.gz-20220405" + "\\données_électricité"
        >>> nom_fichier=['2013-01.json.gz']
        >>> delimiteur = ';'
        >>> mon_chargementJson = ChargementJson(chemin_dossier, nom_fichier, delimiteur, True)
        >>> mon_chargementJson.charge()

        """
        #On récupère la liste de TOUS les fichiers (avec le chemin absolu
        # )contenus dans le dossier donné en paramètre
        fichiers_trouves = {}
        for repertoire, sous_repertoire, fichiers in os.walk(self.chemin_dossier):
            for fichier in fichiers:
                fichiers_trouves[fichier] = os.path.abspath(f"{repertoire}/{fichier}")

        fichiers_conserves = {}
        for key, value in fichiers_trouves.items():
            #Si le nom de fichier a une extension 'json.gz' on le garde
            if value.split('\\')[-1].split('.')[-2:] == ['json', 'gz']:

                fichiers_conserves[key] = value

        fichiers_conserves_2 ={}
        #On conserve soit tous les fichiers, soit uniquement ceux entrés dans le paramètre noms_fichiers
        if self.noms_fichiers == ['all']:
            fichiers_conserves_2 = fichiers_conserves #on conserve tous les fichiers
        else:
            # On vérifie que tous les fichiers demandés sont présents dans le dossier sélectionné
            liste_fichiers_conserves = [value.split('\\')[-1] for key, value in fichiers_conserves.items()] #liste des fichiers conservés
            for fichier_demande in self.noms_fichiers: #on parcourt la liste des fichiers demandés
                    if fichier_demande not in liste_fichiers_conserves: # si le fichier demandé n'est pas dans la liste des fichiers du dossier
                        if fichier_demande != ['all']:
                            raise Exception("Un fichier demandé n'est pas dans le dossier sélectionné.")
            # on garde uniquement les fichiers demandés parmi l'ensemble des fichiers conservés
            for key, value in fichiers_conserves.items():
                nom_du_fichier_conserve = value.split('\\')[-1]
                if nom_du_fichier_conserve  in self.noms_fichiers: #si le nom du fichier conservé fait partie de la liste des fichiers demandés
                    fichiers_conserves_2[key] = value #alors on l'ajoute au dictionnaire

        # retour pour la doctest
        # for key, value in fichiers_conserves_2.items():
        #     print(value.split('\\')[-1:][0]) # le nom du fichier
        #     print(value.split('\\')[-1:][0].split('.')[0]) #la date du fichier


        for fichier, chemin in fichiers_conserves_2.items():

            #On crée un jeu de données pour chaque fichier
            donnees = [] #pour récupérer les données qui iront dans le
            #constructeur de Donnees
            presence_na = False # indique la présence de données manquantes
            with gzip.open(chemin) as gzfile:
                data = json.load(gzfile)
                # print("data est de type : " + str(type(data)))
                #On va récupérer la liste des variables du fichier
                variables_dict = [] # les données qui nous intéressent
                variables = [] # Les clés de chaque ligne de data
                cle_dict = [] # les clés qui sont également des dictionnaires

                # Chaque ligne de data est un dictionnaire
                # On itère pour récupérer les clés
                for row in data:
                    for cle in row.keys():
                        if cle not in variables and not isinstance(row[cle], dict):
                            variables.append(cle)
                        # On sait qu'une seule des clés ("fields") est en fait elle-même un
                        # dictionnaire et contient les données
                        if isinstance(row[cle], dict):
                            for cle_dict in row[cle].keys():
                                if cle_dict not in variables_dict:
                                    variables_dict.append(cle_dict)
                # On recommence à itérer sur les lignes pour obtenir les données
                # print(variables_dict)
                # print(variables)
                variables += variables_dict # Concaténation de toutes les variables qui ont une valeur unique (pas un dictionnaire derrière)
                # print("variables :"+ str(variables))

                donnees_fichier = np.array(variables, dtype=object) # la première ligne est constituée des variables du fichier
                # print(donnees_fichier)
                # print([donnees_fichier[i] for i in range(0,np.shape(donnees_fichier)[0])])
                for row in data[:2]: # On parcourt chaque ligne de data
                    for cle in row.keys(): # on parcourt chaque clé
                        if isinstance(row[cle], dict):
                            result = row[cle].items()
                            tableau_ligne = list(result)
                             #On transpose le tableau pour obtenir la liste des variables et celle des valeurs de la row
                            tableau_ligne = np.array(tableau_ligne, dtype=object)
                            tableau_ligne = np.transpose(tableau_ligne)
                            variables_ligne = list(tableau_ligne[0])
                            data_ligne = list(tableau_ligne[1])
                            ligne_complete = [np.nan for i in enumerate(variables)]

                            for i, variable in enumerate(variables):
                                if variable in variables_ligne:
                                    #index de la variable dans la ligne de variables complète
                                    index_variable = variables_ligne.index(variable)
                                    # on récupère sa valeur et on la place au bon endroit dans la ligne_complete
                                    ligne_complete[index_variable] = data_ligne[index_variable]
                            print(ligne_complete)




# variables =['datasetid', 'recordid', 'fields', 'code_insee_region', 'date', 'region', 'date_heure', 'heure', 'record_timestamp', 'consommation_brute_gaz_terega', 'statut_terega', 'consommation_brute_electricite_rte', 'statut_rte', 'consommation_brute_gaz_grtgaz', 'consommation_brute_totale', 'consommation_brute_gaz_totale', 'statut_grtgaz']

if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose = False)