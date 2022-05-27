import os
import gzip
import json
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
        vaut 'all' par défaut pour charger tous les fichiers de type csv
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
            vaut 'all' par défaut pour charger tous les fichiers de type csv
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
        if self.noms_fichiers == 'all':
            fichiers_conserves_2 = fichiers_conserves #on conserve tous les fichiers
        else:
            # On vérifie que tous les fichiers demandés sont présents dans le dossier sélectionné
            liste_fichiers_conserves = [value.split('\\')[-1] for key, value in fichiers_conserves.items()] #liste des fichiers conservés
            for fichier_demande in self.noms_fichiers: #on parcourt la liste des fichiers demandés
                    if fichier_demande not in liste_fichiers_conserves: # si le fichier demandé n'est pas dans la liste des fichiers du dossier
                        raise Exception("Un fichier demandé n'est pas dans le dossier sélectionné.")
            # on garde uniquement les fichiers demandés parmi l'ensemble des fichiers conservés
            for key, value in fichiers_conserves.items():
                nom_du_fichier_conserve = value.split('\\')[-1]
                if nom_du_fichier_conserve  in self.noms_fichiers: #si le nom du fichier conservé fait partie de la liste des fichiers demandés
                    fichiers_conserves_2[key] = value #alors on l'ajoute au dictionnaire

        # retour pour la doctest
        for key, value in fichiers_conserves_2.items():
            print(value.split('\\')[-1:][0]) # le nom du fichier
            print(value.split('\\')[-1:][0].split('.')[0]) #la date du fichier





        # folder = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD\\"
        # filename = "2013-01.json.gz"
        # with gzip.open(folder + filename, mode = 'rt') as gzfile :
        #     data = json.load(gzfile)

        # with gzip.open(folder + filename, 'rb') as f:
        #     line = f.readline()
        #     one_line = json.loads(line)
        #     print(one_line)

if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose = False)