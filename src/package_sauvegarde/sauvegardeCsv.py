''' Classe SauvegardeCsv '''

from src.package_sauvegarde.sauvegarde import Sauvegarde
import numpy as np


class SauvegardeCsv(Sauvegarde):
    """_summary_

    Attributes
    ----------
    chemin : str
            chemin du dossier où l'on veut sauvegarder
    nom : str
        nom sous lequel on veut sauvegarder les données
    Examples
    --------
    >>> from src.donnees  import Donnees
    >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
    >>> import os
    >>> from pathlib import Path
    >>> from src.package_sauvegarde.sauvegardeCsv import SauvegardeCsv
    >>> chemin = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD"
    >>> nom = "mon_test_export"
    >>> isinstance(SauvegardeCsv(chemin, nom), SauvegardeCsv)
    True
    """

    def ope(self, pipeline):
        """ce que fait la sauvegarde

        Parameters
        ----------
        pipeline : Pipeline
            la Pipeline dont il faut sauvegarder les données

        Examples
        --------
        >>> import numpy as np
        >>> from src.donnees import Donnees
        >>> from src.pipeline import Pipeline
        >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
        >>> isinstance(mes_donnees, Donnees)
        True
        >>> import os
        >>> from pathlib import Path
        >>> chemin = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD"
        >>> nom = "mon_test_export"
        >>> mon_pipeline = Pipeline([SauvegardeCsv(chemin, nom)], mes_donnees)
        >>> mon_pipeline.resultat.variables
        ['nom', 'valeur']
        >>> mon_pipeline.resultat.nom
        'mon_nom_jeu_de_donnees'
        >>> isinstance(mon_pipeline.resultat.data, np.ndarray)
        True
        >>> mon_pipeline.execute() # doctest:+ELLIPSIS
        >>> nom2 = "synop.201301_csv"
        >>> chemin_dossier = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD\\Fichiers de Données .csv.gz-20220405"
        >>> nom_fichier=['synop.201301.csv.gz']
        >>> delimiteur = ';'
        >>> from src.package_chargement.chargement_csv import ChargementCsv
        >>> mes_donnees = ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True).charge()
        jeu de Données créé issu d'un .csv: synop_201301
        Attention: le jeu de données synop_201301 présente des valeurs manquantes
        >>> import os
        >>> chemin_dossier = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD\\Fichiers de Données .csv.gz-20220405"
        >>> nom_fichier=['synop.201301.csv.gz']
        >>> delimiteur = ';'
        >>> from src.pipeline import Pipeline
        >>> pipeline1 = Pipeline([ChargementCsv(chemin_dossier, nom_fichier, delimiteur, True)])
        >>> pipeline1.add_ope(SauvegardeCsv(chemin, nom2))
        >>> result = pipeline1.execute() # doctest:+ELLIPSIS
        jeu de Données créé issu d'un .csv: synop_201301
        Attention: le jeu de données synop_201301 présente des valeurs manquantes
        >>> chemin_dossier = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD\\Fichiers de Données .json.gz-20220405\\données_électricité"
        >>> nom_fichier=['2013-01.json.gz']
        >>> nom2 = "201301_json"
        >>> from src.package_chargement.chargement_json import ChargementJson
        >>> pipeline2 = Pipeline([ChargementJson(chemin_dossier, nom_fichier, delimiteur, True), SauvegardeCsv(chemin, nom2)])
        >>> result = pipeline2.execute() # doctest:+ELLIPSIS
        jeu de Données créé issu d'un .json : 2013-01


        """
        chemin_complet = self.chemin + "\\" + self.nom +".csv"
        np.savetxt(chemin_complet, pipeline.resultat.data, delimiter=",", header = ','.join(pipeline.resultat.variables),  fmt='%s', encoding = 'utf8')


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)
    # print(globals().keys())

