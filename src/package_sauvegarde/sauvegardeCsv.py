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
    >>> chemin = str(Path(os.getcwd()).absolute())
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
        >>> chemin = str(Path(os.getcwd()).absolute())
        >>> nom = "mon_test_export"
        >>> mon_pipeline = Pipeline([SauvegardeCsv(chemin, nom)], mes_donnees)
        >>> mon_pipeline.resultat.variables
        ['nom', 'valeur']
        >>> mon_pipeline.resultat.nom
        'mon_nom_jeu_de_donnees'
        >>> isinstance(mon_pipeline.resultat.data, np.ndarray)
        True


        """
        chemin_complet = self.chemin + "\\" + self.nom +".csv"
        np.savetxt(chemin_complet, pipeline.resultat, delimiter=",", header = pipeline.resultat.variables)


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)

