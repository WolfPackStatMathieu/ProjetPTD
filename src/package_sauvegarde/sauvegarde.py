'''classe sauvegarde'''

from src.operation  import Operation


class Sauvegarde(Operation):
    """classe sauvegarde

    Attributes
    ----------

    """
    def __init__(self, chemin, nom):
        """constructeur de sauvegarde

        Parameters
        ----------
        chemin : str
            chemin du dossier où l'on veut sauvegarder
        nom : str
            nom sous lequel on veut sauvegarder les données
        """
        self.chemin = chemin
        self.nom = nom
