''''
module carte.py
'''
import os
from src.package_affichage.affichage import Affichage
from src.package_affichage.affichage import CartoPlot


class Carte(Affichage):
    """Classe permettant l'affichage puis l'export d'une variable d'un jeu de
    données sur un fond de carte. Le jeu de données doit comporter une variable
    géographique (code du département ou nom de la région)

    Parameters
    ----------
    chemin_fichier : str
        Le chemin du fichier résultat de l'export

    Attributes
    ----------
    chemin_fichier : str
        Le chemin du fichier résultat de l'export
    """
    def __init__(self, variables, chemin_fichier, nom_variable):
        """constructeur

        Args:
            variables (list): liste des variables
            chemin_fichier (str): chemin du fichier
            nom_variable (str): variable à utiliser
        """
        self.variables = variables
        self.chemin_fichier = chemin_fichier
        self.nom_variable = nom_variable

    def ope(self, pipeline):
        """renvoie un jeu de données sur un fond de carte

        Args:
            pipeline (Pipeline): un pipeline
        """


        var_geo = ("dep", "nomReg")  # Possibilités de noms de variables
        # Liste de bool indiquant si la var est présente

        presence_var_geo = [(nom in var_geo) for nom in self.variables]

        donnees = pipeline.resultat.data

        nom_var_geo = self.variables[presence_var_geo.index(True)]

        echelon = nom_var_geo if nom_var_geo == "dep" else "reg"

        index_var_geo = self.variables.index(nom_var_geo)
        index_var_plot = self.variables.index(self.nom_variable)
        # Dictionnaire des données à afficher sur la carte
        data = {donnees[i, index_var_geo]: donnees[i, index_var_plot]
                for i in range(donnees.shape[0])}
        if echelon == "dep" and '69' in data:
            data['69D'] = data.pop('69')
            data['69M'] = data['69D']
        fig = eval("CartoPlot().plot_" + echelon + "_map(data)")
        fig.show()
        input()
        fig.savefig(self.chemin_fichier)


