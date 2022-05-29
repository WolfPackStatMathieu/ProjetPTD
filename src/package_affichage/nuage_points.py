from src.package_affichage.affichage import Affichage
import numpy as np
import matplotlib.pyplot as plt



class Nuage_points(Affichage):
    """affiche un nuage de points

    Args:
        Affichage (classe): classe parente

    Attributes:
        variables : list[]
            liste des variables dont on veut afficher le nuage de points
    """
    def __init__(self, variables):
        """constructeur de la classe

        Args:
            variables (liste): liste des variables dont on veut afficher le nuage de points
        """
        self.variables = variables

    def ope(self, pipeline):
        """renvoie un nuage de points

        Args:
            pipeline (Pipeline): un pipeline
        """

        if len(self.variables) != 2:
            raise Exception("Il faut 2 variables. \n You need 2 variables fool")
        else:
            liste_1 = []
            liste_2 = []
            j1 = pipeline.resultat.get_var(self.variables[0])
            j2 = pipeline.resultat.get_var(self.variables[1])
            for i in range(pipeline.resultat.data.shape[0]):
                if not(np.isnan(pipeline.resultat.data[i, j1])) and not(np.isnan(pipeline.resultat.data[i, j2])):
                    liste_1.append(pipeline.resultat.data[i, j1])
                    liste_2.append(pipeline.resultat.data[i, j2])
        plt.scatter(liste_1, liste_2, c='blue')
        plt.xlabel(self.variables[0])
        plt.ylabel(self.variables[1])
        plt.title('Nuage de points')
        plt.show()


