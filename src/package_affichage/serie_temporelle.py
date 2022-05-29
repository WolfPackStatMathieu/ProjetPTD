'''
module serie_temporelle.py
'''
from calendar import c
from math import pi
from this import d
from datetime import date
import numpy as np
import matplotlib.pyplot as plt
import random
from src.package_affichage.affichage import Affichage


class Serie_temporelle(Affichage):
    """classe représentant l'affichage d'une série temporelle

    Args:
        Affichage (classe): classe parente

    Attributes
    ----------
    variables : list
        liste des variables dont on veut afficher une série temporelle
    var_time : str
        variable temporelle
    """
    def __init__(self, variables, var_time):
        """constructeur de la classe

        Args:
            variables (list): liste des variables dont on veut afficher une série temporelle
            var_time (str): variable temporelle
        """
        self.variables = variables
        self.var_time = var_time

    def ope(self, pipeline):
        """renvoie une série temporelle

        Args:
            pipeline (Pipeline): un pipeline
        """
        if pipeline.resultat.var_type(self.var_time) != date:
            raise Exception("Merci de bien entrer une variable temporelle")
        else:
            grosse_liste = []
            k = pipeline.resultat.get_var(self.var_time)
            for v in self.variables:
                petite_liste_x=[]
                petite_liste_y=[]
                j = pipeline.resultat.get_var(v)

                for i in range(pipeline.resultat.data.shape[0]):
                    if not(np.isnan(pipeline.resultat.data[i,j])):
                        petite_liste_y.append(pipeline.resultat.data[i, j])
                        petite_liste_x.append(pipeline.resultat.data[i, k] )

                grosse_liste.append([petite_liste_x, petite_liste_y] )


        couleur = []
        n = len(grosse_liste)
        for i in range(n):
            couleur.append( '#%06X' % random.randint(0, 0xFFFFFF))


        for k in grosse_liste:
            plt.plot(grosse_liste[i][0], grosse_liste[i][1], c = couleur[k])
        plt.title("Série temporelle des données météo et énergie")
        plt.show()






