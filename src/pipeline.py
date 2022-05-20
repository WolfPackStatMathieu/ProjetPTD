'''module contenant la classe de la pipeline'''

from numpy import array
from chargement.Donnees import Donnees
from chargement.chargement_csv import ChargementCsv 
from operation import Operation

class Pipeline:
    '''représente une pipeline

    Parameters
    ----------
    etapes : list[Operations]
        Liste des opérations à appliquer
    resultat : Donnees
        jeu de donnees sur lequel s'apllqiue les etapes

    Attributes
    ----------
    etapes : list[Operations]
        Liste des opérations à appliquer
    resultat : Donnees
        jeu de donnees sur lequel s'apllqiue les etapes

    Examples
    --------
    >>> import numpy as np
    >>> test = Pipeline([])
    '''
    def __init__(self,etapes, resultat = Donnees('vide',[],[])) :
        self.etapes = etapes
        self.resultat = resultat

    def get_pip(self):
        print(self.etapes)
        return self.etapes

    def get_res(self):
        print(self.resultat)
        return self.resultat

    def add_ope(self,operation):
        self.etapes.append(operation)

    def del_ope(self):
        self.etapes.pop()

    def fus_pip(self, autre_pipeline):
        self.etapes += autre_pipeline.etapes

    def execute(self):
        for commande in self.etapes :
            commande.ope(self)



