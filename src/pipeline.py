'''module contenant la classe de la pipeline'''

from numpy import array
# import operation
from src.donnees  import Donnees

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
        jeu de donnees sur lequel s'appliquent les etapes

    Examples
    --------
    >>> import numpy as np
    >>> test = Pipeline([])
    '''
    def __init__(self,etapes, resultat = Donnees('vide',[],[])) :
        self.etapes = etapes
        self.resultat = resultat

    def get_pip(self):
        '''renvoie les etapes
        '''
        print(self.etapes)
        return self.etapes

    def get_res(self):
        ''' renvoie l'attribut resultat'''
        print(self.resultat)
        return self.resultat

    def add_ope(self,operation):
        ''' ajoute une opération aux étapes'''
        self.etapes.append(operation)

    def del_ope(self):
        ''' enleve la derniere operation aux etapes'''
        self.etapes.pop()

    def fus_pip(self, autre_pipeline):
        ''' ajoute les etapes d'une autre pipeline apres les etapes de la pipeline'''
        self.etapes += autre_pipeline.etapes

    def execute(self):
        ''' execute l'ensemble des etapes'''
        for commande in self.etapes :
            commande.ope(self)




