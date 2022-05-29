'''module contenant la classe de la pipeline'''

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
    >>> isinstance(test, Pipeline)
    True
    >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
    >>> import os
    >>> from pathlib import Path
    >>> from src.package_sauvegarde.sauvegardeCsv import SauvegardeCsv
    >>> chemin = str(Path(os.getcwd()).absolute())
    >>> nom = "mon_test_export"
    >>> mon_pipeline = Pipeline([SauvegardeCsv(chemin, nom)], mes_donnees)
    >>> mon_pipeline.resultat.variables
    ['nom', 'valeur']
    >>> mon_pipeline.resultat.nom
    'mon_nom_jeu_de_donnees'
    >>> type(mon_pipeline.resultat.data)
    <class 'numpy.ndarray'>
    >>> isinstance(mon_pipeline.resultat.data, np.ndarray)
    True

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

if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)


