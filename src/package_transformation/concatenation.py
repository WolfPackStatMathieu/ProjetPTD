from src.pipeline import Pipeline
from transformation import Transformation

class Concatenation(Transformation):
    '''classe de l'opération de concatenation qui permet de concaténer une liste de données en entrée

    Parameters
    ----------
    variables : list[Donnees]
        Liste des données à rentrer
    Attributes
    ----------

    variables : list[Donnees]
        Liste des données à rentrer

    Examples
    --------
    >>> import numpy as np
    >>> mes_donnees_1 = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
    >>> mes_donnees_2 = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',2], ['b', 6 ], ['c',10]])
    >>> mes_donnees_3 = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',3], ['b', 7 ], ['c',8]])
    >>> mon_pipeline = Pipeline([Concatenation([mes_donnees_2,mes_donnees_3])], mes_donnes_1)
    >>> mon_pipeline.execute()
    >>> print(mon_pipeline)
    [['a' 1]
     ['b' 5]
     ['c' 9]
     ['a' 2]
     ['b' 6]
     ['c' 10]
     ['a' 3]
     ['b' 7]
     ['c' 8]]  


    '''
    def __init__(self, liste_donnees):
        self.donnees = liste_donnees
    
    def ope(self, pipeline : Pipeline):
        '''méthode qui permet d'exécuter l'opération

        Parameters
        ----------
        pipeline : Pipeline
            pipeline sur lequel s'éxecute l'opération
        '''
        for d in self.donnees:
            pipeline.resultat.concat(d)
