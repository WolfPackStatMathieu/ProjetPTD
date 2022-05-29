'''module de selection des variables'''
from transformation import Transformation
from src.pipeline import Pipeline

class Selection__var(Transformation):
    '''classe de l'opération de selection de variable qui permet de ne garder que les variables selectionnées

    Parameters
    ----------
    variables : list[str]
        Liste des noms de variables à garder
    Attributes
    ----------

    variables : list[str]
        Liste des noms de variables à garder

    Examples
    --------
    >>> import numpy as np
    >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
    >>> mon_pipeline = Pipeline([Selection_var(['nom'])], mes_donnes)
    >>> mon_pipeline.execute()
    >>> print(mon_pipeline)
    [['a']
     ['b']
     ['c']]    


    '''
    def __init__(self, selection):
        self.selection = selection

    def ope(self,pipeline : Pipeline):
        '''méthode qui permet d'exécuter l'opération

        Parameters
        ----------
        pipeline : Pipeline
            pipeline sur lequel s'éxecute l'opération
        '''
        for v in pipeline.resultat.variables:
            if not v in self.selection:
                pipeline.resultat.del_var([v])
if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)