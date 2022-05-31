
from src.pipeline import Pipeline
from transformation import Transformation
from package_estimation.moyenne import Moyenne

class Centrage(Transformation):
    '''classe de l'opération de centrage qui permet de soustraire la moyenne à toute les valeurs

    Parameters
    ----------
    variables : list[str]
        Liste des noms de variables à centrer
    Attributes
    ----------

    variables : list[str]
        Liste des noms de variables à centrer

    Examples
    --------
    >>> import numpy as np
    >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
    >>> mon_pipeline = Pipeline([Centrage(['valeur'])], mes_donnes)
    >>> mon_pipeline.execute()
    >>> print(mon_pipeline)
    [['a' -4.0]
     ['b' 0.0]
     ['c' 4.0]]    


    '''
    def __init__(self, variables):
        self.variables = variables

    def ope(self,pipeline : Pipeline):
        '''méthode qui permet d'exécuter l'opération

        Parameters
        ----------
        pipeline : Pipeline
            pipeline sur lequel s'éxecute l'opération
        '''
        for v in self.variables:
            v_moyen = Moyenne([v]).ope(Pipeline('',[v],pipeline.resultat.data[:][pipeline.resultat.get_var(v)]))
            pipeline.resultat.transform(v +'_centr',[v],lambda x : x[0] - v_moyen)

if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)