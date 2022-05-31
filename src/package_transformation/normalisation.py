'''normalise une variable
'''
from src.pipeline import Pipeline
<<<<<<< HEAD
from transformation import Transformation
from package_estimation.moyenne import Moyenne
from package_estimation.variance import Variance
=======
from src.package_transformation.transformation import Transformation
from src.package_estimation.moyenne import Moyenne
from src.package_estimation.variance import Variance
>>>>>>> a921665def220ecf4cba874a3d3a3a950e50efa2

class Normalisation(Transformation):
    '''classe de l'opération de normalisation qui permet de soustraire la moyenne à toute les valeurs et
    diviser par la variance

    Parameters
    ----------
    variables : list[str]
        Liste des noms de variables à normaliser
    Attributes
    ----------

    variables : list[str]
        Liste des noms de variables à normaliser

    Examples
    --------
    >>> import numpy as np
    >>> from src.pipeline import Pipeline
    >>> from src.donnees import Donnees
    >>> from src.package_estimation.moyenne import Moyenne
    >>> from src.package_estimation.variance import Variance
    >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 2], ['c',3]])
    >>> mon_pipeline = Pipeline([Normalisation(['valeur'])], mes_donnees)
    >>> mon_pipeline.execute()
    >>> print(mon_pipeline.resultat)
    [['a' 1 -1.49]
     ['b' 2 0.0]
     ['c' 3 1.49]]


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
            v_moyen = Moyenne([v]).ope(pipeline)[0]
            v_variance = Variance([v]).ope(pipeline)[0]
            pipeline.resultat.transform(v +'_centr',[v],lambda x : round(((x[0] - v_moyen)/ v_variance),2) )


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)