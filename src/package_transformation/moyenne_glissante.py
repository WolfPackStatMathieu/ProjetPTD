from src.donnees import Donnees
from src.pipeline import Pipeline
from transformation import Transformation
import numpy as np
from package_estimation.moyenne import Moyenne

class Moyenne_Glissante(Transformation):
    '''classe d'opération permettant de calculer la moyenne glissante des variables en entrée"

    Parameters
    ----------
    variables : list[string]
        liste des variables à calculer
    pas : int
        pas de la moyenne glissante d'un coté seulement


    Attributes
    ----------

    variables : list[string]
        liste des variables à calculer
    pas : int
        pas de la moyenne glissante d'un coté seulement

    Examples
    --------
    >>> import numpy as np
    >>> mes_donnees_1 = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 2 ], ['c',3],['d',4],['e',5],['f',6]])
    >>> mon_pipeline = Pipeline([Moyenne_Glissante(['valeur'], 1)])
    >>> mon_pipeline.execute()
    >>> print(mon_pipeline)
    [['a' 1 nan]
     ['b' 2 2]
     ['c' 3 3]
     ['d' 4 4]
     ['e' 5 5]
     ['f' 6 nan]]

    '''
    def __init__(self, variables, pas):

        self.variables=variables
        self.pas = pas

    def ope(self, pipeline : Pipeline):
        '''méthode qui permet d'exécuter l'opération

        Parameters
        ----------
        pipeline : Pipeline
            pipeline sur lequel s'éxecute l'opération
        '''
        i = 0
        def glissante(var):
            if i < self.pas or i > pipeline.resultat.data.shape[0] - self.pas - 1 :
                i+=1
                return np.nan
            else:
                j = pipeline.resultat.get_var(var)
                suite=[pipeline.resultat.data[i - self.pas + k ][j] for k in range(2 * self.pas +1)]
                i+=1
                return sum(suite)/len(suite)

        for v in self.variables:
            i = 0
            pipeline.resultat.transform(v +'_gliss',[v],glissante(v))

if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)