from src.donnees import Donnees
from src.pipeline import Pipeline
from transformation import Transformation
import numpy as np
from estimation.moyenne import Moyenne

class Moyenne_Glissante(Transformation):

    def __init__(self, variables, pas):

        self.variables=variables
        self.pas = pas

    def ope(self, pipeline : Pipeline):
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