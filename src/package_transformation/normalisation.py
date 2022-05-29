from src.pipeline import Pipeline
from transformation import Transformation
from estimation.moyenne import Moyenne
from estimation.variance import Variance

class Normalisation(Transformation):

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
            v_variance = Variance([v]).ope(Pipeline('',[v],pipeline.resultat.data[:][pipeline.resultat.get_var(v)]))
            pipeline.resultat.transform(v +'_centr',[v],lambda x : (x[0] - v_moyen)/ v_variance)


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)