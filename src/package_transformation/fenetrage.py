from transformation import Transformation
from src.pipeline import Pipeline

class Fenetrage(Transformation):

    def __init__(self,debut, fin, time_var,echantillon = "keep", ):
        self.debut = debut
        self.fin = fin
        self.time_var = time_var
        self.echantillon = echantillon

    def ope(self, pipeline : Pipeline):
        pipeline.resultat.filtre([self.time_var], lambda x : self.debut <= x[0] <= self.fin)

if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)

