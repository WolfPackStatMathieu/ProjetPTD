'''module de selection des variables'''
from transformation import Transformation
from src.pipeline import Pipeline

class Selection__var(Transformation):

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