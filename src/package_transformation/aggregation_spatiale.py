from src.pipeline import Pipeline
from transformation import Transformation

class Aggreg(Transformation):
    def __init__(self, space_var,) :
        self.space_var = space_var

    def ope(self,pipeline : Pipeline):
        '''méthode qui permet d'exécuter l'opération

        Parameters
        ----------
        pipeline : Pipeline
            pipeline sur lequel s'éxecute l'opération
        '''
        pipeline.resultat.var_num()
        def aggregat():
            pass
            
        pipeline.resultat.transform("code_insee_region",[self.space_var],aggregat())

if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)
