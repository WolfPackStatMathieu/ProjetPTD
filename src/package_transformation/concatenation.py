from src.pipeline import Pipeline
from transformation import Transformation

class Concatenation(Transformation):

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
