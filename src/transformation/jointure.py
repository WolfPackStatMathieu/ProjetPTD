'''module pour joindre un jeu de donnes avec un autre'''
from src.pipeline import Pipeline
from transformation import Transformation

class Jointure(Transformation):
    '''classe d'opération permettant de joindre des jeux de donnees'''
    def __init__(self,autre_donnees, keys ): 
        self.autre_donnes = autre_donnees
        self.keys = keys
    
    def ope(self,pipeline : Pipeline):
        variables_suppl = []
        for v in self.autre_donnes :
            if v in pipeline.resultat.variables :
                if not v in self.keys:
                    raise Exception("Collusion de variables dans la jointure : " + v)
            else : 
                variables_suppl.append(v)
        
        
        
