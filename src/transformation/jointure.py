'''module pour joindre un jeu de donnes avec un autre'''
from transformation import Transformation

class Jointure(Transformation):
    '''classe d'opération permettant de joindre des jeux de donnees'''
    def __init__(self,autre_donnees, keys ): 
        self.autre_donnes = autre_donnees
        self.keys = keys
    
    def ope(pipeline):
        
    
