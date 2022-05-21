'''module pour joindre un jeu de donnese abev un autre'''
from transformation import Transformation

class Jointure(Transformation):
    '''classe d'opération permettant de joindre des jeux de donnees'''
    def __init__(self,autre_donnees ): 
        self.autre_donnes = autre_donnees
    
    
