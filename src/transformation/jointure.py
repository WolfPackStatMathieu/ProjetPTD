'''module pour joindre un jeu de donnes avec un autre'''
from src.donnees import Donnees
from src.pipeline import Pipeline
import numpy as np
from transformation import Transformation
from transformation import Transformation
class Jointure(Transformation):
    '''classe d'opération permettant de joindre à gauche des jeux de donnees'''
    def __init__(self,autre_donnees : Donnees, keys ): 
        self.autre_donnes = autre_donnees
        self.keys = keys
    
    def ope(self,pipeline : Pipeline):
        variables_suppl = []
        for v in self.autre_donnes.variables :
            if v in pipeline.resultat.variables :
                if not v in self.keys:
                    raise Exception("Collusion de variables dans la jointure : " + v)
            else : 
                variables_suppl.append(v)
        # on va supposer que la clé est identifiant (unicité par clé)
        ajout =  np.full((pipeline.resultat.data.shape[0], len(variables_suppl)), np.nan, dtype= object)

        def test (i, k):
            for v in variables_suppl :
                if self.autre_donnes.data[i][self.autre_donnes.get_var(v)] != pipeline.resultat.data[k][pipeline.resultat.get_var(v)] :
                    return False
            return True
        
        def parcours(i):
            for k in range(pipeline.resultat.data.shape[0]):
                if test(i,k):
                    for n in len(variables_suppl):
                        v = variables_suppl[n]
                        ajout[k][n] = self.autre_donnes.data[i][self.autre_donnes.get_var(v)] 
                    return


        for i in range(np.shape(self.autre_donnes.data)[0]):
            parcours(i)
        pipeline.resultat.add_var(variables_suppl,ajout)
        
