from src.Donnees import Donnees
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
                
                return np.nan
            else:
                return Moyenne([var]).ope(pipeline)
        