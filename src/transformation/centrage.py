from src.pipeline import Pipeline
from transformation import Transformation
from estimation.moyenne import Moyenne

class Centrage(Transformation):

    def __init__(self, variables):
        self.variables = variables
    
    def ope(self,pipeline : Pipeline):
        for v in self.variables:
            v_moyen = Moyenne([v]).ope(Pipeline('',[v],pipeline.resultat.data[:][pipeline.resultat.get_var(v)]))
            pipeline.resultat.transform(v +'_centr',[v],lambda x : x[0] - v_moyen)
