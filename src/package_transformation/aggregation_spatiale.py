from src.pipeline import Pipeline
from transformation import Transformation

class Aggreg(Transformation):
    def __init__(self, space_var, echelle = "region") :
        self.echelle = echelle
        self.space_var = space_var

    def ope(self,pipeline : Pipeline):
        def aggregat():
            pass
        pipeline.resultat.transform(self.space_var + "_aggr",[self.space_var],aggregat())
