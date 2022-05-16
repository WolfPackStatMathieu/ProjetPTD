from boto import set_file_logger
from numpy import array
from donnees import np, Donnees
from operation import Operation
''''''
class Pipeline:
    def __init__(self,etapes, resultat = Donnees([],array([]))) :
        self.etapes = etapes
        self.resultat = resultat
    
    def get_pip(self):
        print(self.etapes)
        return self.etapes
    
    def get_res(self):
        print(self.resultat)
        return self.resultat
    
    def add_ope(self,operation):
        self.etapes.append(operation)
    
    def del_ope(self):
        self.etapes.pop()
    
    def fus_pip(self, autre_pipeline):
        self.etapes += autre_pipeline.etapes
    
    def execute(self):
        for commande in self.etapes :
            commande.ope(self.resultat)
        

    
