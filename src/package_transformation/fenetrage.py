from transformation import Transformation
from src.pipeline import Pipeline
import datetime as dt

class Fenetrage(Transformation):
    '''classe de l'opération de fenetrage qui enleve les observations en dehors de la fenetre temporelle

    Parameters
    ----------
    debut : datetime
        debut de la fenetre temporelle
    fin : datetime
        fin de la fenetre temporelle
    time_var : variable temporelle

    Attributes
    ----------

    debut : datetime
        debut de la fenetre temporelle
    fin : datetime
        fin de la fenetre temporelle
    time_var : variable temporelle

    Examples
    --------
    >>> import numpy as np
    >>> mes_donnees_1 = Donnees('mon_nom_jeu_de_donnees',['nom', 'time'],[['a',dt.datetime(2001,1,1,0,0)], ['b',dt.datetime(2001,1,15,0,0) ], ['c',dt.datetime(2001,5,5,0,0)]])
    >>> mon_pipeline = Pipeline([Fenetrage(dt.datetime(2000,1,1,0,0),dt.datetime(2001,4,1,0,0),'time')], mes_donnes_1)
    >>> mon_pipeline.execute()
    >>> print(mon_pipeline)
    [['a' datetime(2001,1,1,0,0)]
     ['b' datetime(2001,1,15,0,0)]]  


    '''
    def __init__(self,debut, fin, time_var ):
        self.debut = debut
        self.fin = fin
        self.time_var = time_var

    def ope(self, pipeline : Pipeline):
        '''méthode qui permet d'exécuter l'opération

        Parameters
        ----------
        pipeline : Pipeline
            pipeline sur lequel s'éxecute l'opération
        '''
        pipeline.resultat.filtre([self.time_var], lambda x : self.debut <= x[0] <= self.fin)

if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)

