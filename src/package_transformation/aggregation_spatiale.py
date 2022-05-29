from donnees import Donnees
from src.pipeline import Pipeline
from src.package_transformation.jointure import Jointure
from src.pipeline import Pipeline
from transformation import Transformation

class Aggregation(Transformation):
    '''classe de l'opération de d'aggregation spatiale qui permet d'aggréger des valeurs

    Parameters
    ----------
    space_var : str
        variable d'aggrégation spatiale

    Attributes
    ----------

    space_var : str
        variable d'aggrégation spatiale

    Examples
    --------
    >>> import numpy as np
    >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'numer_sta'],[['a',], ['b', ], ['c',]])
    >>> mon_pipeline = Pipeline([Aggregation(['numer_sta'])], mes_donnes)
    >>> mon_pipeline.execute()
    >>> print(mon_pipeline)
    [['a' -4.0]
     ['b' 0.0]
     ['c' 4.0]]    


    '''
    def __init__(self, space_var,) :
        self.space_var = space_var

    def ope(self,pipeline : Pipeline):
        '''méthode qui permet d'exécuter l'opération

        Parameters
        ----------
        pipeline : Pipeline
            pipeline sur lequel s'éxecute l'opération
        '''
        correspondance = 0
        tableau_joint = Pipeline([Jointure((correspondance,'numer_sta')), pipeline.resultat]).get_res()
        tableau_joint.var_num(['date','code_insee_region'])
        groupement={}
        j = tableau_joint.get_var('code_insee_region')
        k = tableau_joint.get_var('date')
        for i in range(tableau_joint.data.shape[0]):
            cle =[tableau_joint.data[i,j],tableau_joint.data[i,k]]
            if not cle in groupement.keys():
                groupement[cle]= Donnees('',tableau_joint.variables,tableau_joint[i,:])
            else:
                groupement[cle].concat(Donnees('',tableau_joint.variables,tableau_joint[i,:]))

        nouvelles_lignes=[]
        for cle in groupement.keys():
            memo_date = groupement[cle].data[0,k]
            memo_geo = groupement[cle].data[0,j]
            groupement[cle].del_var([])

            
        
    
                






if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)
