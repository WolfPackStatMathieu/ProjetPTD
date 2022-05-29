'''module pour joindre un jeu de donnes avec un autre'''
from src.donnees import Donnees
from src.pipeline import Pipeline
import numpy as np
from transformation import Transformation
from transformation import Transformation
class Jointure(Transformation):
    '''classe d'opération permettant de joindre à gauche des jeux de donnees

    Parameters
    ----------
    autre_donnees : Donnees
        donnees a joindre
    keys : list[string]
        clé de jointure

    Attributes
    ----------

    autre_donnees : Donnees
        donnees a joindre
    keys : list[string]
        clé de jointure

    Examples
    --------
    >>> import numpy as np
    >>> mes_donnees_1 = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
    >>> mes_donnees_2 = Donnees('mon_nom_jeu_de_donnees',['nom', 'titre'],[['a','numero1'], ['b', 'numero2' ], ['c','numero3']])
    >>> mon_pipeline = Pipeline([Jointure(mes_donnees_2,['nom'])], mes_donnees_1)
    >>> mon_pipeline.execute()
    >>> print(mon_pipeline)
    [['a' 1 'numero1']
     ['b' 5 'numero2']
     ['c' 9 'numero3']]

    '''
    def __init__(self,autre_donnees : Donnees, keys ):
        self.autre_donnes = autre_donnees
        self.keys = keys

    def ope(self,pipeline : Pipeline):
        '''méthode qui permet d'exécuter l'opération

        Parameters
        ----------
        pipeline : Pipeline
            pipeline sur lequel s'éxecute l'opération
        '''
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

if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)
