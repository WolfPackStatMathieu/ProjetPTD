from donnees import Donnees
from src.package_transformation.concatenation import Concatenation
from src.pipeline import Pipeline
from src.package_transformation.jointure import Jointure
from src.pipeline import Pipeline
from transformation import Transformation
from package_estimation.moyenne import Moyenne
import numpy as np

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

        ##### Chargement des Données avec l'identifiant
        # de la station et la région ###

        ##### on crée les données
        path = Path(os.getcwd()).parent.parent.absolute()
        cheminDossier = str(path) + "\\fichiers stations et régions"
        nom_fichier=['postesSynopAvecRegions.csv.gz']
        delimiteur = ';'
        pipeline1 = Pipeline([ChargementCsv(cheminDossier, nom_fichier, delimiteur, True)]) #pipeline créant les données de correspondance
        correspondance = pipeline1.execute().get_res() #récupération des Données de correspondance

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
            groupement[cle].del_var(['date', 'code_insee_region'])
            aggregat = Pipeline([Moyenne(groupement[cle].variables)],groupement[cle]).get_res
            aggregat.add_var(['date','code_insee_region'],np.array([memo_date,memo_geo]))
            nouvelles_lignes.append(aggregat)

        resultat=Pipeline([Concatenation(nouvelles_lignes[1,:])],nouvelles_lignes[0])

        pipeline.resultat = resultat


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)
