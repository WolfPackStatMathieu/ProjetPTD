import os
from pathlib import Path
import numpy as np
from src.donnees import Donnees
from src.package_transformation.concatenation import Concatenation
from src.pipeline import Pipeline
from src.package_transformation.jointure import Jointure
from src.pipeline import Pipeline
from src.package_transformation.transformation import Transformation
from src.package_estimation.moyenne import Moyenne
from src.package_chargement.chargement_csv import ChargementCsv

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
    # >>> import numpy as np
    # >>> from src.pipeline import Pipeline
    # >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'numer_sta','date'],[['a',1, ], ['b', 3], ['c',5]])
    # >>> mon_pipeline = Pipeline([Aggregation(['numer_sta'])], mes_donnees)
    # >>> mon_pipeline.execute()
    # [['a' -4.0]
    #  ['b' 0.0]
    #  ['c' 4.0]]


    '''
    def __init__(self, space_var='region',) :
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
        path = 'C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD'
        cheminDossier = str(path) + "\\fichiers stations et régions"
        nom_fichier=['postesSynopAvecRegions.csv.gz']
        delimiteur = ';'
        liste_donnees = ChargementCsv(cheminDossier, nom_fichier, delimiteur, True).charge()
        correspondance = liste_donnees[0] #récupération des Données de correspondance
        correspondance.variables[correspondance.get_var("ID")]="numer_sta"

        joindre=Pipeline([Jointure(pipeline.resultat,["numer_sta"])], correspondance)
        joindre.execute()
        tableau_joint = joindre.resultat
        tableau_joint.variables[tableau_joint.get_var('Region')]='region'
        tableau_joint.var_num(['date','region'])


        groupement={}
        liste=[]
        indice=0
        j = tableau_joint.get_var('region')
        k = tableau_joint.get_var('date')
        for i in range(tableau_joint.data.shape[0]):
            cle =(tableau_joint.data[i,j],tableau_joint.data[i,k])
            if ( cle not in groupement):
                liste.append(Donnees('',tableau_joint.variables,[tableau_joint.data[i,:]]))
                groupement[cle]= indice
                indice+=1
            else:
                liste[groupement[cle]].concat(Donnees('',tableau_joint.variables,tableau_joint[i,:]))

        nouvelles_lignes=[]
        for l in liste :
            memo_date = l.data[:,k]
            memo_geo = l.data[:,j]
            l.del_var(['date', 'region'])
            aggregat = Pipeline([Moyenne(l.variables)],l).get_res()
            aggregat.add_var(['date'],[memo_date])
            aggregat.add_var(['region'],[memo_geo])
            nouvelles_lignes.append(aggregat)

        resultat=Pipeline([Concatenation(nouvelles_lignes[1,:])],nouvelles_lignes[0])
        resultat.execute()

        pipeline.resultat = resultat.resultat


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)
