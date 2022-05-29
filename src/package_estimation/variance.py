from src.package_estimation.estimation import Estimation
import numpy as np

class Variance(Estimation):
    """classe représentant le calcul de la moyenne

    Args:
        Estimation (classe): classe parente
    """
    def __init__(self, variables):
        """constructeur de la classe

        Args:
            variables (liste): liste de variables dont on veut calculer la moyenne
        """
        self.variables = variables


    def ope(self, pipeline):
        """renvoie la liste des moyennes pour les variables quantitatives

        Args:
            pipeline (pipeline): un pipeline

        Returns
        -------
        list[float]
            liste des moyennes ordonnée selon l'ordre des variables du jeu de données
        Examples
        --------
        >>> import numpy as np
        >>> from src.pipeline import Pipeline
        >>> from src.donnees import Donnees
        >>> from src.package_estimation.estimation import Estimation
        >>> test = Donnees('nom',['nom', 'valeur'],[['a', 1], ['b', 4], ['c', 8]])
        >>> test.get_var('valeur')
        1
        >>> test_2 = test.var_num()
        >>> from src.package_estimation.moyenne import Moyenne
        >>> mon_operation1 = Moyenne(['valeur'])
        >>> ma_liste_operations = [mon_operation1]
        >>> mypipeline = Pipeline(ma_liste_operations, test)
        >>> isinstance(mypipeline , Pipeline)
        >>> mon_operation1.ope(mypipeline)


        """
        liste_moyennes = []
        for v in self.variables:
            liste = []
            j = pipeline.resultat.get_var(v)
            for i in range(pipeline.resultat.data.shape[0]):
                if not(np.isnan(pipeline.resultat.data[i,j])):
                    liste.append(pipeline.resultat.data[i,j])
            somme = sum(liste)
            moyenne = round(somme/len(liste), 2)
        liste_moyennes.append(moyenne)
        return liste_moyennes

if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=False)
