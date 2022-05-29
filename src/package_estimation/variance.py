from estimation import Estimation
import numpy as np
from pipeline import Pipeline

class Variance(Estimation):
    """classe représentant le calcul de la variance

    Args:
        Estimation (classe): classe parente
    """
    def __init__(self, variables):
        """constructeur de la classe

        Args:
            variables (liste): liste de variables dont on veut calculer la variance
        """
        self.variables = variables


    def ope(self, pipeline : Pipeline):
        """renvoie la liste des variances pour les variables quantitatives

        Args:
            pipeline (pipeline): un pipeline

        Returns
        -------
        list[float]
            liste des variances ordonnée selon l'ordre des variables du jeu de données
        Examples
        --------
        >>> import numpy as np
        >>> from src.pipeline import Pipeline
        >>> from src.donnees import Donnees
        >>> from src.package_estimation.estimation import Estimation
        >>> test = Donnees('nom',['nom', 'valeur'],[['a', 1], ['b', 2], ['c', 6]])
        >>> test.get_var('valeur')
        1
        >>> test_2 = test.var_num()
        >>> from src.package_estimation.moyenne import Moyenne
        >>> mon_operation1 = Variance(['valeur'])
        >>> ma_liste_operations = [mon_operation1]
        >>> mypipeline = Pipeline(ma_liste_operations, test)
        >>> isinstance(mypipeline , Pipeline)
        True
        >>> mon_operation1.ope(mypipeline)
        [9.56]


        """
        liste_variances = []
        for v in self.variables:
            if not(pipeline.resultat.var_type(v)== float or  pipeline.resultat.var_type(v)== int):
                raise Exception("Pas de varaince pour une variable qualitative")
            liste = []
            j = pipeline.resultat.get_var(v)
            for i in range(pipeline.resultat.data.shape[0]):
                if not(np.isnan(pipeline.resultat.data[i,j])):
                    liste.append(pipeline.resultat.data[i,j])
            somme = sum(liste)
            moyenne = somme/len(liste)
            liste2 = [(k - moyenne)**2 for k in liste]
            variance = round(sum(liste2)/len(liste2),2)
            liste_variances.append(variance)
        return liste_variances

if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=False)
