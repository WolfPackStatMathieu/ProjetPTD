''' Classe SauvegardeCsv '''

from src.package_sauvegarde.sauvegarde import Sauvegarde
import numpy as np


class SauvegardeCsv(Sauvegarde):
    """_summary_

    Parameters
    ----------
    Sauvegarde : _type_
        _description_
    """

    def ope(self, pipeline):
        """ce que fait la sauvegarde

        Parameters
        ----------
        pipeline : Pipeline
            la Pipeline dont il faut sauvegarder les données

        Examples
        --------
        >>> import numpy as np
        >>> from src.donnees import Donnees
        >>> test = Donnees('nom',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])

        """

        a = numpy.asarray([ [1,2,3], [4,5,6], [7,8,9] ])
        numpy.savetxt("foo.csv", a, delimiter=",")


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=True)

