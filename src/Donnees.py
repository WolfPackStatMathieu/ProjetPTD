'''module Donnees pour représenter un jeu de données
'''
import numpy as np

class Donnees :
    '''représente un jeu de données

    Parameters
    ----------
    variables : list[str]
        Liste des noms de variables
    data : np.array
        donnees du jeu de donnees

    Attributes
    ----------
    variables : list[str]
        Liste des noms de variables
    data : np.array
        donnees du jeu de donnees

    Examples
    --------
    >>> import numpy as np
    >>> donnees = Donnees(['nom', 'valeur'],
    ...                    np.array([['a',1], ['b', 5, ], ['c',9]]))


    '''
    def __init__(self, variables, data):
        self.variables = variables
        self.data = data


if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=True)