'''classe abstraite transformation'''
from src.operation import Operation
class Transformation(Operation):
    '''classe abstraite des transformations'''
    pass

if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=False)