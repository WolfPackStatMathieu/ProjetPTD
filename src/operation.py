from pipeline import Pipeline
from abc import ABC, abstractmethod

class Operation(ABC):
    """classe abstraite Opération

    Parameters
    ----------
    ABC : _type_
        _description_
    """
    @abstractmethod
    def ope(self, pipeline):
        """classe ope vide

        Parameters
        ----------
        pipeline : pipeline
            le pipeline sur lequel s'applique l'opération
        """
