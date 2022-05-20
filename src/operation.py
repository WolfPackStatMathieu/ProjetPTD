<<<<<<< HEAD
from pipeline import Pipeline
=======
'classe opération'
>>>>>>> 0fd4ec5c8756fd8f4fd5fb98752573c132a7d000
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
