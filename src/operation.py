'''module contenant la classe abstraite des opérations'''

from abc import ABC, abstractmethod
from src.pipeline import Pipeline

class Operation(ABC):
    """classe abstraite Opération
    """
    @abstractmethod
    def ope(self, pipeline):
        """méthode ope vide

        Parameters
        ----------
        pipeline : pipeline
            le pipeline sur lequel s'applique l'opération
        """
