from abc import ABC, abstractmethod

class Operation(ABC):
    @abstractmethod
    def ope(self, jeu_de_donnees):
        pass