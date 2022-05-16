from abc import ABC, abstractmethod

class Operation(ABC):
    @abstractmethod
    def ope(self, pipeline):
        pass