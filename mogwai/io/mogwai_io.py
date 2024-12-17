from abc import ABC, abstractmethod
from mogwai.core import MogwaiGraph

class IOBackend(ABC):
    @abstractmethod
    def read(self, file_path: str):
        pass

    @abstractmethod
    def write(self, file_path: str, graph:MogwaiGraph):
        pass

class GraphSON(IOBackend):
    pass

class GraphML(IOBackend):
    pass

class RDF(IOBackend):
    pass