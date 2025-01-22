from .base_steps import Step
from mogwai.core.traversal import Traversal, Traverser
from typing import Iterable, Type
from mogwai.core.exceptions import QueryError, GraphTraversalError
from mogwai.core import MogwaiGraphConfig
from .enums import IO as IOEnum
from mogwai.io import GraphSON, GraphSONWrapped, GraphML, RDF, JSON, IOBackend
import logging
logger = logging.getLogger("Mogwai")

_NA = object()

class IO(Step):
    def __init__(self, traversal:'Traversal', file_path:str, read:bool=None, write:bool=None, config:MogwaiGraphConfig=None, **kwargs):
        super().__init__(traversal, flags=IO.SUPPORTS_WITH, **kwargs)
        self.file_path = file_path
        self.read = read
        self.write = write
        self.backend = None
        self.task = None
        self.config = config

    @property
    def with_(self):
        return (self.task, self.backend, self.config)

    @with_.setter
    def with_(self, value):
        if not isinstance(value, (tuple, list)):
            value = (value,)
        for val in value:
            if val in [IOEnum.reader, IOEnum.writer]:
                self.task = value[0]
            elif val in [IOEnum.graphson, IOEnum.graphson_wrapped, IOEnum.json, IOEnum.graphml, IOEnum.rdf]:
                self.backend = value[1]
            elif type(val) == MogwaiGraphConfig:
                self.config = val
            else:
                raise QueryError("with() expected arguments (IO.reader, backend), (IO.writer, backend) or (GraphConfig)")

    def build(self):
        if self.task is None:
            if self.read is None and self.write is None:
                raise QueryError("IO step must be followed by either a `read` or `write` step")
            if not (self.read ^ self.write):
                raise QueryError("IO step must be followed by either a `read` or `write` step")
            if self.read:
                self.task = IOEnum.reader
            else:
                self.task = IOEnum.writer

        #determines the type of reader/writer to use
        if self.backend is None:
            import os #should never fail
            #auto determine the backend
            ext = os.path.splitext(self.file_path)[1].lower()
            match(ext):
                case '.json':
                    self.backend = IOEnum.graphson
                case '.xml' | '.graphml':
                    self.backend = IOEnum.graphml
                case '.rdf':
                    self.backend = IOEnum.rdf
                case _:
                    raise QueryError(f"Cannot automatically infer which parser to use for file format: {ext}. Please use the with() step to specify the parser.")
            logger.debug(f"Auto-detected IO-backend: {self.backend.value}")
        else:
            if self.backend not in [IOEnum.graphson, IOEnum.graphson_wrapped, IOEnum.json, IOEnum.graphml, IOEnum.rdf]:
                raise QueryError(f"Invalid backend specified: {self.backend}")
        return
    
    def get_backend_class(self) -> Type[IOBackend]:
        match self.backend:
            case IOEnum.graphson_wrapped:
                return GraphSONWrapped
            case IOEnum.graphson:
                return GraphSON
            case IOEnum.json:
                return JSON
            case IOEnum.graphml:
                return GraphML
            case IOEnum.rdf:
                return RDF
            case _:
                raise QueryError(f"Invalid backend specified: {self.backend}")

    def __call__(self, traversers: Iterable['Traverser']) -> Iterable['Traverser']:
        #traversers should be empty
        if next(iter(traversers), _NA) != _NA:
            raise GraphTraversalError("Cannot perform IO step on a non-empty Traversal.")
        backend = self.get_backend_class()()
        if self.task == IOEnum.reader:
            #read from file
            graph = backend.read(self.file_path, self.config)
            #merge the graph with the current graph
            self.traversal.graph.merge(graph)
        elif self.task == IOEnum.writer:
            #write to file
            backend.write(self.traversal.graph, self.file_path)
        else:
            raise QueryError("Invalid/No IO task specified.")
        #return an empty traverser set
        return []

    def print_query(self) -> str:
        task = self.task or (IOEnum.reader if self.read else (IOEnum.writer if self.write else None))
        return f"{self.__class__.__name__}(task={task}, backend={self.backend}, file_path={self.file_path})"