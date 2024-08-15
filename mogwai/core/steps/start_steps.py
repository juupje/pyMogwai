from .base_steps import Step
from mogwai.core import MogwaiGraph
from mogwai.core.traverser import Traverser
from mogwai.core.traversal import Traversal
from typing import List, Tuple
from ..exceptions import GraphTraversalError

class V(Step):
    def __init__(self, graph:MogwaiGraph, init:int|List[int]=None):
        super().__init__(None, flags=Step.ISSTART)
        self.graph = graph
        self.init = init if init is None or isinstance(init, list) else [init]

    def set_traversal(self, traversal:Traversal):
        self.traversal = traversal

    def __call__(self, traversers:List[Traverser]) -> List[Traverser]:
        if len(traversers)>0:
            raise GraphTraversalError("Cannot perform V step on a non-empty Traversal.")
        else:
            if self.init is None:
                #spawn a traverser for every vertex in graph
                for v in self.graph.nodes():
                    traversers.append(Traverser(v, track_path=self.traversal.needs_path))
            else:
                for v in self.init:
                    if v in self.graph.nodes:
                        traversers.append(Traverser(v, track_path=self.traversal.needs_path))
                    else:
                        raise GraphTraversalError(f"No node with id {v}.")
        return traversers
    
class E(Step):
    def __init__(self, graph:MogwaiGraph, init:Tuple[int]|List[Tuple[int]]):
        super().__init__(None, flags=Step.ISSTART)
        self.graph = graph
        self.init = init if init is None or isinstance(init, list) else [init]
    
    def set_traversal(self, traversal:Traversal):
        self.traversal = traversal

    def __call__(self, traversers:List[Traverser]) -> List[Traverser]:
        if len(traversers)>0:
            raise GraphTraversalError("Cannot perform E step on a non-empty Traversal.")
        else:
            if self.init is None:
                #spawn a traverser for every edge in graph
                for e in self.graph.edges():
                    traversers.append(Traverser(*e, track_path=self.traversal.needs_path))
            else:
                for e in self.init:
                    if e in self.graph.edges:
                        traversers.append(Traverser(*e, track_path=self.traversal.needs_path))
                    else:
                        raise GraphTraversalError(f"No edge from {e[0]} to {e[1]}.")
        return traversers