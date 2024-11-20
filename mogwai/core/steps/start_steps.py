from .base_steps import Step, SideEffectStep
from mogwai.core import MogwaiGraph
from mogwai.core.traverser import Traverser
from mogwai.core.traversal import Traversal
from typing import List, Tuple, Set
from ..exceptions import GraphTraversalError

class V(Step):
    def __init__(self, graph:MogwaiGraph, init:int|List[int]=None):
        super().__init__(None, flags=Step.ISSTART)
        self.graph = graph
        self.init = init if init is None or isinstance(init, (list, tuple)) else [init]

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

class AddV(SideEffectStep):
    def __init__(self, graph:MogwaiGraph, labels:str|Set[str], name:str="", **kwargs):
        super(SideEffectStep, self).__init__(None, flags=Step.ISSTART)
        self.graph = graph
        self.name = name
        self.properties = kwargs
        self.labels = {labels} if isinstance(labels, str) else (set(labels) if isinstance(labels, (list,tuple)) else labels)
    
    def set_traversal(self, traversal:Traversal):
        self.traversal = traversal
    
    def __call__(self, traversers:List[Traverser]) -> List[Traverser]:
        if len(traversers)>0:
            raise GraphTraversalError("Cannot perform AddV step on a non-empty Traversal.")
        v = self.graph.add_labeled_node(self.labels, name=self.name, **self.properties)
        traversers.append(Traverser(v, track_path=self.traversal.needs_path))
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

class AddE(SideEffectStep):
    def __init__(self, graph:MogwaiGraph, relation:str, from_:int=None, to_:int=None, **kwargs):
        super(SideEffectStep, self).__init__(None, flags=Step.ISSTART|Step.SUPPORTS_FROMTO)
        self.graph = graph
        self.relation = relation
        self.from_, self.to_ = from_, to_
        self.properties = kwargs
            
    def set_traversal(self, traversal:Traversal):
        self.traversal = traversal
    
    def build(self):
        if self.from_ is None or self.to_ is None:
            raise GraphTraversalError("AddE step requires both from_ and to_ parameters. Did you forget a from_() or to() step?")
    
    def __call__(self, traversers:List[Traverser]) -> List[Traverser]:
        if len(traversers)>0:
            raise GraphTraversalError("Cannot perform AddE step on a non-empty Traversal.")
        self.graph.add_labeled_edge(self.from_, self.to_, self.relation, **self.properties)
        traversers.append(Traverser(self.from_, self.to_, track_path=self.traversal.needs_path))
        return traversers