from .base_steps import FlatMapStep
from typing import Iterable
from mogwai.core import Traversal
from mogwai.core import Traverser
from mogwai.decorators import as_traversal_function
from mogwai.core.exceptions import GraphTraversalError
from mogwai.utils.type_utils import TypeUtils as tu

class Out(FlatMapStep):
    def __init__(self, traversal:Traversal, direction:str=None):
        super().__init__(traversal)
        self.direction = direction
        if self.direction is None:
            self._flatmap = lambda t: (t.copy_to(neighbor) for neighbor in self.traversal.graph.successors(t.node_id))
        else:
            self._flatmap = lambda t: (t.copy_to(outedge[1]) for outedge in self.traversal.graph.out_edges(t.node_id,data='labels') if outedge[-1]==self.direction)

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers) #we need a list to loop over them twice
        if any((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to edges.")
        return super().__call__(traversers=traversers)

class OutE(FlatMapStep):
    def __init__(self, traversal:Traversal, direction:str=None):
        super().__init__(traversal)
        self.direction = direction
        if self.direction is None:
            self._flatmap = lambda t: (t.copy_to(*edge) for edge in self.traversal.graph.out_edges(nbunch=t.node_id))
        else:
            self._flatmap = lambda t: (t.copy_to(*edge[:-1]) for edge in self.traversal.graph.out_edges(nbunch=t.node_id,data='labels') if edge[-1]==self.direction)

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers)
        if any((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to edges.")
        return super().__call__(traversers=traversers)

class In(FlatMapStep):
    def __init__(self, traversal:Traversal, direction:str=None):
        super().__init__(traversal)
        self.direction = direction
        if self.direction is None:
            self._flatmap = lambda t: (t.copy_to(neighbor) for neighbor in self.traversal.graph.predecessors(t.node_id))
        else:
            self._flatmap = lambda t: (t.copy_to(inedge[0]) for inedge in self.traversal.graph.in_edges(t.node_id,data='labels') if inedge[-1]==self.direction)

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers)
        if any((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to edges.")
        return super().__call__(traversers=traversers)

class InE(FlatMapStep):
    def __init__(self, traversal:Traversal, direction:str=None):
        super().__init__(traversal)
        self.direction = direction
        if self.direction is None:
            self._flatmap = lambda t: (t.copy_to(*edge) for edge in self.traversal.graph.in_edges(nbunch=t.node_id))
        else:
            self._flatmap = lambda t: (t.copy_to(*edge[:-1]) for edge in self.traversal.graph.in_edges(nbunch=t.node_id,data='labels') if edge[-1]==self.direction)

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers)
        if any((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to edges.")
        return super().__call__(traversers=traversers)

class InV(FlatMapStep):
    def __init__(self, traversal:Traversal):
        super().__init__(traversal)
        self._flatmap = lambda t: (t.move_to(t.target),) #needs to be an iterable

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers)
        if not all((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to vertices.")
        return super().__call__(traversers=traversers)

class OutV(FlatMapStep):
    def __init__(self, traversal:Traversal):
        super().__init__(traversal)
        self._flatmap = lambda t: (t.move_to(t.node_id),) #needs to be an iterable

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers)
        if not all((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to vertices.")
        return super().__call__(traversers=traversers)

class Both(FlatMapStep):
    def __init__(self, traversal:Traversal, direction:str=None):
        super().__init__(traversal)
        self.direction = direction
        if self.direction is None:
            def _flatmap(t:'Traverser'):
                for edge in self.traversal.graph.out_edges(nbunch=t.node_id):
                    yield t.copy_to(edge[1])
                for edge in self.traversal.graph.in_edges(nbunch=t.node_id):
                    yield t.copy_to(edge[0])
        else:
            def _flatmap(t:'Traverser'):
                for edge in self.traversal.graph.out_edges(nbunch=t.node_id,data="labels"):
                    if edge[-1] == direction: yield t.copy_to(edge[1])
                for edge in self.traversal.graph.in_edges(nbunch=t.node_id,data="labels"):
                    if edge[-1] == direction: yield t.copy_to(edge[0])
        self._flatmap = _flatmap

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers)
        if any((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to edges.")
        return super().__call__(traversers=traversers)

class BothE(FlatMapStep):
    def __init__(self, traversal:Traversal, direction:str=None):
        super().__init__(traversal)
        self.direction = direction
        if self.direction is None:
            def _flatmap(t:'Traverser'):
                for edge in self.traversal.graph.out_edges(nbunch=t.node_id): yield t.copy_to(*edge)
                for edge in self.traversal.graph.in_edges(nbunch=t.node_id): yield t.copy_to(*edge)
        else:
            def _flatmap(t:'Traverser'):
                for edge in self.traversal.graph.out_edges(nbunch=t.node_id,data="labels"):
                    if edge[-1] == direction: yield t.copy_to(*edge[:-1])
                for edge in self.traversal.graph.in_edges(nbunch=t.node_id,data="labels"):
                    if edge[-1] == direction: yield t.copy_to(*edge[:-1])
        self._flatmap = _flatmap

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers)
        if any((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to edges.")
        return super().__call__(traversers=traversers)

class BothV(FlatMapStep):
    def __init__(self, traversal:Traversal):
        super().__init__(traversal)
        self._flatmap = lambda t: (t.copy_to(t.node_id),t.move_to(t.target))

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        traversers = tu.ensure_is_list(traversers)
        if not all((t.is_edge for t in traversers)): #make sure to use a generator here!
            raise GraphTraversalError(f"{self.__class__.__name__} cannot be applied to vertices.")
        return super().__call__(traversers=traversers)

@as_traversal_function
def out(direction: str=None):
    return Out(None, direction)

@as_traversal_function
def outE(direction:str=None):
    return OutE(None, direction)

@as_traversal_function
def outV():
    return OutV(None)

@as_traversal_function
def in_(direction: str=None):
    return In(None, direction)

@as_traversal_function
def inE(direction:str=None):
    return InE(None, direction)

@as_traversal_function
def inV():
    return InV(None)

@as_traversal_function
def both(direction:str=None):
    return Both(None, direction)

@as_traversal_function
def bothE(direction:str=None):
    return Both(None, direction)

@as_traversal_function
def bothV():
    return BothV(None)