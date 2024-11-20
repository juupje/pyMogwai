from ..traverser import Traverser
from typing import Any, Callable, Iterable
from abc import abstractmethod
from ..exceptions import GraphTraversalError, QueryError
import typing
if typing.TYPE_CHECKING:
    from mogwai.core.traversal import Traversal, AnonymousTraversal
from mogwai.utils.type_utils import TypeUtils as tu

class Step:
    ISTERMINAL  = 1<<0
    ISSTART     = 1<<1
    NEEDS_PATH  = 1<<2
    SUPPORTS_BY = 1<<3
    SUPPORTS_ANON_BY = 1<<4 | SUPPORTS_BY
    SUPPORTS_MULTIPLE_BY = 1<<5 | SUPPORTS_BY
    SUPPORTS_FROMTO = 1<<6

    def __init__(self, traversal:'Traversal', flags:int=0):
        self.traversal = traversal
        self.flags = flags
        self._by = None

    @property
    def isstart(self):
        return (self.flags & Step.ISSTART)!=0
    @property
    def isterminal(self):
        return (self.flags & Step.ISTERMINAL)!=0
    @property
    def needs_path(self):
        return (self.flags & Step.NEEDS_PATH)!=0
    @property
    def supports_by(self):
        return (self.flags & Step.SUPPORTS_BY)!=0
    @property
    def supports_anonymous_by(self):
        return (self.flags & Step.SUPPORTS_ANON_BY)==Step.SUPPORTS_ANON_BY
    @property
    def supports_multiple_by(self):
        return (self.flags & Step.SUPPORTS_MULTIPLE_BY)==Step.SUPPORTS_MULTIPLE_BY
    @property
    def supports_fromto(self):
        return (self.flags & Step.SUPPORTS_FROMTO)==Step.SUPPORTS_FROMTO

    @abstractmethod
    def __call__(self, traversers: Iterable['Traverser']) -> Iterable['Traverser']:
        pass

    @abstractmethod
    def build(self):
        pass

    def print_query(self) -> str: return self.__class__.__name__

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}[isstart={self.isstart}, isterminal={self.isterminal}]>"

class MapStep(Step):
    def __init__(self, traversal:'Traversal', **kwargs):
        super().__init__(traversal, **kwargs)
        self._map = None #self._map should contain a function that takes an object and returns exactly one object

    def __call__(self, traversers: Iterable['Traverser']|Iterable[Any]) -> Iterable['Traverser']|Iterable[Any]:
        if(self._map is None): raise GraphTraversalError("No map defined! This is most likely a bug.")
        return (x for t in traversers if (x:=self._map(t)) is not None)

class FlatMapStep(Step):
    def __init__(self, traversal:'Traversal', **kwargs):
        super().__init__(traversal, **kwargs)
        self._flatmap = None # self._flatmap should be a function that maps a traverser onto an iterable.

    def __call__(self, traversers: Iterable['Traverser']) -> Iterable['Traverser']:
        if self._flatmap is None: raise GraphTraversalError("No flatmap defined! This is most likely a bug.")
        return (x for t in traversers for x in self._flatmap(t))

class SideEffectStep(Step):
    def __init__(self, traversal: 'Traversal', side_effect: 'AnonymousTraversal|Callable[[Traverser], None]', **kwargs):
        super().__init__(traversal, **kwargs)
        self.side_effect = side_effect

    def __call__(self, traversers: Iterable['Traverser']) -> Iterable['Traverser']:
        from mogwai.core import AnonymousTraversal
        traversers = tu.ensure_is_list(traversers)
        if isinstance(self.side_effect, AnonymousTraversal):
            for traverser in traversers:
                self.side_effect(self.traversal, [traverser.copy()])
        else:
            for traverser in traversers:
                self.side_effect(traverser)
        return traversers

class FilterStep(Step):
    def __init__(self, traversal:'Traversal', **kwargs):
        super().__init__(traversal, **kwargs)
        self._filter = None #self._filter should contain a function that takes a dict-like object and returns a bool

    def __call__(self, traversers: Iterable['Traverser']) -> Iterable['Traverser']:
        if(self._filter is None): raise GraphTraversalError("No filter defined! This is most likely a bug.")
        return (t for t in traversers if self._filter(t))

class BranchStep(Step):
    def __init__(self, traversal:'Traversal', **kwargs):
        super().__init__(traversal, **kwargs)

    @abstractmethod
    def __call__(self, traversers: Iterable['Traverser']) -> Iterable['Traverser']:
        pass