from .base_steps import FilterStep
from typing import List, Set, Tuple, Any, Generator, Iterable
from mogwai.core import Traversal, AnonymousTraversal
from mogwai.core import Traverser
from mogwai.core.traverser import Value, Property
from mogwai.decorators import as_traversal_function
from mogwai.utils.type_utils import TypeUtils as tu
from mogwai.core.exceptions import GraphTraversalError, QueryError
import logging

logger = logging.getLogger("Mogwai")

_NA = object() #represents missing values

class Filter(FilterStep):
    def __init__(self, traversal:Traversal, filter:AnonymousTraversal):
        super().__init__(traversal, flags=Filter.NEEDS_PATH if filter.needs_path else 0)
        def _filter(t:'Traverser'):
            return next(iter(filter([t.copy()])), _NA) is not _NA
        self._filter = _filter
        self.filter_traversal = filter

    def build(self):
        self.filter_traversal._build(self.traversal)

    def print_query(self) -> str:
        return f"{self.__class__.__name__}({self.filter_traversal.print_query()})"

class Not(FilterStep):
    def __init__(self, traversal:Traversal, filter:AnonymousTraversal):
        super().__init__(traversal, flags=Filter.NEEDS_PATH if filter.needs_path else 0)
        def _filter(t:'Traverser'):
            return next(iter(filter([t.copy()])), _NA) is _NA
        self._filter = _filter
        self.filter_traversal = filter

    def build(self):
        self.filter_traversal._build(self.traversal)

    def print_query(self) -> str:
        return f"{self.__class__.__name__}({self.filter_traversal.print_query()})"

class And(FilterStep):
    def __init__(self, traversal:Traversal, optA:AnonymousTraversal,optB:AnonymousTraversal):
        super().__init__(traversal, flags=Filter.NEEDS_PATH if optA.needs_path or optB.needs_path else 0)
        def _filter(t:'Traverser'):
            return next(iter(optA([t.copy()])), _NA) is not _NA and next(iter(optB([t.copy()])), _NA) is not _NA
        self._filter = _filter
        self.anon_travs = (optA, optB)

    def build(self):
        self.anon_travs[0]._build(self.traversal)
        self.anon_travs[1]._build(self.traversal)

    def print_query(self) -> str:
        return f"{self.__class__.__name__}({', '.join((t.print_query() for t in self.anon_travs))})"

class Or(FilterStep):
    def __init__(self, traversal:Traversal, optA:AnonymousTraversal,optB:AnonymousTraversal):
        super().__init__(traversal, flags=Filter.NEEDS_PATH if optA.needs_path or optB.needs_path else 0)
        def _filter(t:'Traverser'):
            return next(iter(optA([t.copy()])), _NA) is not _NA or next(iter(optB([t.copy()])), _NA) is not _NA
        self._filter = _filter
        self.anon_travs = (optA, optB)

    def build(self):
        self.anon_travs[0]._build(self.traversal)
        self.anon_travs[1]._build(self.traversal)

    def print_query(self) -> str:
        return f"{self.__class__.__name__}({', '.join((t.print_query() for t in self.anon_travs))})"

class Has(FilterStep):
    def __init__(self, traversal:Traversal, key:str|List[str], value:Any=None, label:str|None=None):
        super().__init__(traversal)
        self.key = key
        self.label = label
        self.value = value
        indexer = (lambda t: t.get) if key=="id" else tu.get_dict_indexer(key, _NA)
        if value is None:
            self._filter = lambda t: indexer(self.traversal._get_element(t)) != _NA
        elif label is not None:
            if value is None:
                raise QueryError("Cannot filter by label without a value")
            if callable(value):
                raise QueryError("Cannot filter by label and a function as value")
            self._filter = lambda t: indexer(self.traversal._get_element(t)) == self.value and self.label in self.traversal._get_element(t)['labels']
        else:
            if callable(value):
                self._filter = lambda t: self.value(indexer(self.traversal._get_element(t)))
            else:
                self._filter = lambda t: indexer(self.traversal._get_element(t)) == self.value

class HasWithin(FilterStep):
    """
    Similar to `Has`, but with multiple options for the value
    """
    def __init__(self, traversal:Traversal, key:str|List[str], valueOptions:List|Tuple):
        super().__init__(traversal)
        self.key = key
        self.valueOptions = valueOptions
        indexer = (lambda t: t.get) if key=="id" else tu.get_dict_indexer(key, _NA)
        self._filter = lambda t: indexer(self.traversal._get_element(t)) in self.valueOptions

class HasNot(FilterStep):
    def __init__(self, traversal:Traversal, key:str|List[str]):
        super().__init__(traversal)
        self.key = key
        indexer = tu.get_dict_indexer(key, _NA)
        self._filter = lambda t: indexer(self.traversal._get_element(t)) == _NA

class HasKey(FilterStep):
    def __init__(self, traversal:Traversal, *keys:str):
        super().__init__(traversal)
        if len(keys) == 0:
            raise QueryError("HasKey needs at least one id")
        if len(keys) == 1:
            self.keys = keys[0]
            def _filter(p:Traverser|Property|Value):
                if type(p) is Property:
                    return p.key == self.keys
                raise GraphTraversalError("HasKey can only be used on Property objects")
            self._filter = _filter
        else:
            self.keys = keys
            def _filter(p:Traverser|Property|Value):
                if type(p) is Property:
                    return p.key in self.keys
                raise GraphTraversalError("HasKey can only be used on Property objects")
            self._filter = _filter

class HasValue(FilterStep):
    def __init__(self, traversal:Traversal, *values:str):
        super().__init__(traversal)
        if len(values) == 0:
            raise QueryError("HasValue needs at least one id")
        if len(values) == 1:
            self.value = values[0]
            if callable(self.value):
                raise QueryError("HasValue cannot be used with a predicate function")
            def _filter(p:Traverser|Property|Value):
                if type(p) is Property:
                    return p.value == self.value
                raise GraphTraversalError("HasValue can only be used on Property objects")
            self._filter = _filter
        else:
            self.values = values
            def _filter(p:Traverser|Property|Value):
                if type(p) is Property:
                    return p.value in self.values
                raise GraphTraversalError("HasValue can only be used on Property objects")
            self._filter = _filter

class HasId(FilterStep):
    def __init__(self, traversal:Traversal, *ids:int|tuple):
        super().__init__(traversal)
        if len(ids) == 0:
            raise QueryError("HasId needs at least one id")
        if len(ids) == 1:
            self.ids = ids[0]
            if callable(self.ids):
                self._filter = lambda t: self.ids(t.get)
            else:
                self._filter = lambda t: t.get==self.ids
        else:
            self.ids = ids
            self._filter = lambda t: t.get in self.ids

class Contains(FilterStep):
    #same as has, but they key can correspond to a collection
    # returns `true` if `value` is in that collection.
    def __init__(self, traversal:Traversal, key:str|List[str], value:Any):
        super().__init__(traversal)
        self.key = key
        self.value = value
        indexer = tu.get_dict_indexer(self.key, [])
        self._filter = lambda t: self.value in indexer(self.traversal._get_element(t))

class ContainsAll(FilterStep):
    #same as Has, but `values` can be a collection; all items in `values` need to be present
    def __init__(self, traversal:Traversal, key:str|List[str], values:Set[Any]|List[Any]):
        super().__init__(traversal)
        self.key = key
        if(type(values) is set):
            def _hasall(x):
                if(type(x) is set):
                    return values.issubset(x) #this is a lot faster
                else: return all([v in x for v in values])
        else:
            _hasall = lambda x: all([v in x for v in values])

        indexer = tu.get_dict_indexer(self.key)
        self._filter = lambda t: _hasall(indexer(self.traversal._get_element(t, [])))

class Within(FilterStep):
    #same as has, but the passes the traverser if the value matches one of the given options
    def __init__(self, traversal:Traversal, key:str|List[str], options:List[Any]):
        super().__init__(traversal)
        self.key = key
        self.options = options
        indexer = tu.get_dict_indexer(key)
        self._filter = lambda t: indexer(self.traversal._get_element(t)) in options

class Is(FilterStep):
    def __init__(self, traversal: Traversal, condition: Any):
        super().__init__(traversal)
        self.condition = condition

    def __call__(self, traversers: Iterable['Traverser']) -> 'Generator[Traverser]':
        traversers = tu.ensure_is_list(traversers)
        dtype = tu.get_list_type(traversers)
        if dtype is None:
            return traversers
        if issubclass(dtype, Value):
            if callable(self.condition):
                self._filter = lambda v: self.condition(v.value)
            else:
                self._filter = lambda v: v.value == self.condition
        else:
            raise GraphTraversalError(f"Cannot apply Is filter to traversers")
        return super().__call__(traversers)

class Limit(FilterStep):
    def __init__(self, traversal: Traversal, n:int):
        super().__init__(traversal)
        self.n = n
        self.counter = 0
        def _filter(t:Traverser):
            self.counter += 1
            return self.counter <= self.n
        self._filter = _filter

    def __call__(self, traversers: Iterable[Traverser]) -> Iterable[Traverser]:
        self.counter = 0
        return super().__call__(traversers)

class Range(FilterStep):
    def __init__(self, traversal: Traversal, low:int, high:int):
        super().__init__(traversal)
        self.low, self.high = low, high
        self.counter = 0
        self.no_limits = False
        self.stop = False
        if high == -1 and low==0: # no limits
            self.no_limits = True
        if high == -1: #no upper limit
            self._slice = slice(low, None)
            def _filter(t):
                self.counter += 1
                return self.counter > self.low
        elif low == 0: #no lower limit
            self._slice = slice(None, high)
            def _filter(t):
                self.counter += 1
                if self.counter > self.high:
                    self.stop = True
                    return False
                return True
        else:
            self._slice = slice(low, high)
            def _filter(t):
                self.counter += 1
                if self.counter > self.high:
                    self.stop = True
                    return False
                return self.counter > self.low
        self._filter = _filter

    def __call__(self, traversers: Iterable[Traverser]) -> Iterable[Traverser]:
        if self.no_limits: return traversers
        if isinstance(traversers, (list, tuple)):
            return traversers[self._slice]
        self.counter = 0
        self.stop = False
        def gen():
            for t in traversers:
                if self._filter(t):
                    yield t
                if self.stop:
                    return
        return gen()

class SimplePath(FilterStep):
    def __init__(self, traversal:Traversal, by:str|List[str]):
        super().__init__(traversal, flags=SimplePath.SUPPORTS_BY|SimplePath.NEEDS_PATH)
        self.by = by
        self.indexer = None
        def _filter(trav:Traverser):
            path = trav.path
            if self.by is not None:
                path = (self.indexer(item) for item in path)
            seen = set()
            for item in path:
                if item in seen:
                    return False
                seen.add(item)
            return True
        self._filter = _filter

    def build(self):
        self.indexer = tu.get_dict_indexer(self.by) if self.by else None

class Dedup(FilterStep):
    def __init__(self, traversal: Traversal, by:str|List[str]=None):
        super().__init__(traversal, flags=Dedup.SUPPORTS_BY)
        self.by = by

    def build(self):
        self.seen = []
        def _dedup(obj):
            if obj in self.seen: return False
            else:
                self.seen.append(obj)
                return True

        if self.by is None:
            self._filter = lambda t: _dedup((t.get,t.path) if isinstance(t, Traverser) else t.val)
        else:
            indexer = tu.get_dict_indexer(self.by, None)
            self._filter = lambda t: (x:=indexer(self.traversal._get_element(t))) is not None and _dedup(x)

    def __call__(self, traversers: Iterable[Traverser]) -> Iterable[Traverser]:
        self.seen = []
        return super().__call__(traversers)

@as_traversal_function
def or_(optA:AnonymousTraversal, optB:AnonymousTraversal):
    return Or(None, optA, optB)

@as_traversal_function
def and_(optA:AnonymousTraversal, optB:AnonymousTraversal):
    return And(None, optA, optB)

@as_traversal_function
def not_(opt:AnonymousTraversal):
    return Not(None, opt)

@as_traversal_function
def has(*args):
    if len(args)==1:
        key, value = args[0], None
        return Has(None, key, value)
    elif len(args)==2:
        key, value = args
        return Has(None, key, value)
    elif len(args)==3:
        label, key, value = args
        return Has(None, key, value, label=label)
    else:
        raise QueryError("Invalid number of arguments for `has`")

@as_traversal_function
def has_not(key:str|List[str]):
    return HasNot(None, key)

@as_traversal_function
def has_id(*ids:int):
    return HasId(None, *ids)

@as_traversal_function
def has_name(name: str):
    return Has(None, "name", value=name)

@as_traversal_function
def has_label(label: str):
    return Contains(None, "labels", value=label)

@as_traversal_function
def contains(key:str|List[str], value:Any):
    if isinstance(value, list):
        return ContainsAll(None, key, value)
    else:
        return Contains(None, key, value)

@as_traversal_function
def filter_(filter:AnonymousTraversal):
    return Filter(None, filter)

@as_traversal_function
def simple_path(by:str=None):
    return SimplePath(None, by=by)

@as_traversal_function
def limit(n:int):
    return Limit(None, n)

@as_traversal_function
def dedup(by:str|List[str]=None):
    return Dedup(None, by)