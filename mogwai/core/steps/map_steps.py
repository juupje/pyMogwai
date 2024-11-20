from .base_steps import MapStep
from typing import List, Any, Iterable, Generator, Callable
from mogwai.core.traversal import Traversal, AnonymousTraversal
from mogwai.core.traverser import Traverser, Value as TravValue, Property
from mogwai.core.steps.enums import Scope, Order as EnumOrder
from mogwai.core.exceptions import QueryError, GraphTraversalError
from mogwai.decorators import as_traversal_function
from mogwai.utils.type_utils import TypeUtils as tu
import logging
logger = logging.getLogger("Mogwai")

_NA = object()

class Value(MapStep):
    def __init__(self, traversal:Traversal):
        super().__init__(traversal)
        def _map(t:Property|Any)->TravValue:
            if isinstance(t, Property): return t.to_value()
            else: raise GraphTraversalError("Cannot extract value from non-Property object.")
        self._map = _map

class Key(MapStep):
    def __init__(self, traversal:Traversal):
        super().__init__(traversal)
        def _map(t:Property|Any)->TravValue:
            if isinstance(t, Property): return t.to_key()
            else: raise GraphTraversalError("Cannot extract keys from non-Property object.")
        self._map = _map
        
class Id(MapStep):
    def __init__(self, traversal:Traversal):
        super().__init__(traversal)
        def _map(t:Traverser|Any)->TravValue:
            if isinstance(t, Traverser): return t.to_value(t.node_id)
            else: raise GraphTraversalError("Cannot extract id from non-Traverser object.")
        self._map = _map

class Values(MapStep):
    def __init__(self, traversal:Traversal, *keys:str|List[str]):
        super().__init__(traversal=traversal)
        self.indexers = [tu.get_dict_indexer(key, _NA) for key in keys]
        self.keys = [(tuple(key) if type(key) is list else key) for key in keys]
        if len(keys)==0:
            def _map(t:Traverser|Property) -> Generator[TravValue,None,None]:
                #We need to iterate over all keys as there is only one key.
                #First, we need to discover the keys
                if type(t)==Traverser:
                    obj = self.traversal._get_element(t)
                    keys = set(obj.keys())
                    keys.remove("labels")
                else:
                    obj = t.val
                    if isinstance(obj, dict):
                        keys = obj.keys()
                    else:
                        return
                indexers = [tu.get_dict_indexer(key, _NA) for key in keys]
                for i in range(len(keys)):
                    x = indexers[i](obj)
                    if x==_NA: continue
                    elif isinstance(x, list):
                        for y in x:
                            yield t.to_value(y)
                    elif x!=_NA:
                        yield t.to_value(x)
            self._map = _map
        elif len(keys)==1:
            def _map(t:Traverser|Property) -> Generator[TravValue,None,None]:
                #We don't need to iterate over the keys as there is only one key.
                x = self.indexers[0](self.traversal._get_element(t) if type(t)==Traverser else t.val)
                if isinstance(x, list):
                    for y in x:
                        yield t.to_value(y)
                elif x!=_NA:
                    yield t.to_value(x)
            self._map = _map
        else:
            def _map(t:Traverser|Property) -> Generator[TravValue,None,None]:
                #Same as above, but now with an iteration over the keys.
                for i in range(len(self.keys)):
                    x = self.indexers[i](self.traversal._get_element(t) if type(t)==Traverser else t.val)
                    if x == _NA: continue
                    elif isinstance(x, list):
                        for y in x:
                            yield t.to_value(y)
                    else:
                        yield t.to_value(x)
            self._map = _map

    def __call__(self, traversers: Iterable[Traverser] | Iterable[Property]) -> Iterable[TravValue]:
        return (x for t in traversers for x in self._map(t))

class Properties(MapStep):
    def __init__(self, traversal:Traversal, *keys:str|List[str]):
        super().__init__(traversal=traversal)
        self.indexers = [tu.get_dict_indexer(key, _NA) for key in keys]
        self.keys = [(tuple(key) if type(key) is list else key) for key in keys]
        if len(keys)==0:
            def _map(t:Traverser|Property) -> Generator[Property,None,None]:
                #We need to iterate over all keys as there is only one key.
                #First, we need to discover the keys
                if type(t)==Traverser:
                    obj = self.traversal._get_element(t)
                    keys = set(obj.keys())
                    keys.remove("labels")
                else:
                    obj = t.val
                    if isinstance(obj, dict):
                        keys = obj.keys()
                    else:
                        return None
                indexers = [tu.get_dict_indexer(key, _NA) for key in keys]
                keys = [(tuple(key) if type(key) is list else key) for key in keys]
                for i in range(len(keys)):
                    x = indexers[i](obj)
                    if x==_NA: continue
                    elif isinstance(x, dict):
                        for y in x:
                            yield t.to_property(keys[i], {y: x[y]})
                    elif isinstance(x, list):
                        for y in x:
                            yield t.to_property(keys[i], y)
                    elif x!=_NA:
                        yield t.to_property(keys[i], x)
            self._map = _map
        elif len(keys)==1:
            def _map(t:Traverser|Property) -> Generator[Property,None,None]:
                #We don't need to iterate over the keys as there is only one key.
                x = self.indexers[0](self.traversal._get_element(t) if type(t)==Traverser else t.val)
                if isinstance(x, dict):
                    for y in x:
                        yield t.to_property(self.keys[0], {y: x[y]})
                elif isinstance(x, list):
                    for y in x:
                        yield t.to_property(self.keys[0], y)
                elif x!=_NA:
                    yield t.to_property(self.keys[0], x)
            self._map = _map
        else:
            def _map(t:Traverser|Property) -> Generator[Property,None,None]:
                #Same as above, but now with an iteration over the keys.
                for i in range(len(self.keys)):
                    x = self.indexers[i](self.traversal._get_element(t) if type(t)==Traverser else t.val)
                    if x == _NA: continue
                    elif isinstance(x, dict):
                        for y in x:
                            yield t.to_property(self.keys[i], {y: x[y]})
                    elif isinstance(x, list):
                        for y in x:
                            yield t.to_property(self.keys[i], y)
                    else:
                        yield t.to_property(self.keys[i], x)
            self._map = _map

    def __call__(self, traversers: Iterable[Traverser] | Iterable[Property]) -> Iterable[Property]:
        return (x for t in traversers for x in self._map(t))

class ElementMap(MapStep):
    def __init__(self, traversal:Traversal, keys:str|List[str]|None=None):
        super().__init__(traversal=traversal)
        if keys is not None:
            if len(keys)==1: keys = keys[0]
            if type(keys) is list:
                indexers = [tu.get_dict_indexer(key, _NA) for key in keys]
                def _map(t:Traverser|Any) -> dict:
                    if type(t)==Traverser:
                        obj = self.traversal._get_element(t)
                        res = {key: x for key, index in zip(keys, indexers) if (x:=index(obj))!=_NA}
                        return res if len(res)>0 else None
                    else:
                        raise GraphTraversalError("Cannot map non-traverser objects to elements.")
            else:
                indexer = tu.get_dict_indexer(keys, _NA)
                def _map(t:Traverser|Any) -> dict:
                    if type(t)==Traverser:
                        obj = self.traversal._get_element(t)
                        return {keys: x} if (x:=indexer(obj))!=_NA else None
                    else:
                        raise GraphTraversalError("Cannot map non-traverser objects to elements.")
        else:
            def _map(t:Traverser|Property|Any) -> dict:
                if type(t)==Traverser:
                    return self.traversal._get_element(t, data=True)
                elif type(t)==Property:
                    return {"key": t.key, "value": t.value}
                else:
                    raise GraphTraversalError("Cannot map non-traverser objects to elements.")
        self._map = _map

class Select(MapStep):
    def __init__(self, traversal:Traversal, keys:str|List[str], by:List[str]|None=None, **kwargs):
        flags = kwargs.pop('flags', 0)
        super().__init__(traversal=traversal, flags=flags|Select.SUPPORTS_MULTIPLE_BY, **kwargs)
        self.keys = keys
        self.by = [by] if by else []

    def build(self):
        if len(self.by) > 0:
            #First construct the 'by' indexers
            if type(self.keys) is list:
                if len(self.by)==1:
                    self.indexers = [tu.get_dict_indexer(self.by[0])]*len(self.keys)
                elif len(self.by)==len(self.keys):
                    self.indexers = [tu.get_dict_indexer(by) for by in self.by]
                else:
                    raise QueryError("The number of `by`-modulators to `select` should be 1 or equal to the number of keys.")
            else:
                if len(self.by)==1:
                    self.indexers = [tu.get_dict_indexer(self.by[0])]
                else:
                    raise QueryError("The number of `by`-modulators to `select` should be 1 or equal to the number of keys.")

            #We don't want any control-flow (if's) inside the _map function if possible.
            # So we put all if's that only depend on the step's parameters outside the _map function.
            def apply_by_indexer(t:Traverser|TravValue|Property, idx:int):
                if isinstance(t, Traverser):
                    return self.indexers[idx](self.traversal._get_element(t))
                else:
                    raise GraphTraversalError(f"Cannot select object `{type(t)}` by `{self.by[idx] if len(self.by)>1 else self.by[0]}`.")

            #Now, construct the maps using the by indexers
            if type(self.keys) is list:
                def _map(traverser:Traverser|TravValue|Property) -> List[TravValue]:
                    objects = (apply_by_indexer(traverser.load(self.keys[i]), i) for i in range(self.keys))
                    if(isinstance(traverser, Property)):
                        return [traverser.to_value().set_value(obj) for obj in objects]
                    elif(isinstance(traverser, TravValue)):
                        return [traverser.copy().set_value(obj) for obj in objects]
                    else:
                        return [traverser.to_value(obj) for obj in objects]
            else:
                def _map(traverser:Traverser|TravValue|Property) -> TravValue:
                    if isinstance(traverser, Property):
                        return traverser.to_value().set_value(apply_by_indexer(traverser.load(self.keys),0))
                    if(isinstance(traverser, TravValue)):
                        return traverser.set_value(apply_by_indexer(traverser.load(self.keys),0))
                    else:
                        return traverser.to_value(apply_by_indexer(traverser.load(self.keys),0))
        else:
            #construct the maps without any by-modulation
            if type(self.keys) is list:
                def _map(traverser:Traverser|TravValue|Property) -> List:
                    return [traverser.load(key) for key in self.keys]
            else:
                def _map(traverser:Traverser|TravValue|Property) -> Traverser|TravValue|Property:
                    return traverser.load(self.keys)

        self._map = _map

class Order(MapStep):
    def __init__(self, traversal:Traversal, by:str|List[str]|AnonymousTraversal=None, order:EnumOrder|None=None, asc:bool|None=None, **kwargs):
        super().__init__(traversal, flags=Order.SUPPORTS_ANON_BY)

        desc = kwargs.get('desc', False)
        if asc is None:
            asc = True if desc is False else False
        if not(asc ^ desc):
            from mogwai.core.exceptions import QueryError
            raise QueryError("Either `asc` or `desc` should be True")
        self.asc = asc
        self.order = order
        self._by = by
        logger.debug(f"Order is sorting in {('ascending' if self.asc else 'descending')} order")

    @property
    def by(self):
        return self._by

    @by.setter
    def by(self, value):
        if isinstance(value, tuple):
            if len(value)==2:
                if isinstance(value[1], EnumOrder):
                    self.order = value[1]
                else:
                    raise QueryError("Invalid `order` value. Must be an enums.Order object.")
                self._by = value[0]
        elif isinstance(value, EnumOrder):
            self.order = value
        else:
            self._by = value
        
    def build(self):
        if isinstance(self.by, AnonymousTraversal):
            self.by._build(self.traversal)
        if self.order is not None:
            if self.order == EnumOrder.asc:
                self.asc = True
            elif self.order == EnumOrder.desc:
                self.asc = False
            else:
                raise QueryError(f"Unsupported order: {self.order}")

    def __call__(self, traversers: Iterable[Traverser] | Iterable[Any]) -> Iterable[Traverser] | Iterable[Any]:
        def extract_first_item(x):
            if len(x)!=1:
                raise GraphTraversalError(f"Cannot compare more than one or zero objects. Anonymous Traversal returned {len(x)} items.")
            if isinstance(x[0], TravValue):
                return x[0].value
            else:
                raise GraphTraversalError(f"Cannot compare non-values.")
        try:
            logger.debug("Trying to use NumPy for sorting...")
            import numpy as np #try to use numpy's sorting as it is a lot faster in most cases.
            travs, keys = [], []
            if self.by:
                if isinstance(self.by, (str,list)):
                    indexer = tu.get_dict_indexer(self.by,None)
                    for t in traversers:
                        if (x:=indexer(self.traversal._get_element(t))) is not None:
                            travs.append(t)
                            keys.append(x)
                elif isinstance(self.by, AnonymousTraversal):
                    for t in traversers:
                        travs.append(t)
                        keys.append(extract_first_item(tu.ensure_is_list(self.by([t.copy()]))))
            else:
                traversers = tu.ensure_is_list(traversers)
                if(len(traversers)==0): return traversers
                if isinstance(traversers[0], TravValue):
                    travs = traversers
                    keys = [t.value for t in traversers]
                else:
                    raise GraphTraversalError("Cannot order traversers without `by` key")
            keys = np.array(keys)
            sortedidx = np.argsort(keys)
            if not self.asc: sortedidx = sortedidx[::-1]
            #print(sortedidx, keys, travs)
            return np.array(travs)[sortedidx].tolist()
        except:
            logger.debug("Failed to use numpy, falling back to python built-ins")
            if self.by:
                if isinstance(self.by, (str,list)):
                    indexer = tu.get_dict_indexer(self.by,None)
                    sortmap = {t:x for t in traversers if (x:=indexer(self.traversal._get_element(t))) is not None}
                elif isinstance(self.by, AnonymousTraversal):
                    sortmap = {t:extract_first_item(tu.ensure_is_list(self.by([t.copy()]))) for t in traversers}
            else:
                traversers = tu.ensure_is_list(traversers)
                if(len(traversers)==0): return traversers
                if isinstance(traversers[0], TravValue):
                    sortmap = {t:t.value for t in traversers}
                else:
                    raise GraphTraversalError("Cannot order traversers without `by` key")
            return (k for k, _ in sorted(sortmap.items(), key=lambda item: item[1], reverse=not(self.asc)))

    def print_query(self) -> str:
        if isinstance(self.by, AnonymousTraversal):
            return f"{self.__class__.__name__}(by {self.by.print_query()})"
        else:
            return super().print_query()

class Path(MapStep):
    def __init__(self, traversal:Traversal, by:str|List[str]=None):
        super().__init__(traversal, flags=Path.SUPPORTS_BY|Path.NEEDS_PATH)
        self.by = by

    def build(self):
        if(self.by):
            self.indexer = tu.get_dict_indexer(self.by, None)
            self._map = lambda t: t.to_value([self.indexer(self.traversal._get_element_from_id(x)) for x in t.path])
        else:
            self._map = lambda t: t.to_value(t.path)

class Count(MapStep):
    def __init__(self, traversal:Traversal, scope:Scope=Scope.global_):
        super().__init__(traversal)
        self.scope = scope

    def __call__(self, traversers:Iterable[Traverser]) -> List[TravValue]:
        if self.scope==Scope.local:
            def gen():
                for t in traversers:
                    if isinstance(t, TravValue):
                        try:
                            yield t.set_value(len(t.value))
                        except: yield t.set_value(1)
                    else:
                        yield t.to_value(1)
            return gen()
        return [TravValue(len(tu.ensure_is_list(traversers)))]

class Max(MapStep):
    def __init__(self, traversal: Traversal, scope:Scope=Scope.global_):
        super().__init__(traversal)
        self.scope = scope

    def __call__(self, traversers: Iterable[Traverser] | Iterable[Any]) -> Iterable[Traverser] | Iterable[Any]:
        if self.scope == Scope.global_:
            max_val = None
            max_items = []
            try:
                for trav in traversers:
                    if isinstance(trav, TravValue):
                        if(max_val is None or trav.val >= max_val):
                            if(max_val==trav.val):
                                max_items.append(trav)
                            else:
                                max_items = [trav]
                                max_val = trav.val
                    else:
                        raise GraphTraversalError("Cannot compare non-value traversers.")
            except TypeError as e:
                raise GraphTraversalError("Cannot compare values: " + str(e))
            return max_items
        else:
            def _map(t:Traverser):
                if isinstance(t, TravValue):
                    return max(t)
                else:
                    raise GraphTraversalError("Cannot compare non-value traversers.")
            self._map = _map
            return super().__call__(traversers)

class Min(MapStep):
    def __init__(self, traversal: Traversal, scope:Scope=Scope.global_):
        super().__init__(traversal)
        self.scope = scope

    def __call__(self, traversers: Iterable[Traverser] | Iterable[Any]) -> Iterable[Traverser] | Iterable[Any]:
        if self.scope == Scope.global_:
            min_val = None
            min_items = []
            try:
                for trav in traversers:
                    if isinstance(trav, TravValue):
                        if(min_val is None or trav.val <= min_val):
                            if(min_val==trav.val):
                                min_items.append(trav)
                            else:
                                min_items = [trav]
                                min_val = trav.val
                    else:
                        raise GraphTraversalError("Cannot compare traversers that reference elemnts.")
            except TypeError as e:
                raise GraphTraversalError("Cannot compare values: " + str(e))
            return min_items
        else:
            def _map(t:Traverser):
                if isinstance(t, TravValue):
                    return max(t)
                else:
                    raise GraphTraversalError("Cannot compare non-value traversers.")
            self._map = _map
            return super().__call__(traversers)

class Aggregate(MapStep):
    def __init__(self, traversal: Traversal, aggfunc:str, scope:Scope=Scope.global_):
        super().__init__(traversal)
        if aggfunc in ['mean', 'sum']:
            self.aggfunc = aggfunc
        else:
            raise QueryError(f"Unsupported aggregation function `{aggfunc}`")
        self.scope = scope

    def __call__(self, traversers: Iterable[Traverser] | Iterable[Any]) -> Iterable[Traverser] | Iterable[Any]:
        from numbers import Number
        if self.scope == Scope.global_:
            #first ensure that all items are numbers
            numbers = []
            try:
                for trav in traversers:
                    if isinstance(trav, TravValue) and isinstance(trav.val, Number):
                        numbers.append(trav.val)
                    else:
                        raise GraphTraversalError("Encountered non-numeric traverser.")
            except TypeError:
                raise GraphTraversalError("Encountered non-numeric traverser.")
            match self.aggfunc:
                case "mean":
                    return [TravValue(sum(numbers)/len(numbers))]
                case "sum":
                    return [TravValue(sum(numbers))]
        else:
            if self.aggfunc == "sum":
                def _map(t:Traverser|TravValue):
                    if isinstance(t, TravValue) and isinstance(t.val, Iterable):
                        return t.set_value(sum(t.val))
            elif(self.aggfunc == 'mean'):
                def _map(t:Traverser|TravValue):
                    if isinstance(t, TravValue) and isinstance(t.val, Iterable):
                        val = tu.ensure_is_list(t.val)
                        return t.set_value(sum(t.val)/len(t.val))
            self._map = _map
            return super().__call__(traversers)

@as_traversal_function
def element_map(*keys:str) -> 'Traversal':
    if len(keys) == 1:
        keys = keys[0]
    elif len(keys)==0:
        keys=None
    return ElementMap(None, keys)

@as_traversal_function
def value():
    return Value(None)

@as_traversal_function
def key():
    return Key(None)

@as_traversal_function
def id_():
    return Id(None)

@as_traversal_function
def values(*keys:str|List[str]):
    return Values(None, *keys)

@as_traversal_function
def name():
    return Values(None, "name")

@as_traversal_function
def label():
    return Values(None, "labels")

@as_traversal_function
def properties(*keys:str|List[str]):
    return Properties(*keys)

@as_traversal_function
def select(*args:str, by:str=None):
    return Select(None, keys=args[0] if len(args)==1 else list(args), by=by)

@as_traversal_function
def path(by:str|List[str]):
    return Path(None, by=by)

@as_traversal_function
def count(scope:Scope=Scope.global_):
    return Count(None, scope)

@as_traversal_function
def max_(scope:Scope=Scope.global_):
    return Max(None, scope)

@as_traversal_function
def min_(scope:Scope=Scope.global_):
    return Min(None, scope)

@as_traversal_function
def mean(scope:Scope=Scope.global_):
    return Aggregate(None, 'mean', scope)

@as_traversal_function
def sum_(scope:Scope=Scope.global_):
    return Aggregate(None, 'sum', scope)