"""
Created on 2024

@author: Joep Geuskens
"""
import logging
from copy import deepcopy
from typing import Any
from abc import ABC, abstractmethod
logger = logging.getLogger("Mogwai")

class BaseTraverser(ABC):
    def __init__(self, track_path: bool = False):
        self.track_path = track_path
        self.cache = {"__store__": {}}
        self.path = []

    def to(self, other:'BaseTraverser') -> 'BaseTraverser':
        other.cache = deepcopy(self.cache)
        other.path = self.path.copy() #we assume that elements in the path don't change
        return other

    @abstractmethod
    def copy(self) -> 'BaseTraverser':
        pass

class Traverser(BaseTraverser):
    """
    see https://tinkerpop.apache.org/javadocs/3.7.3/core/org/apache/tinkerpop/gremlin/process/traversal/Traverser.html

    A Traverser represents the current state of an object
    flowing through a Traversal.

    A traverser maintains a reference to the current object,
    a traverser-local "sack",
    a traversal-global sideEffect, a bulk count,
    and a path history.

    Different types of traversers can exist
    depending on the semantics of the traversal
    and the desire for space/time optimizations of
    the developer.
    """

    def __init__(
        self, node_id: str, other_node_id: str = None, track_path: bool = False
    ):
        super().__init__(track_path=track_path)
        self.node_id = node_id
        self.track_path = track_path
        self.target = other_node_id
        self.path = [self.get] if track_path else None

    def move_to_edge(self, source: str, target: str) -> None:
        self.node_id = source
        self.target = target
        if self.track_path:
            self.path.append((source, target))

    @property
    def get(self) -> str | tuple:
        return (self.node_id, self.target) if self.is_edge else self.node_id

    @property
    def source(self) -> str:
        return self.node_id

    @property
    def is_edge(self) -> bool:
        return self.target is not None

    def save(self, key):
        if self.is_edge:
            to_store = Traverser(*self.get, track_path=self.track_path)
        else:
            to_store = Traverser(self.get, track_path=self.track_path)

        to_store.cache = self.cache.copy()  # no need to deep copies
        self.cache["__store__"][key] = to_store

    def load(self, key):
        # logger.debug(f"Cache: {self.cache['__store__'].keys()}")
        try:
            return self.cache["__store__"][key]
        except KeyError:
            raise ValueError(
                f"No object `{key}` was saved in this traverser. Use .as('{key}') to save traversal steps."
            )

    def get_cache(self, key):
        return self.cache.get(key, None)

    def set(self, key: str, val: Any):
        assert key != "__store__", "`__store__` is a reserved key"
        self.cache[key] = val

    def move_to(self, node_id: str) -> "Traverser":
        # logging.debug("Moving traverser from", self.get, "to", node_id)
        self.node_id = node_id
        self.target = None
        if self.track_path:
            self.path.append(node_id)
        return self

    def copy(self):
        t = Traverser(
            node_id=self.node_id, other_node_id=self.target, track_path=self.track_path
        )
        t.cache = deepcopy(self.cache)
        t.path = self.path.copy() if self.path else None
        return t

    def copy_to(self, node_id: str, other_node_id: str = None) -> "Traverser":
        t = self.copy()
        if other_node_id:
            t.move_to_edge(node_id, other_node_id)
        else:
            t.move_to(node_id)
        return t

    def to_value(self, val, dtype=None):
        val = Value(val, dtype=dtype)
        val.cache = deepcopy(self.cache)
        if self.track_path:
            val.path = self.path.copy()
        return val

    def to_property(self, key, val, dtype=None):
        p = Property(key, val, dtype=dtype)
        p.cache = deepcopy(self.cache)
        if self.track_path:
            p.path = self.path.copy()
        return p

    def __str__(self):
        return f"<{self.__class__.__name__}[get={self.get}, is_edge={self.is_edge}]>"


class Value(BaseTraverser):
    def __init__(self, val, dtype=None, track_path:bool=False):
        super().__init__(track_path=track_path)
        if dtype:
            self.val = dtype(val)
            self.dtype = dtype
        else:
            self.val = val
            self.dtype = type(val)
        self.path = [val] if track_path else None

    @property
    def value(self):
        return self.val

    def set_value(self, val):
        self.val = val
        self.dtype = type(val)
        if self.track_path:
            self.path.append(val)
        return self

    def save(self, key):
        self.cache["__store__"][key] = self.copy()

    def load(self, key):
        # logger.debug(f"Cache: {self.cache['__store__'].keys()}")
        try:
            return self.cache["__store__"][key]
        except KeyError:
            raise ValueError(
                f"No object `{key}` was saved in this traverser. Use .as('{key}') to save traversal steps."
            )

    def get_cache(self, key):
        return self.cache.get(key, None)

    def set(self, key: str, val: Any):
        assert key != "__store__", "`__store__` is a reserved key"
        self.cache[key] = val

    def copy(self):
        val = Value(deepcopy(self.value))
        val.cache = deepcopy(self.cache)
        if self.track_path:
            val.path = self.path.copy()
        return val

    def __str__(self):
        return f"{self.__class__.__qualname__}[value={self.val}]"

class Property(Value):
    def __init__(self, key: str, val: Any, dtype=None, track_path=False):
        super().__init__(val, dtype=dtype, track_path=track_path)
        self.key = key
        self.path = [{key: val}] if track_path else None

    def get_key(self):
        return self.key

    def set_value(self, val):
        self.val = val
        self.dtype = type(val)
        if self.track_path:
            self.path.append({self.key: val})
        return self

    def to_value(self):
        val = Value(self.val, dtype=self.dtype)
        val.cache = deepcopy(self.cache)
        if self.track_path:
            val.path = self.path.copy()
        return val

    def to_key(self):
        val = Value(self.key, dtype=str)
        val.cache = deepcopy(self.cache)
        if self.track_path:
            val.path = self.path.copy()
        return val

    def to_dict(self):
        return {self.key: self.val}

    def copy(self):
        prop = Property(deepcopy(self.key), deepcopy(self.value))
        prop.cache = deepcopy(self.cache)
        if self.track_path:
            prop.path = self.path.copy()
        return prop

    def __str__(self):
        return f"{self.__class__.__qualname__}[key={self.key}, value={self.val}]"
