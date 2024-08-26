from .base_steps import Step
from mogwai.core.traversal import Traversal
from mogwai.core.traverser import Traverser, Value, Property
from typing import Set, List, Iterable, Generator, Any
from mogwai.utils import get_dict_indexer, ensure_is_list
import logging
logger = logging.getLogger("Mogwai")

class ToList(Step):
    def __init__(self, traversal:Traversal, by:List[str]|str=None, include_data:bool=True, **kwargs):
        super().__init__(traversal, flags=Step.ISTERMINAL|Step.SUPPORTS_BY, **kwargs)
        self.data = include_data
        self.by = by

    def __call__(self, traversers:Iterable[Traverser]|Iterable[Value]|Iterable[Property]) -> List[Any]:
        if self.by:
            get_property = get_dict_indexer(self.by)
        '''def convert_func(trav):
            if isinstance(trav, Iterable):
                #we've got nested data...
                logger.debug("Recursively listing traversers...")
                return [convert_func(t) for t in trav]
            elif isinstance(trav, Traverser):
                if(self.data):
                    return self.traversal._get_element(trav, data=True)
                elif(self.by):
                    return get_property(self.traversal._get_element(trav))
                else:
                    return trav.get
            elif isinstance(trav, Property):
                return {trav.key: trav.value}
            elif isinstance(trav, Value):
                return trav.value
            else:
                return trav
        return convert_func(traversers)'''
        return list(AsGenerator(self.traversal, by=self.by, include_data=self.data)(traversers))

class Next(Step):
    def __init__(self, traversal:Iterable):
        super().__init__(traversal, flags=Step.ISTERMINAL)

    def __call__(self, traversers:Iterable[Traverser]|Iterable[Value]|Iterable[Property]) -> Any:
        x = next(iter(traversers), False)
        if isinstance(x, Traverser):
            return x.get
        elif isinstance(x, Property):
            return {x.key: x.value}
        elif isinstance(x, Value):
            return x.value
        else: return x

class AsGenerator(Step):
    def __init__(self, traversal:Traversal, by:List[str]|str=None, include_data:bool=True, **kwargs):
        super().__init__(traversal, flags=Step.ISTERMINAL|Step.SUPPORTS_BY, **kwargs)
        self.data = include_data
        self.by = by

    def __call__(self, traversers:Iterable[Traverser]|Iterable[Value]|Iterable[Property]) -> Generator:
        if self.by:
            get_property = get_dict_indexer(self.by)
        def convert_func(trav):
            if isinstance(trav, (list, tuple, set)):
                #we've got nested data...
                logger.debug("Recursively listing traversers...")
                return [convert_func(t) for t in trav]
            elif isinstance(trav, Traverser):
                if(self.data):
                    return self.traversal._get_element(trav, data=True)
                elif(self.by):
                    return get_property(self.traversal._get_element(trav))
                else:
                    return trav.get
            elif isinstance(trav, Property):
                return {trav.key: trav.value}
            elif isinstance(trav, Value):
                return trav.value
            else:
                return trav
        return (convert_func(trav) for trav in traversers)

class HasNext(Step):
    def __init__(self, traversal:Iterable):
        super().__init__(traversal, flags=Step.ISTERMINAL)

    def __call__(self, traversers:Iterable[Traverser]|Iterable[Value]|Iterable[Property]) -> bool:
        NA = object()
        try:
            x = next(iter(traversers), NA)
            return x != NA
        except StopIteration:
            return False

class AsPath(Step):
    def __init__(self, traversal:Iterable, by:List[str]|str=None, **kwargs):
        super().__init__(traversal, flags=Step.ISTERMINAL|Step.NEEDS_PATH|Step.SUPPORTS_BY, **kwargs)
        self.by = by

    def __call__(self, traversers:Iterable[Traverser]) -> List[List[int]]:
        if(self.by):
            get_prop = get_dict_indexer(self.by, None)
            paths = [[get_prop(self.traversal._get_element_from_id(item)) for item in t.path]
                     for t in traversers]
        else:
            paths = [t.path for t in traversers]
        return paths
    
class Iterate(AsGenerator):
    def __init__(self, traversal:Iterable):
        super().__init__(traversal)
        self.flags = Step.ISTERMINAL
    
    def __call__(self, traversers:Iterable[Traverser]|Iterable[Value]|Iterable[Property]) -> None:
        for _ in traversers: pass #TODO: optimize this: we needn't execute this if there are no side effects!