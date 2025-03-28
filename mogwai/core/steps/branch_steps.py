from .base_steps import BranchStep, MapStep
from mogwai.core import Traversal, AnonymousTraversal
from mogwai.core.traverser import Traverser, Value
from mogwai.core.exceptions import GraphTraversalError, QueryError
from mogwai.decorators import as_traversal_function, with_call_order
from mogwai.utils.type_utils import TypeUtils as tu
from typing import Iterable, Dict, Any, Tuple, Set
import logging
logger = logging.getLogger("Mogwai")

_NA = object()

class Repeat(BranchStep):
    def __init__(self, traversal:Traversal, do:AnonymousTraversal, times:int=None, until:AnonymousTraversal=None, until_do:bool=False):
        super().__init__(traversal=traversal)
        self.until_do = until_do
        self.do = do
        self.register_anon_traversal(do)
        self.times = times
        self._emit = None #set by Traversal
        self.emit_before = False
        self._until = None
        if until is not None:
            self.until = until
    
    @property
    def until(self) -> AnonymousTraversal:
        return self._until
    
    @until.setter
    def until(self, until:AnonymousTraversal):
        self._until = until
        self.register_anon_traversal(until)

    @property
    def emit(self) -> bool | AnonymousTraversal:
        return self._emit

    @emit.setter
    def emit(self, emit:bool|AnonymousTraversal):
        self._emit = emit
        if isinstance(emit, AnonymousTraversal):
            self.register_anon_traversal(emit)

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        if self.emit:
            def emit_filter(travs:Iterable[Traverser]):
                if(self.emit==True):
                    return travs
                else:
                    return (t for t in travs if next(iter(self.emit([t])), _NA)!=_NA)

        if self.times:
            result = (emit_filter(traversers) if self.emit_before else []) if self.emit else None
            for _ in range(self.times):
                traversers = self.do(traversers)
                if(self.emit):
                    traversers = tu.ensure_is_list(traversers)
                    result.extend(emit_filter(traversers))
            return result or traversers
        else:
            #prepare by setting the counter to 0 and defining the steps
            self.counter = 0
            def do_step(travs:Iterable[Traverser]) -> Iterable[Traverser]:
                travs = self.do(travs)
                self.counter += 1
                if(self.counter>=self.traversal.max_iteration_depth):
                    raise GraphTraversalError("Max iteration depth reached in Repeat step")
                return travs
            def until_step(travs:Iterable[Traverser]) -> Tuple[Set[Traverser], Set[Traverser]]:
                travs = tu.ensure_is_set(travs)
                #we can't use generators here
                condition_fulfilled = {t for t in travs if next(iter(self.until([t.copy()])), _NA)!=_NA}
                #for remaining_travs the condition is fullfilled
                return condition_fulfilled, travs-condition_fulfilled

            #initialize the result
            result = set()
            emitted = set()
            traversers = tu.ensure_is_list(traversers)
            if self.emit_before:
                emitted = set(emit_filter(traversers))
            #set all traversers to be active
            active_traversers = traversers

            if self.until_do:
                while True:
                    _result, active_traversers = until_step(active_traversers)
                    result |= _result
                    if(len(active_traversers)==0):
                        break
                    active_traversers = do_step(active_traversers)
                    if self.emit:
                        emitted |= set(emit_filter(active_traversers))
            else:
                while(len(active_traversers)>0):
                    active_traversers = do_step(active_traversers)
                    _result, active_traversers = until_step(active_traversers)
                    result |= _result
                    if self.emit:
                        emitted |= set(emit_filter(active_traversers))
            if self.emit:
                return emitted | set(emit_filter(result))
            return result

    def print_query(self) -> str:
        if self.times:
            return f"{self.__class__.__name__}({self.do.print_query()}, x{self.times})"

        if self.until_do:
            s = f"until {self.until.print_query()}, do {self.do.print_query()}"
        else:
            s = f"do {self.do.print_query()}, until {self.until.print_query()}"
        if self.emit:
            from mogwai.core import AnonymousTraversal
            emitstr = f"emit {self.emit.print_query()}" if isinstance(self.emit, AnonymousTraversal) else "emit"
            if self.emit_before:
                s = f"{emitstr}, {s}"
            else:
                s = f"{s}, {emitstr}"
        return f"{self.__class__.__name__}({s})"

class Branch(BranchStep):
    def __init__(self, traversal:'Traversal',branchFunc:'AnonymousTraversal'):
        super().__init__(traversal=traversal)
        self.branchFunc = branchFunc
        self.options:Dict[Any,'AnonymousTraversal'] = {}
        self.defaultStep=None
        self.register_anon_traversal(branchFunc)

    def add_option(self, key, option_trav:'AnonymousTraversal'):
        if key not in self.options:
            self.options[key] = option_trav
            self.register_anon_traversal(option_trav)
        else:
            raise QueryError("Duplicate key " + str(key) + ", please use distinct keys")

    def set_default(self, default_step:'AnonymousTraversal'):
        if self.defaultStep is None:
            self.defaultStep = default_step
            self.register_anon_traversal(default_step)
        else:
            raise QueryError("Provided two default (None) options. This is not allowed")
        
    def build(self):
        super().build()
        if not isinstance(self.branchFunc.query_steps[-1], MapStep):
            raise QueryError(f"The branch function (anonymous traversal) should end with a Map step (currently, it is {self.branchFunc.print_query()})")

    def __call__(self,traversers:Iterable[Traverser])->Iterable[Traverser]:
        valueTraverserPairs = []
        for traverser in traversers:
            ret = self.branchFunc([traverser.copy()])
            ret = next(iter(ret), _NA)
            if ret!=_NA:
                if isinstance(ret, Value):
                    valueTraverserPairs.append((traverser,ret.value))
                elif isinstance(ret, Traverser):
                    valueTraverserPairs.append((traverser,ret.get))
                else:
                    raise GraphTraversalError(f"Branch function should return a Value or a Traverser, got {type(ret)}")
        ret = []
        for valueTraverserPair in valueTraverserPairs:
            if valueTraverserPair[1] in self.options.keys():
                func = self.options[valueTraverserPair[1]]
                ret.extend(list(func([valueTraverserPair[0]])))
            else:
                if self.defaultStep is not None:
                    ret.extend(list(self.defaultStep([valueTraverserPair[0]])))
                #else:
                #    ret.append(valueTraverserPair[0])
        return ret

    def print_query(self) -> str:
        options_str = {f"'{k}'": at.print_query() for k, at in self.options.items()}
        if self.defaultStep is not None:
            options_str['default'] = self.defaultStep.print_query()
        return self.__class__.__name__ + f"({self.branchFunc.print_query()}: " + "{" \
            + ", ".join((f"{k}: {option}" for k, option in options_str.items())) + "})"

class Union(BranchStep):
    def __init__(self, traversal:Traversal, *traversals:AnonymousTraversal):
        super().__init__(traversal=traversal)
        if len(traversals)<1:
            raise QueryError("Union requires at least 1 traversal.")
        self.register_anon_traversal(*traversals)

    def __call__(self, traversers:Iterable[Traverser]) -> Iterable[Traverser]:
        #Isn't generator comprehension beautiful?
        return (t for trav in traversers for anon_traversal in self.anon_traversals for t in anon_traversal([trav]))

    def print_query(self) -> str:
        return f"{self.__class__.__name__}(" + ", ".join((t.print_query() for t in self.anon_traversals)) + ")"

class Local(BranchStep):
    def __init__(self, traversal:Traversal, localTrav:AnonymousTraversal):
        super().__init__(traversal=traversal)
        self.register_anon_traversal(localTrav)
    
    def __call__(self, traversers: Iterable[Traverser]) -> Iterable[Traverser]:
        return (t1 for t in traversers for t1 in self.anon_traversals[0]([t]))

    def print_query(self) -> str:
        return f"{self.__class__.__name__}({self.anon_traversals[0].print_query()})"

@with_call_order
@as_traversal_function
def repeat(do:'AnonymousTraversal', times:int=None, until:'AnonymousTraversal|None'=None, **kwargs):
        if(until is not None):
            until_do = len(kwargs.get("_order", []))>0 and kwargs["_order"][0]=="until"
        else: until_do = None
        return Repeat(None, do, times=times, until=until, until_do=until_do)

@as_traversal_function
def branch(branchFunc:'AnonymousTraversal'):
    return Branch(None, branchFunc)

@as_traversal_function
def union(*traversals:AnonymousTraversal):
    return Union(None, *traversals)