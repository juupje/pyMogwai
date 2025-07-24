import logging
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Set, Tuple

from mogwai.config import DEFAULT_ITERATION_DEPTH, USE_MULTIPROCESSING
from mogwai.core.exceptions import GraphTraversalError
from mogwai.core.steps.enums import Cardinality
from mogwai.core.steps.enums import Order as EnumOrder
from mogwai.core.steps.enums import Scope
from mogwai.core.traverser import Traverser
from mogwai.decorators import add_camel_case_methods, with_call_order
from mogwai.utils.type_utils import TypeUtils as tu

from .exceptions import QueryError
from .mogwaigraph import MogwaiGraph
from .steps.base_steps import Step

logger = logging.getLogger("Mogwai")


def step_method(not_anonymous=False):
    def decorator(func):
        """Decorator to mark methods as step methods while preserving type hints."""
        func._anonymous = not (not_anonymous)
        func._is_step_method = True

        @wraps(func)  # Preserves metadata and type hinting
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator


@add_camel_case_methods
class Traversal:
    """
    see https://tinkerpop.apache.org/javadocs/3.7.3/core/org/apache/tinkerpop/gremlin/process/traversal/Traversal.html

    This class represents the base class for all traversals. Each traversal is a directed walk over a graph, executed
    using an iterator-based traversal.
    You shouldn't create instances of this class directly, but instead use a Traversal Source, (e.g. the `MogwaiGraphTraversalSource`)
    to create a new traversal.
    Then, you can chain traversal steps to create a query that will be executed when you call the `run()` method.
    """

    # the following comment is misleading as it does not apply to this specific Traversal class
    """
    see https://tinkerpop.apache.org/javadocs/3.7.3/core/org/apache/tinkerpop/gremlin/process/traversal/Traversal.html

    A Traversal represents a directed walk over a Graph.
    This is the base interface for all traversals,
    where each extending interface is seen as a domain
    specific language. For example, GraphTraversal
    is a domain specific language for traversing a graph
    using "graph concepts" (e.g. vertices, edges).

    A Traversal is evaluated in one of two ways:
    iterator-based OLTP or GraphComputer-based OLAP.
    OLTP traversals leverage an iterator and are executed
    within a single execution environment (e.g. JVM)
    (with data access allowed to be remote).

    OLAP traversals leverage GraphComputer and are executed
    between multiple execution environments (e.g.JVMs) (and/or cores).
    """
    __steps__ = set()

    def __init__(
        self,
        source: "MogwaiGraphTraversalSource",
        start: "Step",
        optimize: bool = True,
        eager: bool = False,
        query_verify: bool = False,
        use_mp: bool = False,
    ):
        if start is None:
            raise QueryError("start step cannot be None")
        self.query_steps = [start]
        if not self.query_steps[0].isstart:
            raise QueryError(
                "The first step should be a start-step, got " + str(self.query_steps[0])
            )
        self.graph = source.connector
        self.terminated = False
        self.eager = eager
        self.use_mp = use_mp
        self.verify_query = query_verify
        self.optimize = optimize
        self.max_iteration_depth = DEFAULT_ITERATION_DEPTH

    def number_of_steps(self, recursive: bool = False) -> int:
        if recursive:
            return sum(
                [step.number_of_steps(recursive=True) for step in self.query_steps]
            )
        return len(self.query_steps)

    def _add_step(self, step: "Step"):
        if self.terminated:
            raise QueryError("Cannot add steps to a terminated traversal.")
        self.query_steps.append(step)
        if step.isterminal:
            self.terminated = True

    ## ===== FILTER STEPS ======
    @step_method()
    def filter_(self, condition: "AnonymousTraversal") -> "Traversal":
        from .steps.filter_steps import Filter

        self._add_step(Filter(self, condition))
        return self

    @step_method()
    def has(self, *args) -> "Traversal":
        """
        Filter traversers based on whether they have the given properties.
        * If one argument is given, it is assumed to be a key, and the step checks if a property with that key exists, regardless of its value.
        * If two arguments are given, it is assumed to be a key and a value, and the step checks if a property with that key exists and has the given value.
        * If three arguments are given, the first argument is assumed to be a label, and the step checks if a property with the given key and value exists on an element with that label.
        """
        # if `key` is a list, like ['a', 'b'], the value will be compared to data['a']['b']
        from .steps.filter_steps import Has

        if len(args) == 1:
            key, value = args[0], None
            self._add_step(Has(self, key, value))
        elif len(args) == 2:
            key, value = args
            self._add_step(Has(self, key, value))
        elif len(args) == 3:
            label, key, value = args
            self._add_step(Has(self, key, value, label=label))
        else:
            raise QueryError("Invalid number of arguments for `has`")
        return self

    @step_method()
    def has_not(self, key: str):
        from .steps.filter_steps import HasNot

        self._add_step(HasNot(self, key))
        return self

    @step_method()
    def has_key(self, *keys: str):
        from .steps.filter_steps import HasKey

        self._add_step(HasKey(self, *keys))
        return self

    @step_method()
    def has_value(self, *values: Any) -> "Traversal":
        from .steps.filter_steps import HasValue

        self._add_step(HasValue(self, *values))
        return self

    @step_method()
    def has_id(self, *ids: str | tuple) -> "Traversal":
        from .steps.filter_steps import HasId

        self._add_step(HasId(self, *ids))
        return self

    @step_method()
    def has_name(self, *name: str) -> "Traversal":
        if len(name) == 0:
            raise QueryError("No name provided for `has_name`")
        elif len(name) == 1:
            return self.has("name", name[0])
        elif len(name) > 1:
            from .steps.filter_steps import HasWithin

            self._add_step(HasWithin(self, "name", name))
            return self

    @step_method()
    def has_label(self, label: str | Set[str]) -> "Traversal":
        if isinstance(label, set):
            from .steps.filter_steps import ContainsAll

            self._add_step(ContainsAll(self, "labels", label))
        else:
            from .steps.filter_steps import Contains

            self._add_step(Contains(self, "labels", label))
        return self

    @step_method()
    def is_(self, condition: Any) -> "Traversal":
        from .steps.filter_steps import Is

        self._add_step(Is(self, condition))
        return self

    @step_method()
    def contains(self, key: str | List[str], value: Any) -> "Traversal":
        if isinstance(value, list):
            from .steps.filter_steps import ContainsAll

            self._add_step(ContainsAll(self, key, value))
        else:
            from .steps.filter_steps import Contains

            self._add_step(Contains(self, key, value))
        return self

    @step_method()
    def within(self, key: str | List[str], options: List[Any]) -> "Traversal":
        from .steps.filter_steps import Within

        self._add_step(Within(self, key, options))
        return self

    @step_method()
    def simple_path(self, by: str | List[str] = None) -> "Traversal":
        from .steps.filter_steps import SimplePath

        self._add_step(SimplePath(self, by=by))
        return self

    @step_method()
    def limit(self, n: int) -> "Traversal":
        from .steps.filter_steps import Range

        self._add_step(Range(self, 0, n))
        return self

    @step_method()
    def range(self, start: int, end: int) -> "Traversal":
        from .steps.filter_steps import Range

        self._add_step(Range(self, start, end))
        return self

    @step_method()
    def skip(self, n: int) -> "Traversal":
        from .steps.filter_steps import Range

        self._add_step(Range(self, n, -1))
        return self

    @step_method()
    def dedup(self, by: str | List[str] = None) -> "Traversal":
        from .steps.filter_steps import Dedup

        self._add_step(Dedup(self, by=by))
        return self

    @step_method()
    def not_(self, condition: "AnonymousTraversal") -> "Traversal":
        from .steps.filter_steps import Not

        self._add_step(Not(self, condition))
        return self

    @step_method()
    def and_(self, A: "AnonymousTraversal", B: "AnonymousTraversal") -> "Traversal":
        from .steps.filter_steps import And

        self._add_step(And(self, A, B))
        return self

    @step_method()
    def or_(self, A: "AnonymousTraversal", B: "AnonymousTraversal") -> "Traversal":
        from .steps.filter_steps import Or

        self._add_step(Or(self, A, B))
        return self

    ## ===== MAP STEPS ======
    @step_method()
    def identity(self) -> "Traversal":  # required for math reasons
        return self

    @step_method()
    def id_(self) -> "Traversal":
        from .steps.map_steps import Id

        self._add_step(Id(self))
        return self

    # Important: `value` extract values from *Property's*
    # `values` extracts values from *elements*!
    # So, .properties(key).value() is the same as .values(key)
    @step_method()
    def value(self) -> "Traversal":
        from .steps.map_steps import Value

        self._add_step(Value(self))
        return self

    @step_method()
    def key(self) -> "Traversal":
        from .steps.map_steps import Key

        self._add_step(Key(self))
        return self

    @step_method()
    def values(self, *keys: str | List[str]) -> "Traversal":
        from .steps.map_steps import Values

        self._add_step(Values(self, *keys))
        return self

    @step_method()
    def name(self) -> "Traversal":
        return self.values("name")

    @step_method()
    def label(self) -> "Traversal":
        return self.values("labels")

    @step_method()
    def properties(self, *keys: str | List[str]) -> "Traversal":
        from .steps.map_steps import Properties

        self._add_step(Properties(self, *keys))
        return self

    @step_method()
    def select(self, *args: str, by: str = None) -> "Traversal":
        from .steps.map_steps import Select

        self._add_step(
            Select(self, keys=args[0] if len(args) == 1 else list(args), by=by)
        )
        return self

    @step_method()
    def order(
        self,
        by: str | List[str] | EnumOrder | "AnonymousTraversal" = None,
        asc: bool | None = None,
        **kwargs,
    ) -> "Traversal":
        from .steps.map_steps import Order

        if isinstance(by, EnumOrder):
            if asc is not None:
                raise QueryError("Cannot provide `asc` argument when `by` is an Order")
            self._add_step(Order(self, order=by, **kwargs))
        else:
            self._add_step(Order(self, by, asc=asc, **kwargs))
        return self

    @step_method()
    def fold(self, seed: Any = None, foldfunc: Callable[[Any, Any], Any] = None):
        from .steps.map_steps import Fold

        self._add_step(Fold(self, seed, foldfunc))
        return self

    @step_method()
    def count(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Count

        self._add_step(Count(self, scope))
        return self

    @step_method()
    def path(self, by: str | List[str] = None) -> "Traversal":
        from .steps.map_steps import Path

        self._add_step(Path(self, by=by))
        return self

    @step_method()
    def max_(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Max

        self._add_step(Max(self, scope))
        return self

    @step_method()
    def min_(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Min

        self._add_step(Min(self, scope))
        return self

    @step_method()
    def sum_(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Aggregate

        self._add_step(Aggregate(self, "sum", scope))
        return self

    @step_method()
    def mean(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Aggregate

        self._add_step(Aggregate(self, "mean", scope))
        return self

    @step_method()
    def element_map(self, *keys: str) -> "Traversal":
        from .steps.map_steps import ElementMap

        if len(keys) == 1:
            keys = keys[0]
        elif len(keys) == 0:
            keys = None
        self._add_step(ElementMap(self, keys))
        return self

    ## ===== FLATMAP STEPS ======
    @step_method()
    def out(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import Out

        self._add_step(Out(self, direction))
        return self

    @step_method()
    def outE(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import OutE

        self._add_step(OutE(self, direction))
        return self

    @step_method()
    def outV(self) -> "Traversal":
        from .steps.flatmap_steps import OutV

        self._add_step(OutV(self))
        return self

    @step_method()
    def in_(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import In

        self._add_step(In(self, direction))
        return self

    @step_method()
    def inE(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import InE

        self._add_step(InE(self, direction))
        return self

    @step_method()
    def inV(self) -> "Traversal":
        from .steps.flatmap_steps import InV

        self._add_step(InV(self))
        return self

    @step_method()
    def both(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import Both

        self._add_step(Both(self, direction))
        return self

    @step_method()
    def bothE(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import BothE

        self._add_step(BothE(self, direction))
        return self

    @step_method()
    def bothV(self) -> "Traversal":
        from .steps.flatmap_steps import BothV

        self._add_step(BothV(self))
        return self

    ## ===== BRANCH STEPS =====
    @step_method()
    @with_call_order
    def repeat(
        self,
        do: "Traversal",
        times: int | None = None,
        until: "AnonymousTraversal|None" = None,
        **kwargs,
    ) -> "Traversal":
        from .steps.branch_steps import Repeat
        from .steps.modulation_steps import Temp

        if until is not None:
            until_do = (
                len(kwargs.get("_order", [])) > 0 and kwargs["_order"][0] == "until"
            )
        else:
            until_do = None

        step = Repeat(self, do, times=times, until=until, until_do=until_do)
        while isinstance((prevstep := self.query_steps[-1]), Temp):
            if prevstep.kwargs["type"] == "emit":
                step.emit = prevstep.kwargs["filter"]
                step.emit_before = True
            elif prevstep.kwargs["type"] == "until":
                if until is not None or times is not None:
                    raise QueryError(
                        "Provided `until` to repeat when `times` or `until` was already set."
                    )
                step.until = prevstep.kwargs["cond"]
                step.until_do = True
            else:
                break
            self.query_steps.pop(-1)  # remove the temporary step
        self._add_step(step)
        return self

    @step_method()
    def local(self, localTrav: "AnonymousTraversal") -> "Traversal":
        from .steps.branch_steps import Local

        self._add_step(Local(self, localTrav))
        return self

    @step_method()
    def branch(self, branchFunc: "AnonymousTraversal") -> "Traversal":
        from .steps.branch_steps import Branch

        if branchFunc.number_of_steps() == 0:
            raise QueryError("No steps provided for branch function")
        self._add_step(Branch(self, branchFunc))
        return self

    @step_method()
    def union(self, *traversals: "AnonymousTraversal") -> "Traversal":
        from .steps.branch_steps import Union

        self._add_step(Union(self, *traversals))
        return self

    ## ===== SIDE EFFECT STEPS ======
    @step_method()
    def side_effect(
        self, side_effect: "AnonymousTraversal|Callable[[Traverser], None]"
    ) -> "Traversal":
        from .steps.base_steps import SideEffectStep

        self._add_step(SideEffectStep(self, side_effect))
        return self

    @step_method()
    def property(self, *args) -> "Traversal":
        from .steps.base_steps import SideEffectStep

        if len(args) == 2:
            key, value = args
            if key == Cardinality.label:
                key = "labels"
                value = (
                    set(value)
                    if isinstance(value, (list, tuple, set, dict))
                    else {value}
                )
        elif len(args) == 3:
            cardinality, key, value = args
            match cardinality:
                case Cardinality.set_:
                    value = (
                        set(value)
                        if isinstance(value, (list, tuple, set, dict))
                        else {value}
                    )
                case Cardinality.list_:
                    value = (
                        list(value)
                        if isinstance(value, (list, tuple, set, dict))
                        else [value]
                    )
                case Cardinality.map_ | Cardinality.dict_:
                    pass  # we keep the value as a value
                case _:
                    raise ValueError(
                        "Invalid cardinality for property, expected `set`, `list`, `map` or `dict`"
                    )
        else:
            raise ValueError(
                "Invalid number of arguments for `property`, expected signature (cardinality, key, value) or (key, value)"
            )
        if isinstance(key, (tuple, list)):
            indexer = tu.get_dict_indexer(key[:-1])
            key = key[-1]
        else:
            indexer = lambda x: x

        def effect(t: "Traverser"):
            indexer(self._get_element(t))[key] = value

        self._add_step(SideEffectStep(self, side_effect=effect))
        return self

    ## ===== IO =====
    @step_method(not_anonymous=True)
    def io(self, file_path: str, read: bool = None, write: bool = None) -> "Traversal":
        from .steps.io_step import IO

        if read is not None and write is not None:
            if not read ^ write:
                raise QueryError("read and write cannot be both true or both false")
        self._add_step(IO(self, file_path, read=read, write=write))
        return self

    ## ===== MODULATION STEPS ======
    @step_method()
    def option(self, branchKey, option_trav: "AnonymousTraversal") -> "Traversal":
        from .steps.branch_steps import Branch

        branchStep = self.query_steps[len(self.query_steps) - 1]
        if type(branchStep) is Branch:
            if branchKey is not None:
                branchStep.add_option(branchKey, option_trav)
            else:
                branchStep.set_default(option_trav)
            return self
        else:
            raise QueryError("Option can only be used after Branch step")

    @step_method()
    def until(self, cond: "AnonymousTraversal"):
        from .steps.branch_steps import Repeat

        prevstep = self.query_steps[-1]
        if isinstance(prevstep, Repeat):
            if prevstep.until is None and prevstep.times is None:
                prevstep.until = cond
                prevstep.until_do = False
            else:
                raise QueryError(
                    "Provided `until` to repeat when `times` or `until` was already set."
                )
        else:
            from .steps.modulation_steps import Temp

            self._add_step(Temp(self, type="until", cond=cond))
        return self

    @step_method()
    def times(self, reps: int):
        from .steps.branch_steps import Repeat

        prevstep = self.query_steps[-1]
        if isinstance(prevstep, Repeat):
            if prevstep.times is None and prevstep.until is None:
                prevstep.times = reps
            else:
                raise QueryError(
                    "Provided `times` to repeat when `times` or `until` was already set."
                )
        else:
            raise QueryError(
                f"`times` modulation is not supported by step {prevstep.print_query()}"
            )
        return self

    @step_method()
    def emit(self, filter: "AnonymousTraversal|None" = None):
        from .steps.branch_steps import Repeat

        prevstep = self.query_steps[-1]
        if isinstance(prevstep, Repeat):
            if prevstep.emit is None:
                prevstep.emit = filter or True
                prevstep.emit_before = False
            else:
                raise QueryError(
                    "Provided `emit` to repeat when `emit` was already set."
                )
        else:
            from .steps.modulation_steps import Temp

            self._add_step(Temp(self, type="emit", filter=filter or True))
        return self

    @step_method()
    def as_(self, name: str) -> "Traversal":
        from .steps.modulation_steps import As

        self._add_step(As(self, name))
        return self

    @step_method()
    def by(self, key: str | List[str] | "AnonymousTraversal", *args) -> "Traversal":
        prev_step = self.query_steps[-1]
        if prev_step.supports_by:
            if isinstance(key, AnonymousTraversal):
                if not prev_step.supports_anonymous_by:
                    raise QueryError(
                        f"Step `{prev_step.print_query()}` does not support anonymous traversals as by-modulations."
                    )
            elif type(key) is not str:
                if (
                    isinstance(key, EnumOrder)
                    and prev_step.__class__.__name__ == "Order"
                ):
                    pass
                else:
                    raise QueryError("Invalid key type for by-modulation")

            if prev_step.supports_multiple_by:
                prev_step.by.append(key)
            elif prev_step.by is None:
                prev_step.by = key if len(args) == 0 else (key, *args)
            else:
                raise QueryError(
                    f"Step `{prev_step.print_query()}` does not support multiple by-modulations."
                )
        else:
            raise QueryError(
                f"Step `{prev_step.print_query()}` does not support by-modulation."
            )
        return self

    @step_method()
    def from_(self, src: str) -> "Traversal":
        prev_step = self.query_steps[-1]
        if prev_step.supports_fromto:
            if type(src) is not str:
                raise QueryError(
                    f"Invalid source type `{type(src)}` for from-modulation: str needed!"
                )
            if prev_step.from_ is None:
                prev_step.from_ = src
            else:
                raise QueryError(
                    f"Step `{prev_step.print_query()}` does not support multiple from-modulations."
                )
        else:
            raise QueryError(
                f"Step `{prev_step.print_query()}` does not support from-modulation."
            )
        return self

    @step_method()
    def to_(self, dest: str) -> "Traversal":
        prev_step = self.query_steps[-1]
        if prev_step.supports_fromto:
            if type(dest) is not str:
                raise QueryError("Invalid source type for to-modulation: str needed!")
            if prev_step.to_ is None:
                prev_step.to_ = dest
            else:
                raise QueryError(
                    f"Step `{prev_step.print_query()}` does not support multiple to-modulations."
                )
        else:
            raise QueryError(
                f"Step `{prev_step.print_query()}` does not support to-modulation."
            )
        return self

    @step_method()
    def with_(self, *args) -> "Traversal":
        prev_step = self.query_steps[-1]
        if prev_step.flags & Step.SUPPORTS_WITH:
            if prev_step.with_ is None:
                prev_step.with_ = args
            else:
                raise QueryError(
                    f"Step `{prev_step.print_query()}` does not support multiple with-modulations."
                )
        else:
            raise QueryError(
                f"Step `{prev_step.print_query()}` does not support with-modulation."
            )
        return self

    @step_method(not_anonymous=True)
    def read(self) -> "Traversal":
        from .steps.io_step import IO

        prev_step = self.query_steps[-1]
        if isinstance(prev_step, IO):
            prev_step.read = True
        else:
            raise QueryError(f"the read() step can only be used after an IO step.")
        return self

    @step_method(not_anonymous=True)
    def write(self) -> "Traversal":
        from .steps.io_step import IO

        prev_step = self.query_steps[-1]
        if isinstance(prev_step, IO):
            prev_step.write = True
        else:
            raise QueryError(f"the write() step can only be used after an IO step.")
        return self

    ## ===== TERMINAL STEPS ======
    @step_method(not_anonymous=True)
    def to_list(
        self, by: List[str] | str = None, include_data: bool = False
    ) -> "Traversal":
        # terminal step
        from .steps.terminal_steps import ToList

        self._add_step(ToList(self, by=by, include_data=include_data))
        return self

    @step_method(not_anonymous=True)
    def as_path(self, by: List[str] | str = None) -> "Traversal":
        # terminal step
        from .steps.terminal_steps import AsPath

        self._add_step(AsPath(self, by=by))
        return self

    @step_method(not_anonymous=True)
    def has_next(self) -> "Traversal":
        from .steps.terminal_steps import HasNext

        self._add_step(HasNext(self))
        return self

    @step_method(not_anonymous=True)
    def next(self, n: int = 1) -> "Traversal":
        from .steps.terminal_steps import Next

        self._add_step(Next(self, amount=n))
        return self

    @step_method(not_anonymous=True)
    def iter(
        self, by: str | List[str] = None, include_data: bool = False
    ) -> "Traversal":
        from .steps.terminal_steps import AsGenerator

        self._add_step(AsGenerator(self, by=by, include_data=include_data))
        return self

    @step_method(not_anonymous=True)
    def iterate(self) -> "Traversal":
        from .steps.terminal_steps import Iterate

        self._add_step(Iterate(self))
        return self

    def _optimize_query(self):
        pass

    def _verify_query(self):
        from .steps.modulation_steps import Temp

        for step in self.query_steps:
            if isinstance(step, Temp):
                raise QueryError(f"Remaining modulation step of type `{step['type']}`")
        return True

    def _build(self):
        for step in self.query_steps:
            step.build()

    def run(self) -> Any:
        # first, provide the start step with this traversal
        self.traversers = []
        self.query_steps[0].set_traversal(self)
        self._build()
        self.needs_path = any([s.needs_path for s in self.query_steps])
        if self.optimize:
            self._optimize_query()
        self._verify_query()
        if self.eager:
            try:
                for step in self.query_steps:
                    logger.debug("Running step:" + str(step))
                    self.traversers = step(self.traversers)
                    if (
                        not type(self.traversers) is list and not step.isterminal
                    ):  # terminal steps could produce any type of output
                        self.traversers = list(self.traversers)
            except Exception as e:
                raise GraphTraversalError(
                    f"Something went wrong in step {step.print_query()}"
                )
        else:
            for step in self.query_steps:
                logger.debug("Running step:" + str(step))
                self.traversers = step(self.traversers)
            # TODO: Try to do some fancy error handling
        return self.traversers

    def _get_element(self, traverser: "Traverser", data: bool = False):
        if type(traverser) == Traverser:
            if data:
                return (
                    self.graph.edges[traverser.get]
                    if traverser.is_edge
                    else self.graph.nodes(data=data)[traverser.get]
                )
            return (
                self.graph.edges[traverser.get]
                if traverser.is_edge
                else self.graph.nodes[traverser.get]
            )
        else:
            raise GraphTraversalError(
                "Cannot get element from value or property traverser."
                + " Probably you are performing a step that can only be executed on graph elements on a value or property traverser."
            )

    def _get_element_from_id(self, element_id: str | tuple):
        if isinstance(element_id, tuple):
            node = self.graph.edges[element_id]
        else:
            node = self.graph.nodes[element_id]
        return node

    def print_query(self) -> str:
        text = " -> ".join([x.print_query() for x in self.query_steps])
        return text

    def __str__(self) -> str:
        return self.print_query()


class AnonymousTraversal(Traversal):
    """
    This class implements Anonymous traversals. These are traversals that are not directly bound to a source.
    They are used as subqueries in other traversals, and are not meant to be run on their own.
    As input, they receive a set of traversers from the parent traversal, and they return a set of traversers to the parent traversal.

    Importantly, some steps require information from the source traversal (like the graph's configuration) to be able to construct themselves.
    Therefore, we cannot construct an anonymous traversal at the time it is created, but we need to build it when it is added to a parent traversal.
    This inheritance structure is a bit of a hack to allow for this behavior and will inherently cause some issues with type hinting.

    This behavior is implemented in the following way.
    1. When an anonymous traversal is created, it is empty, and it has a list of step templates.
    2. When a step method is retrieved from the anonymous traversal, this `__getattribute__` call to obtain the method
        is intercepted. Instead of returning the actual step method, which would run immediately and construct the step,
        a deferred step method is returned. This deferred step method stores the step method and its arguments in the step templates.
    3. When the anonymous traversal is build by a parent traversal, the parent traversal constructs the anonymous traversal.
        In the anonymous traversal's build method, it constructs all the steps from the step templates as if the step methods are only called at this point.
    4. The anonymous traversal is now ready to be run.
    """

    def __init__(self, initial_deferred_step: Tuple[Callable[..., Step], Tuple, Dict]):
        self.query_steps: list[Step] = (
            None  # we want to make sure that an error is raised if this is accessed before the anonymous traversal is build.
        )
        self.graph = None
        self.terminated = False
        self._needs_path = False
        self._initial_step = initial_deferred_step
        self._step_templates = []

    # we need this since anonymous traversals need to check this before they're run.
    # this is a very tricky part, since `query_steps` is undefined until the anonymous traversal is build.
    @property
    def needs_path(self):
        return self._needs_path or any((s.needs_path for s in self.query_steps))

    @needs_path.setter
    def needs_path(self, value):
        self._needs_path = value

    def number_of_steps(self, recursive=False):
        if self.query_steps is None:
            if recursive:
                logger.warning(
                    "Anonymous traversal has not been built yet, cannot count steps."
                )
            return 1 + len(self._step_templates)
        else:
            return super().number_of_steps(recursive)

    def run(self):
        raise ValueError("Cannot run anonymous traversals")

    def _build(self, traversal: Traversal):
        # first, set the necessary fields
        self.graph = traversal.graph
        self.eager = traversal.eager
        self.use_mp = traversal.use_mp
        self.verify_query = traversal.verify_query
        self.optimize = traversal.optimize
        # then, build the steps
        self.query_steps = []
        init_step_func, init_args, init_kwargs = self._initial_step
        init_step = init_step_func(*init_args, **init_kwargs)
        init_step.traversal = self
        self.query_steps.append(init_step)
        for step, args, kwargs in self._step_templates:
            step(*args, **kwargs)  # this is the step function
        for step in self.query_steps:
            step.build()
        self.needs_path = any([s.needs_path for s in self.query_steps])
        if traversal.optimize:
            self._optimize_query()
        if self.verify_query:
            self._verify_query()
        if self.query_steps[0].isstart:
            self.query_steps[0].set_traversal(self)
        super()._build()

    def __call__(self, traversers: Iterable["Traverser"]) -> Iterable["Traverser"]:
        # if this traversal is empty, just reflect back the incoming traversers
        if len(self.query_steps) == 0:
            return traversers
        self.traversers = traversers
        if self.eager:
            try:
                for step in self.query_steps:
                    logger.debug("Running step in anonymous traversal:" + str(step))
                    self.traversers = step(self.traversers)
                    if not type(self.traversers) is list:
                        self.traversers = list(self.traversers)
            except Exception as e:
                raise GraphTraversalError(
                    f"Something went wrong in step {step.print_query()}"
                )
        else:
            for step in self.query_steps:
                logger.debug("Running step:" + str(step))
                self.traversers = step(self.traversers)
            # TODO: Try to do some fancy error handling
        return self.traversers

    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if callable(attr) and getattr(attr, "_is_step_method", False):
            if getattr(attr, "_anonymous", True):
                logger.debug("Returning lambda for anonymous step " + attr.__name__)

                def deferred_step(*args, **kwargs):
                    self._step_templates.append((attr, args, kwargs))
                    return self

                return deferred_step
            else:
                raise QueryError(f"Step {name} is not allowed in anonymous traversals")
        return attr

    def print_query(self):
        def format_args(step, args, kwargs):
            step_str = f"{step.__name__}"
            args_str, kwarg_str = None, None
            if len(args) > 0:
                args_str = ", ".join(str(x) for x in args)
            if len(kwargs) > 0:
                kwarg_str = ", ".join(f"{key}={val}" for key, val in kwargs.items())
            if args_str and kwarg_str:
                step_str += f"({args_str}, {kwarg_str})"
            elif args_str:
                step_str += f"({args_str})"
            elif kwarg_str:
                step_str += f"({kwarg_str})"
            return step_str

        texts = [format_args(*self._initial_step)]
        texts += [
            format_args(step, args, kwargs)
            for step, args, kwargs in self._step_templates
        ]
        return " -> ".join(texts)


class MogwaiGraphTraversalSource:
    """
    see https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/process/traversal/dsl/graph/GraphTraversalSource.html

    A GraphTraversalSource is the primary DSL of the Gremlin traversal machine.
    It provides access to all the configurations and steps
    for Turing complete graph computing.
    Any DSL can be constructed based on the methods of both GraphTraversalSource
    and GraphTraversal.

    """

    def __init__(
        self,
        connector: MogwaiGraph,
        eager: bool = False,
        optimize: bool = True,
        use_mp: bool = USE_MULTIPROCESSING,
    ):
        self.connector = connector
        self.traversal_args = dict(
            optimize=optimize, eager=eager, query_verify=True, use_mp=use_mp
        )

    def E(self, *init: Tuple[str] | List[Tuple[str]]) -> "Traversal":
        from .steps.start_steps import E

        if len(init) == 0:
            init = None
        elif len(init) == 1:
            init = init[0]
        return Traversal(self, start=E(self.connector, init), **self.traversal_args)

    def V(self, *init: str) -> "Traversal":
        from .steps.start_steps import V

        if len(init) == 0:
            init = None
        elif len(init) == 1:
            init = init[0]
        return Traversal(self, start=V(self.connector, init), **self.traversal_args)

    def addE(
        self, relation: str, from_: str = None, to_: str = None, **kwargs
    ) -> "Traversal":
        from .steps.start_steps import AddE

        return Traversal(
            self,
            start=AddE(self.connector, relation, from_=from_, to_=to_, **kwargs),
            **self.traversal_args,
        )

    def addV(self, label: str | Set[str], name: str = "", **kwargs) -> "Traversal":
        from .steps.start_steps import AddV

        return Traversal(
            self,
            start=AddV(self.connector, label, name, **kwargs),
            **self.traversal_args,
        )
