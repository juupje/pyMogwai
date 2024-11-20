import logging
from typing import TYPE_CHECKING, Any, Callable, Iterable, List, Set, Tuple

from mogwai.config import DEFAULT_ITERATION_DEPTH, USE_MULTIPROCESSING
from mogwai.core.exceptions import GraphTraversalError
from mogwai.core.traverser import Traverser
from mogwai.decorators import add_camel_case_methods, with_call_order
from mogwai.utils.type_utils import TypeUtils as tu
from mogwai.core.steps.enums import Scope, Cardinality, Order as EnumOrder

from .exceptions import QueryError
from .mogwaigraph import MogwaiGraph
from .steps.base_steps import Step

logger = logging.getLogger("Mogwai")


@add_camel_case_methods
class Traversal:
    """
    see https://tinkerpop.apache.org/javadocs/3.7.3/core/org/apache/tinkerpop/gremlin/process/traversal/Traversal.html

    A Traversal represents a directed walk over a Graph.
    This is the base sterface for all traversal's,
    where each extending sterface is seen as a domain
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

    def _add_step(self, step: "Step"):
        if self.terminated:
            raise QueryError("Cannot add steps to a terminated traversal.")
        self.query_steps.append(step)
        if step.isterminal:
            self.terminated = True

    ## ===== FILTER STEPS ======
    def filter_(self, condition: "AnonymousTraversal") -> "Traversal":
        from .steps.filter_steps import Filter

        self._add_step(Filter(self, condition))
        return self

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

    def has_not(self, key: str):
        from .steps.filter_steps import HasNot

        self._add_step(HasNot(self, key))
        return self

    def has_key(self, *keys: str):
        from .steps.filter_steps import HasKey

        self._add_step(HasKey(self, *keys))
        return self

    def has_value(self, *values: Any) -> "Traversal":
        from .steps.filter_steps import HasValue

        self._add_step(HasValue(self, *values))
        return self

    def has_id(self, *ids: str | tuple) -> "Traversal":
        from .steps.filter_steps import HasId

        self._add_step(HasId(self, *ids))
        return self

    def has_name(self, *name: str) -> "Traversal":
        if len(name) == 0:
            raise QueryError("No name provided for `has_name`")
        elif len(name) == 1:
            return self.has("name", name[0])
        elif len(name) > 1:
            from .steps.filter_steps import HasWithin

            self._add_step(HasWithin(self, "name", name))
            return self

    def has_label(self, label: str | Set[str]) -> "Traversal":
        if isinstance(label, set):
            from .steps.filter_steps import ContainsAll

            self._add_step(ContainsAll(self, "labels", label))
        else:
            from .steps.filter_steps import Contains

            self._add_step(Contains(self, "labels", label))
        return self

    def is_(self, condition: Any) -> "Traversal":
        from .steps.filter_steps import Is

        self._add_step(Is(self, condition))
        return self

    def contains(self, key: str | List[str], value: Any) -> "Traversal":
        if isinstance(value, list):
            from .steps.filter_steps import ContainsAll

            self._add_step(ContainsAll(self, key, value))
        else:
            from .steps.filter_steps import Contains

            self._add_step(Contains(self, key, value))
        return self

    def within(self, key: str | List[str], options: List[Any]) -> "Traversal":
        from .steps.filter_steps import Within

        self._add_step(Within(self, key, options))
        return self

    def simple_path(self, by: str | List[str] = None) -> "Traversal":
        from .steps.filter_steps import SimplePath

        self._add_step(SimplePath(self, by=by))
        return self

    def limit(self, n: int) -> "Traversal":
        from .steps.filter_steps import Range

        self._add_step(Range(self, 0, n))
        return self

    def range(self, start: int, end: int) -> "Traversal":
        from .steps.filter_steps import Range

        self._add_step(Range(self, start, end))
        return self

    def skip(self, n: int) -> "Traversal":
        from .steps.filter_steps import Range

        self._add_step(Range(self, n, -1))
        return self

    def dedup(self, by: str | List[str] = None) -> "Traversal":
        from .steps.filter_steps import Dedup

        self._add_step(Dedup(self, by=by))
        return self

    def not_(self, condition: "AnonymousTraversal") -> "Traversal":
        from .steps.filter_steps import Not

        self._add_step(Not(self, condition))
        return self

    def and_(self, A: "AnonymousTraversal", B: "AnonymousTraversal") -> "Traversal":
        from .steps.filter_steps import And

        self._add_step(And(self, A, B))
        return self

    def or_(self, A: "AnonymousTraversal", B: "AnonymousTraversal") -> "Traversal":
        from .steps.filter_steps import Or

        self._add_step(Or(self, A, B))
        return self

    ## ===== MAP STEPS ======
    def identity(self) -> "Traversal":  # required for math reasons
        return self

    def id_(self) -> 'Traversal':
        from .steps.map_steps import Id
        self._add_step(Id(self))
        return self

    # Important: `value` extract values from *Property's*
    # `values` extracts values from *elements*!
    # So, .properties(key).value() is the same as .values(key)
    def value(self) -> "Traversal":
        from .steps.map_steps import Value

        self._add_step(Value(self))
        return self

    def key(self) -> "Traversal":
        from .steps.map_steps import Key

        self._add_step(Key(self))
        return self

    def values(self, *keys: str | List[str]) -> "Traversal":
        from .steps.map_steps import Values

        self._add_step(Values(self, *keys))
        return self

    def name(self) -> "Traversal":
        return self.values("name")

    def label(self) -> "Traversal":
        return self.values("labels")

    def properties(self, *keys: str | List[str]) -> "Traversal":
        from .steps.map_steps import Properties

        self._add_step(Properties(self, *keys))
        return self

    def select(self, *args: str, by: str = None) -> "Traversal":
        from .steps.map_steps import Select

        self._add_step(
            Select(self, keys=args[0] if len(args) == 1 else list(args), by=by)
        )
        return self

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

    def count(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Count

        self._add_step(Count(self, scope))
        return self

    def path(self, by: str | List[str] = None) -> "Traversal":
        from .steps.map_steps import Path

        self._add_step(Path(self, by=by))
        return self

    def max_(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Max

        self._add_step(Max(self, scope))
        return self

    def min_(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Min

        self._add_step(Min(self, scope))
        return self

    def sum_(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Aggregate

        self._add_step(Aggregate(self, "sum", scope))
        return self

    def mean(self, scope: Scope = Scope.global_) -> "Traversal":
        from .steps.map_steps import Aggregate

        self._add_step(Aggregate(self, "mean", scope))
        return self

    def element_map(self, *keys: str) -> "Traversal":
        from .steps.map_steps import ElementMap

        if len(keys) == 1:
            keys = keys[0]
        elif len(keys) == 0:
            keys = None
        self._add_step(ElementMap(self, keys))
        return self

    ## ===== FLATMAP STEPS ======
    def out(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import Out

        self._add_step(Out(self, direction))
        return self

    def outE(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import OutE

        self._add_step(OutE(self, direction))
        return self

    def outV(self) -> "Traversal":
        from .steps.flatmap_steps import OutV

        self._add_step(OutV(self))
        return self

    def in_(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import In

        self._add_step(In(self, direction))
        return self

    def inE(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import InE

        self._add_step(InE(self, direction))
        return self

    def inV(self) -> "Traversal":
        from .steps.flatmap_steps import InV

        self._add_step(InV(self))
        return self

    def both(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import Both

        self._add_step(Both(self, direction))
        return self

    def bothE(self, direction: str = None) -> "Traversal":
        from .steps.flatmap_steps import BothE

        self._add_step(BothE(self, direction))
        return self

    def bothV(self) -> "Traversal":
        from .steps.flatmap_steps import BothV

        self._add_step(BothV(self))
        return self

    ## ===== BRANCH STEPS =====
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

    def local(self, localTrav: "AnonymousTraversal") -> "Traversal":
        from .steps.branch_steps import Local

        self._add_step(Local(self, localTrav))
        return self

    def branch(self, branchFunc: "AnonymousTraversal") -> "Traversal":
        from .steps.branch_steps import Branch
        from .steps.map_steps import MapStep

        if len(branchFunc.query_steps) == 0 or not isinstance(
            branchFunc.query_steps[-1], MapStep
        ):
            raise TypeError("Branch is only allowed to be given MapSteps")
        self._add_step(Branch(self, branchFunc))
        return self

    def union(self, *traversals: "AnonymousTraversal") -> "Traversal":
        from .steps.branch_steps import Union

        self._add_step(Union(self, *traversals))
        return self

    ## ===== MODULATION STEPS ======
    def option(self, branchKey, OptionStep: "AnonymousTraversal") -> "Traversal":
        from .steps.branch_steps import Branch

        branchStep = self.query_steps[len(self.query_steps) - 1]
        if type(branchStep) is Branch:
            branchStep.flags |= Step.NEEDS_PATH if OptionStep.needs_path else 0
            if branchKey is not None:
                if branchKey not in branchStep.options:
                    branchStep.options[branchKey] = OptionStep
                    return self
                else:
                    raise QueryError(
                        "Duplicate key " + str(branchKey) + ", please use distinct keys"
                    )
            else:
                if branchStep.defaultStep is None:
                    branchStep.defaultStep = OptionStep
                    return self
                else:
                    raise QueryError(
                        "Provided two default (None) options. This is not allowed"
                    )
        else:
            raise QueryError("Options can only be used after Branch()")

    def until(self, cond: "AnonymousTraversal"):
        from .steps.branch_steps import Repeat

        prevstep = self.query_steps[-1]
        if isinstance(prevstep, Repeat):
            if prevstep.until is None and prevstep.times is None:
                prevstep.until = cond
                prevstep.until_do = False
                if cond.needs_path:
                    prevstep.flags |= Repeat.NEEDS_PATH
            else:
                raise QueryError(
                    "Provided `until` to repeat when `times` or `until` was already set."
                )
        else:
            from .steps.modulation_steps import Temp

            self._add_step(Temp(self, type="until", cond=cond))
        return self

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

    def emit(self, filter: "AnonymousTraversal|None" = None):
        from .steps.branch_steps import Repeat

        prevstep = self.query_steps[-1]
        if isinstance(prevstep, Repeat):
            if prevstep.emit is None:
                prevstep.emit = filter or True
                prevstep.emit_before = False
                if filter and filter.needs_path:
                    prevstep.flags |= Repeat.NEEDS_PATH
            else:
                raise QueryError(
                    "Provided `emit` to repeat when `emit` was already set."
                )
        else:
            from .steps.modulation_steps import Temp

            self._add_step(Temp(self, type="emit", filter=filter or True))
        return self

    def as_(self, name: str) -> "Traversal":
        from .steps.modulation_steps import As

        self._add_step(As(self, name))
        return self


    def by(self, key: str | List[str] | "AnonymousTraversal", *args) -> "Traversal":
        prev_step = self.query_steps[-1]
        if prev_step.supports_by:
            if isinstance(key, AnonymousTraversal):
                if not prev_step.supports_anonymous_by:
                    raise QueryError(
                        f"Step `{prev_step.print_query()}` does not support anonymous traversals as by-modulations."
                    )
            elif type(key) is not str:
                if isinstance(key, EnumOrder) and prev_step.__class__.__name__=="Order":
                    pass
                else:
                    raise QueryError("Invalid key type for by-modulation")

            if prev_step.supports_multiple_by:
                prev_step.by.append(key)
            elif prev_step.by is None:
                prev_step.by = key if len(args)==0 else (key, *args)
            else:
                raise QueryError(
                    f"Step `{prev_step.print_query()}` does not support multiple by-modulations."
                )
        else:
            raise QueryError(
                f"Step `{prev_step.print_query()}` does not support by-modulation."
            )
        return self

    def from_(self, src: str) -> "Traversal":
        prev_step = self.query_steps[-1]
        if prev_step.supports_fromto:
            if type(src) is not str:
                raise QueryError(f"Invalid source type `{type(src)}` for from-modulation: str needed!")
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

    ## ===== SIDE EFFECT STEPS ======
    def side_effect(
        self, side_effect: "AnonymousTraversal|Callable[[Traverser], None]"
    ) -> "Traversal":
        from .steps.base_steps import SideEffectStep

        self._add_step(SideEffectStep(self, side_effect))
        return self

    def property(self, *args) -> 'Traversal':
        from .steps.base_steps import SideEffectStep
        if len(args)==2:
            key, value = args
            if key==Cardinality.label:
                key = "labels"
                value = set(value) if isinstance(value, (list,tuple, set, dict)) else {value}
        elif len(args)==3:
            cardinality, key, value = args
            match cardinality:
                case Cardinality.set_: value = set(value) if isinstance(value, (list,tuple, set, dict)) else {value}
                case Cardinality.list_: value = list(value) if isinstance(value, (list,tuple, set, dict)) else [value]
                case Cardinality.map_ | Cardinality.dict_: pass #we keep the value as a value
                case _:
                    raise ValueError("Invalid cardinality for property, expected `set`, `list`, `map` or `dict`")
        else:
            raise ValueError("Invalid number of arguments for `property`, expected signature (cardinality, key, value) or (key, value)")
        if isinstance(key, (tuple, list)):
            indexer = tu.get_dict_indexer(key[:-1])
            key = key[-1]
        else:
            indexer = lambda x: x

        def effect(t: "Traverser"):
            indexer(self._get_element(t))[key] = value

        self._add_step(SideEffectStep(self, side_effect=effect))
        return self

    ## ===== TERMINAL STEPS ======
    def to_list(
        self, by: List[str] | str = None, include_data: bool = False
    ) -> "Traversal":
        # terminal step
        from .steps.terminal_steps import ToList

        self._add_step(ToList(self, by=by, include_data=include_data))
        return self

    def as_path(self, by: List[str] | str = None) -> "Traversal":
        # terminal step
        from .steps.terminal_steps import AsPath

        self._add_step(AsPath(self, by=by))
        return self

    def has_next(self) -> "Traversal":
        from .steps.terminal_steps import HasNext

        self._add_step(HasNext(self))
        return self

    def next(self, n:int=1) -> 'Traversal':
        from .steps.terminal_steps import Next
        self._add_step(Next(self, amount=n))
        return self

    def iter(
        self, by: str | List[str] = None, include_data: bool = False
    ) -> "Traversal":
        from .steps.terminal_steps import AsGenerator

        self._add_step(AsGenerator(self, by=by, include_data=include_data))
        return self

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
    specialized Traversal
    """

    def __init__(self, start: "Step" = None):
        self.query_steps = [start] if start else []
        self.graph = None
        self.terminated = False
        self._needs_path = False

    # we need this since anonymous traversals need to check this before they're run.
    @property
    def needs_path(self):
        return self._needs_path or any((s.needs_path for s in self.query_steps))

    @needs_path.setter
    def needs_path(self, value):
        self._needs_path = value

    def run(self):
        raise ValueError("Cannot run anonymous traversals")

    def _build(self, traversal: Traversal):
        # first, set the necessary fields
        self.graph = traversal.graph
        self.eager = traversal.eager
        self.use_mp = traversal.use_mp
        self.verify_query = traversal.verify_query
        self.needs_path = any([s.needs_path for s in self.query_steps])
        self.optimize = traversal.optimize
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
                    logger.debug("Running step:" + str(step))
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

    def E(self, *init:Tuple[str]|List[Tuple[str]]) -> 'Traversal':
        from .steps.start_steps import E
        if len(init) == 0: init = None
        elif len(init) == 1: init = init[0]
        return Traversal(self, start=E(self.connector, init), **self.traversal_args)

    def V(self, *init:str) -> 'Traversal':
        from .steps.start_steps import V
        if len(init) == 0: init = None
        elif len(init) == 1: init = init[0]
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
