from .base_steps import Step
from typing import Iterable
from mogwai.core import Traversal, AnonymousTraversal
from mogwai.core import Traverser
from mogwai.utils.type_utils import TypeUtils as tu
from mogwai.decorators import as_traversal_function

class As(Step):
    def __init__(self, traversal:Traversal, *args:str):
        super().__init__(traversal)
        self.keys = args

    def __call__(self, traversers:Iterable['Traverser']) -> Iterable['Traverser']:
        traversers = tu.ensure_is_list(traversers)
        for key in self.keys:
            for traverser in traversers:
                traverser.save(key)
        return traversers

class Temp(Step):
    def __init__(self, traversal:Traversal, **kwargs):
        super().__init__(traversal)
        self.kwargs = kwargs

    def __call__(self, *args):
        from mogwai.core.exceptions import GraphTraversalError
        raise GraphTraversalError("Encountered a `Temp` step in the traversal. This is definitely a bug.")

@as_traversal_function
def as_(*args):
    return As(None, *args)

@as_traversal_function
def until(cond:AnonymousTraversal):
    return Temp(None,type="until", cond=cond)

@as_traversal_function
def emit(filter:AnonymousTraversal=None):
    return Temp(None, type='emit', filter=filter or True)