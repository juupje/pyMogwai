import re
__pattern = re.compile(r'(?<!^)(?=[A-Z])') #A simple CamelCase detector
import sys
from types import FunctionType
from functools import wraps
from typing import Callable, TYPE_CHECKING
if TYPE_CHECKING:
    from mogwai.core.traversal import AnonymousTraversal

def as_traversal_function(obj) -> Callable[..., 'AnonymousTraversal']:
    from mogwai.core.traversal import AnonymousTraversal
    if type(obj) is FunctionType:
        #obj is a function
        @wraps(obj)
        def wrapper(*args, **kwargs) -> AnonymousTraversal:
            trav = AnonymousTraversal()
            step = obj(*args, **kwargs)
            step.traversal = trav
            trav._add_step(step)
            return trav
        return wrapper

#With a bit of python-magic, we can determine the order of the keyword arguments to a function
# and pass that order the the function as the _order argument.
def with_call_order(func):
    from functools import wraps
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs, _order=list(kwargs.keys()))
    return wrapper