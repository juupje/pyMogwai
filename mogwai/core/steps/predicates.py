from typing import Any, Callable, Generator
from numbers import Number
import logging
logger = logging.getLogger("Mogwai")

def eq(value: Any) -> Callable[[Any], bool]:
    return lambda x: x == value

def neq(value: Any) -> Callable[[Any], bool]:   
    return lambda x: x != value

def gte(value: Number) -> Callable[[Number], bool]:
    return lambda x: isinstance(x, Number) and x >= value

def lte(value: Number) -> Callable[[Number], bool]:
    return lambda x: isinstance(x, Number) and x <= value

def gt(value: Number) -> Callable[[Number], bool]:
    return lambda x: isinstance(x, Number) and x > value

def lt(value: Number) -> Callable[[Number], bool]:
    return lambda x: isinstance(x, Number) and x < value

def inside(lower_bound: Number, upper_bound: Number) -> Callable[[Number], bool]:
    return lambda x: isinstance(x, Number) and lower_bound < x < upper_bound

def between(lower_bound: Number, upper_bound: Number) -> Callable[[Number], bool]:
    return lambda x: isinstance(x, Number) and lower_bound <= x <= upper_bound

def outside(lower_bound: Number, upper_bound: Number) -> Callable[[Number], bool]:
    return lambda x: isinstance(x, Number) and (x < lower_bound or x > upper_bound)

def within(*values: Any) -> Callable[[Any], bool]:
    if len(values) == 1:
        if isinstance(values[0], (tuple, list, range)):
            values = values[0]
        elif isinstance(values[0], Generator):
            values = list(values[0])
        else:
            logger.warning("You passed only one argument to `within` that is not a tuple, list, range or generator."+
                           " This will be treated as a single value. If you want to check equality to a single value, use `eq` instead.")
    return lambda x: x in values

def without(*values: Any) -> Callable[[Any], bool]:
    if len(values) == 1:
        if isinstance(values[0], (tuple, list, range)):
            values = values[0]
        elif isinstance(values[0], Generator):
            values = list(values[0])
        else:
            logger.warning("You passed only one argument to `without` that is not a tuple, list, range or generator."+
                           " This will be treated as a single value. If you want to check inequality to a single value, use `neq` instead.")
    return lambda x: x not in values

def starting_with(prefix: str) -> Callable[[str], bool]:
    return lambda x: isinstance(x, str) and x.startswith(prefix)

def not_starting_with(prefix: str) -> Callable[[str], bool]:
    return lambda x: isinstance(x, str) and not x.startswith(prefix)

def ends_with(postfix: str) -> Callable[[str], bool]:
    return lambda x: isinstance(x, str) and x.endswith(postfix)

def not_ends_with(postfix: str) -> Callable[[str], bool]:
    return lambda x: isinstance(x, str) and not x.endswith(postfix)

def containing(substring: str) -> Callable[[str], bool]:
    return lambda x: isinstance(x, str) and substring in x

def not_containing(substring: str) -> Callable[[str], bool]:
    return lambda x: isinstance(x, str) and substring not in x

def regex(pattern: str) -> Callable[[str], bool]:
    import re
    try:
        pattern = re.compile(pattern)
    except re.error:    
        logger.error(f"Invalid regex pattern: {pattern}")
        raise
    return lambda x: isinstance(x, str) and re.match(pattern, x) is not None

def not_regex(pattern: str) -> Callable[[str], bool]:
    import re
    try:
        pattern = re.compile(pattern)
    except re.error:    
        logger.error(f"Invalid regex pattern: {pattern}")
        raise
    return lambda x: isinstance(x, str) and re.match(pattern, x) is None
