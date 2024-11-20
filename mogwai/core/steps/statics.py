from .branch_steps import branch, repeat, union
from .filter_steps import has, has_not, has_id, has_label, has_name, contains, filter_, simple_path, limit, dedup, and_, or_, not_
from .flatmap_steps import out, outE, outV, in_, inV, both, bothV, bothE
from .map_steps import properties, value, values, key, id_, select, path, count, min_, max_, mean, sum_, element_map
from .modulation_steps import as_, until, emit
from .predicates import *
from .enums import Scope, Cardinality, Order
desc, asc = Order.desc, Order.asc
local, global_ = Scope.local, Scope.global_
single, map_, list_, set_, dict_ = Cardinality.single, Cardinality.map_, Cardinality.list_, Cardinality.set_, Cardinality.dict_

#I'm reasonably sure that this is NOT good practice, but it works, so ¯\_(ツ)_/¯
def add_camel_case_aliases(module_globals):
    """Add camelCase aliases for all snake_case callables in the module's globals."""
    camel_case_aliases = {}
    for name, obj in module_globals.items():
        if callable(obj) and '_' in name:  # Only convert callable objects with underscores
            components = name.split('_')
            camel_case_name = components[0] + ''.join(x.capitalize() for x in components[1:])
            if name.endswith('_'):
                camel_case_name += '_'
            if camel_case_name != name:
                camel_case_aliases[camel_case_name] = obj
    module_globals.update(camel_case_aliases)
add_camel_case_aliases(globals())