from enum import Enum
class Scope(Enum):
    local = 1
    global_ = 2

class Cardinality(Enum):
    label = 'label'
    single = 'single'
    list_ = 'list'
    set_ = 'set'
    map_ = 'map'
    dict_ = 'dict'

class Order(Enum):
    asc = 'asc'
    desc = 'desc'

class IO(Enum):
    reader = 'reader'
    writer = 'writer'
    json = 'json'
    graphson = 'graphson'
    graphson_wrapped = 'graphson_wrapped'
    graphml = 'graphml'
    rdf = 'rdf'