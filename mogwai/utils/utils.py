from typing import List, Any, Set, Generator

def get_dict_indexer(keys:List[str]|str, default:Any=None):
    if isinstance(keys, (list,tuple)):
        def indexer(x):
            try:
                for key in keys:
                    x = x[key]
                return x
            except:
                return default
        return indexer
    else:
        return lambda x: x.get(keys, default)
    
def get_set_type(s:Set):
    if len(s)==0: return None
    sample = next(iter(s))
    return type(sample)

def get_set_type_all(s:Set):
    if len(s)==0: return None
    sample = next(iter(s))
    t = type(sample)
    for element in s:
        if type(element) is not t:
            return False
    return t

def get_list_type(l:List):
    if(len(l)==0): return None
    return type(l[0])

def get_list_type_all(l:List):
    if len(l)==0: return None
    dtype = type(l[0])
    if all((type(x) is dtype for x in l)):
        return dtype
    return False


def ensure_is_set(s:Set|Generator|List):
    return s if isinstance(s, Set) else set(s)

def ensure_is_list(s:Set|Generator|List):
    return s if isinstance(s, List) else list(s)