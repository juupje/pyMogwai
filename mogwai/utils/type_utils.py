from typing import List, Any, Set, Generator

class TypeUtils:
    """
    utility functions to handle types
    """
    @classmethod
    def get_dict_indexer(cls, keys: List[str]|str, default: Any=None):
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

    @classmethod
    def get_set_type(cls, s: Set):
        if len(s)==0: return None
        sample = next(iter(s))
        return type(sample)

    @classmethod
    def get_set_type_all(cls, s: Set):
        if len(s)==0: return None
        sample = next(iter(s))
        t = type(sample)
        for element in s:
            if type(element) is not t:
                return False
        return t

    @classmethod
    def get_list_type(cls, l: List):
        if(len(l)==0): return None
        return type(l[0])

    @classmethod
    def get_list_type_all(cls, l: List):
        if len(l)==0: return None
        dtype = type(l[0])
        if all((type(x) is dtype for x in l)):
            return dtype
        return False

    @classmethod
    def ensure_is_set(cls, s: Set|Generator|List):
        return s if isinstance(s, Set) else set(s)

    @classmethod
    def ensure_is_list(cls, s: Set|Generator|List):
        return s if isinstance(s, List) else list(s)