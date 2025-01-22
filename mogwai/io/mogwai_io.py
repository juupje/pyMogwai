from abc import ABC, abstractmethod
from mogwai.core import MogwaiGraph, MogwaiGraphConfig
from mogwai.core.exceptions import IOError
import builtins
import datetime as dt
from typing import Any
import logging

logger = logging.getLogger("MogwaiIO")

class IOBackend(ABC):
    @abstractmethod
    def read(self, file_path: str, config:MogwaiGraphConfig=None):
        pass

    @abstractmethod
    def write(self, file_path: str, graph:MogwaiGraph):
        pass

class GraphSON(IOBackend):
    """
    GraphSON is a JSON-based file format for representing graphs.
    https://tinkerpop.apache.org/docs/3.7.2/dev/io/#graphson
    """

    def add_node(self, node:dict, graph:MogwaiGraph):
        pass

    def read(self, file_path:str, config:MogwaiGraphConfig=None):
        import json
        config = config or MogwaiGraphConfig()
        g = MogwaiGraph(config=config)
        try:
            logger.info("Reading GraphSON file (assuming wrapped in 'vertices' key)")
            with open(file_path, 'r') as f:
                data = json.load(f)
                if "vertices" in data:
                    for v in data["vertices"]:
                        self.add_node(v, g)
                else:
                    raise IOError(f"Invalid GraphSON file {file_path}: missing 'vertices' key")
        except Exception:
            logger.info("Nope, it's not wrapped in 'vertices' key")
            logger.info("Reading GraphSON file line by line")
            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        data = json.loads(line)
                        self.add_node(data, g)
            except Exception as e:
                logger.error(f"Error reading GraphSON file {file_path}: {e}", exc_info=True)
                raise IOError(f"Error reading GraphSON file {file_path}: {e}")

    def wrap_type(self, val, dtype=None):
        if dtype is None:
            dtype = type(val)
        match dtype:
            case builtins.int:
                if val > 2**31:
                    return {"@type": "g:Int64", "@value": int(val)}
                return {"@type": "g:Int32", "@value": int(val)}
            case builtins.float:
                return {"@type": "g:Double", "@value": float(val)}
            case builtins.bool | builtins.str:
                return val
            case dt.datetime:
                return {"@type": "g:Timestamp", "@value": int(val.timestamp())}
            case dt.date:
                return {"@type": "g:Timestamp", "@value": int(dt.datetime(val.year, val.month, val.day).timestamp())}
            case builtins.list | builtins.tuple:
                return {"@type": "g:List", "@value": [self.wrap_type(v) for v in val]}
            case builtins.set:
                return {"@type": "g:Set", "@value": [self.wrap_type(v) for v in val]}
            case builtins.dict:
                d = []
                for k, v in val.items():
                    d.append(self.wrap_type(k))
                    d.append(self.wrap_type(v))
                return {"@type": "g:Map", "@value": d}
            case _:
                try:
                    import numpy as np
                    if isinstance(val, (np.int16, np.int32)):
                        return {"@type": "g:Int32", "@value": int(val)}
                    if isinstance(val, np.int64):
                        return {"@type": "g:Int64", "@value": int(val)}
                    if isinstance(val, np.float32):
                        return {"@type": "g:Float", "@value": float(val)}
                    if isinstance(val, np.float64):
                        return {"@type": "g:Double", "@value": float(val)}
                    if isinstance(val, np.bool):
                        return bool(val)
                except:
                    pass
                raise ValueError(f"Unsupported type {dtype} for value {str(val)}")
    
    def unwrap_type(self, val:dict|Any):
        if isinstance(val, (int, float, bool, str)):
            return val
        if isinstance(val, dict):
            dtype = val.get("@type")
            val = val.get("@value")        
        match dtype:
            case "g:Int32" | "g:Int64":
                return int(val)
            case "g:Float" | "g:Double":
                return float(val)
            case "g:Date":
                return dt.date.fromtimestamp(val)
            case "g:Timestamp":
                return dt.datetime.fromtimestamp(val)
            case "g:List":
                return [self.unwrap_type(v) for v in val]
            case "g:Set":
                return {self.unwrap_type(v) for v in val}
            case builtins.dict:
                d = []
                for k, v in val.items():
                    d.append(self.wrap_type(k))
                    d.append(self.wrap_type(v))
                return {"@type": "g:Map", "@value": d}
            case _:
                raise ValueError(f"Unsupported type {dtype} for value {str(val)}")

    def create_list(self, graph):
        l = []
        for node in graph.nodes:
            n = {"id": node.id, "label": node.label}
            for k, v in node.properties.items():
                n[k] = v
            l.append(n)
        return l
    
    def write(self, file_path:str, graph:MogwaiGraph):
        import json
        l = self.create_list(graph)
        with open(file_path, 'w') as f:
            for node in l:
                f.write(json.dumps(node) + "\n")

class GraphSONWrapped(GraphSON):
    def write(self, file_path:str, graph:MogwaiGraph):
        import json
        with open(file_path, 'w') as f:
            data = {"vertices": self.create_list(graph)}
            json.dump(data, f)

class JSON(IOBackend):
    # read json as tree
    def read(self, file_path:str, config:MogwaiGraphConfig=None):
        import json
        config = config or MogwaiGraphConfig()
        g = MogwaiGraph(config=config)

        def add_subnodes(root_id, data:dict|list):
            if isinstance(data, dict):
                for k, v in data.items():
                    if isinstance(v, dict):
                        node_id = g.add_labeled_node(config.default_node_label, k)
                        g.add_edge(root_id, node_id, config.default_edge_label)
                        add_subnodes(node_id, v)
                    elif isinstance(v, list):
                        node_id = g.add_labeled_node(config.default_node_label, k, properties={"type": "list", "length": len(v)})
                        g.add_edge(root_id, node_id, config.default_edge_label)
                        add_subnodes(node_id, v)
                    else:
                        node_id = g.add_labeled_node(config.default_node_label, k, properties={"type": str(type(v)), "value": v})
            elif isinstance(data, list):
                for i, v in enumerate(data):
                    if isinstance(v, dict):
                        node_id = g.add_labeled_node("list_item", f"item_{i}")
                        g.add_edge(root_id, node_id, "has_item")
                        add_subnodes(node_id, v)
                    elif isinstance(v, list):
                        node_id = g.add_labeled_node(config.default_node_label, f"item_{i}", properties={"type": "list", "length": len(v)})
                        g.add_edge(root_id, node_id, "has_item")
                        add_subnodes(node_id, v)
                    else:
                        node_id = g.add_labeled_node(config.default_node_label, f"item_{i}", properties={"type": str(type(v)), "value": v})
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                root_id = g.add_labeled_node(config.default_node_label, "root")
                add_subnodes(root_id, data)
        except Exception as e:
            logger.error(f"Error reading JSON file {file_path}: {e}", exc_info=True)
            logger.info("Trying to read json line by line")
            try:
                with open(file_path, 'r') as f:
                    root_id = g.add_labeled_node(config.default_node_label, "root")
                    for line in f.readline():
                        add_subnodes(root_id, json.loads(line))
            except Exception as e2:
                logging.error(f"Failed to read JSON line by line: {e2}", exc_info=True)
                raise IOError(f"Error reading JSON file {file_path}: {e}")

    def write(self, file_path:str, graph:MogwaiGraph):
        raise NotImplementedError("Writing JSON is not supported, try GraphSON instead.")

class GraphML(IOBackend):

    pass

class RDF(IOBackend):
    pass