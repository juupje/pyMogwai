import networkx as nx
from mogwai.core.mogwaigraph import MogwaiGraph
from mogwai.core.exceptions import MogwaiGraphError
from typing import Callable
from itertools import count
import logging
logger = logging.getLogger("Mogwai")

def graphml_to_mogwaigraph(file:str, node_label_key:str|Callable[[dict],str], node_name_key:str|Callable[[dict],str],
                           edge_label_key:str|Callable[[dict],str]=None,
                           default_node_label:str='Na', default_edge_label:str='Na', default_node_name:str='Na',
                           include_id:bool|str=False)->MogwaiGraph:
    gml = nx.read_graphml(file)
    if not gml.is_directed():
        raise MogwaiGraphError("Can not import undirected graphml graph")
    g = MogwaiGraph()
    edge_label_key = edge_label_key or node_label_key
    if(include_id==True): include_id='id' #use 'id' as the default key
    #Note: these function change the node data!
    # However, this is not a problem, since `gml` is discarded anyway.
    missing_label_count = count()
    if type(node_label_key) is str:
        def node_label_func(data:dict):
            if node_label_key in data: return data.pop(node_label_key)
            else:
                next(missing_label_count)
                return default_node_label
    else:
        node_label_func = node_label_key
    missing_name_count = count()
    if type(node_name_key) is str:
        def node_name_func(data:dict):
            if node_name_key in data: return data.pop(node_name_key)
            else:
                next(missing_name_count)
                return default_node_name
    else:
        node_name_func = node_name_key
    
    missing_edge_count = count()
    if type(edge_label_key) is str:
        def edge_label_func(data:dict):
            if edge_label_key in data: return data.pop(edge_label_key)
            else:
                next(missing_edge_count)
                return default_edge_label
    else:
        edge_label_func = edge_label_key
    
    node_to_id_map = {}
    for node, data in gml.nodes(data=True):
        if(include_id):
            data[include_id] = node
        assigned_id = g.add_labeled_node(label=node_label_func(data), name=node_name_func(data), properties=data)
        node_to_id_map[node] = assigned_id
    for node1, node2, data in gml.edges(data=True):
        g.add_labeled_edge(srcId=node_to_id_map[node1] , destId=node_to_id_map[node2], edgeLabel=edge_label_func(data), properties=data)
    
    missing_edge_count = next(missing_edge_count)
    missing_name_count = next(missing_name_count)
    missing_label_count = next(missing_label_count)
    if(missing_edge_count>0): logger.warning(f"Encountered {missing_edge_count} edges without label")
    if(missing_name_count>0): logger.warning(f"Encountered {missing_name_count} nodes without name")
    if(missing_label_count>0): logger.warning(f"Encountered {missing_label_count} nodes without label")
    return g