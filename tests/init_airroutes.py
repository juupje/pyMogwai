import logging

logger = logging.getLogger("Mogwai")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s:%(levelname)s - %(message)s"))
logger.addHandler(ch)
from mogwai.core.steps.statics import *
from mogwai.core.traversal import MogwaiGraphTraversalSource
from mogwai.parser import graphml_to_mogwaigraph

graph = graphml_to_mogwaigraph(
    "tests/documents/air-routes-latest.graphml",
    node_label_key="labelV",
    edge_label_key="labelE",
    node_name_key=lambda x: x.pop("code") if x["type"] == "airport" else x.pop("desc"),
)
graph_small = graphml_to_mogwaigraph(
    "tests/documents/air-routes-small-latest.graphml",
    node_label_key="labelV",
    edge_label_key="labelE",
    node_name_key=lambda x: x.pop("code") if x["type"] == "airport" else x.pop("desc"),
)
g = MogwaiGraphTraversalSource(graph)
g_s = MogwaiGraphTraversalSource(graph_small)
