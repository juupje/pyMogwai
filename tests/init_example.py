import logging

logger = logging.getLogger("Mogwai")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s:%(levelname)s - %(message)s"))
logger.addHandler(ch)
from mogwai.core import traversal as trav
from mogwai.core.mogwaigraph import MogwaiGraph
from mogwai.core.steps.statics import *

graph = MogwaiGraph()
nodes = []
for i in range(5):
    nodes.append(graph.add_labeled_node("node", str(i + 1)))
graph.add_labeled_edge(nodes[0], nodes[1], "knows")
graph.add_labeled_edge(nodes[1], nodes[3], "knows")
graph.add_labeled_edge(nodes[3], nodes[4], "knows")
graph.add_labeled_edge(nodes[1], nodes[2], "knows")
graph.add_labeled_edge(nodes[2], nodes[3], "knows")
g = trav.MogwaiGraphTraversalSource(graph)
