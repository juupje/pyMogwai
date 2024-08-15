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

crew = MogwaiGraph.crew()
g = trav.MogwaiGraphTraversalSource(crew)
