import networkx
from itertools import count
from .exceptions import MogwaiGraphError

class MogwaiGraph (networkx.DiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counter = count(0)

    def add_labeled_node(self,label:set|str, name:str, properties:dict=None):
        #we can only insert a node by hashable value and as names and ids
        #can occur multiple times we use random values
        label = label if isinstance(label, set) else {label}
        nodeID = next(self.counter)
        super().add_node(nodeID, labels=label,name=name, properties=properties or {})
        return nodeID

    def add_labeled_edge(self,srcId:int,destId:int,edgeLabel:str,properties:dict=None):
        if(self.has_node(srcId) and self.has_node(destId)):
            super().add_edge(srcId,destId,labels=edgeLabel, properties=properties or {})
        else:
            raise MogwaiGraphError(f"Node with id {srcId if srcId<0 else destId} is not in the graph.")

    def add_node(self, *args, **kwargs):
        raise MogwaiGraphError("Please use `add_labeled_node` to add nodes to a MogwaiGraph.")

    def add_edge(self, *args, **kwargs):
        raise MogwaiGraphError("Please use `add_edge_node` to add edges to a MogwaiGraph.")

    def _get_nodes_set(self, label:set, name:str):
        n_none = name is None
        if(n_none):
            return [n for n in self.nodes(date=True) if label.issubset(n[1]["labels"])]
        if(not n_none):
            return [n for n in self.nodes(data=True) if label.issubset(n[1]["labels"]) and n[1]["name"]==name]
        return self.nodes

    def get_nodes(self, label:str|set, name:str):
        if type(label) is set: #check if we are looking for multiple labels
            if(len(label)==0): label = None
            else: return self._get_nodes_set(label, name)

        l_none, n_none = label is None, name is None
        if(not l_none and not n_none):
            return [n for n in self.nodes(data=True) if label in n[1]["labels"] and n[1]["name"]==name]
        if(l_none and not n_none):
            return [n for n in self.nodes(data=True) if n[1]["name"]==name]
        if(not l_none and n_none):
            return [n for n in self.nodes(date=True) if label in n[1]["labels"]]
        return self.nodes

    def merge_subgraph(self, other:'MogwaiGraph', srcId:int, targetId:int, edgeLabel:str):
        mapping = {k: next(self.counter) for k in other.nodes}
        relabeled = networkx.relabel_nodes(other, mapping, copy=True)
        self.add_nodes_from(relabeled.nodes(data=True))
        self.add_edges_from(relabeled.edges(data=True))
        self.add_labeled_edge(srcId=srcId, destId=mapping[targetId], edgeLabel=edgeLabel)

    def draw(self, outputfile, include_id:bool=False, prog='dot'):
        A = networkx.nx_agraph.to_agraph(self)  # convert to a graphviz graph
        for n in A.nodes():
            n = A.get_node(n)
            id = int(n.get_name())
            if include_id:
                n.attr["label"] = f"{id:d}:{','.join(self.nodes[id]['labels'])}:{n.attr['name']}"
            else:
                n.attr["label"] = f"{','.join(self.nodes[id]['labels'])}:{n.attr['name']}"
            n.attr["tooltip"] = n.attr["properties"]
            del n.attr["properties"]
        for e in A.edges():
            if(isinstance(e.attr["labels"], (set, tuple,list))):
                e.attr['label'] = ",".join(e.attr["labels"])
            else:
                e.attr['label'] = e.attr['labels']
            del e.attr['labels']
        A.layout(prog=prog)
        A.draw(outputfile)

    @staticmethod
    def modern():
        return get_modern()

    @staticmethod
    def crew():
        return get_crew()

def get_modern() -> MogwaiGraph:
    """
    create the modern graph
    see https://tinkerpop.apache.org/docs/current/tutorials/getting-started/
    """
    g = MogwaiGraph()
    marko = g.add_labeled_node("Person", "marko", {"age": 29})
    vadas = g.add_labeled_node("Person", "vadas", {"age": 27})
    lop = g.add_labeled_node("Software", "lop", {"lang": "java"})
    josh = g.add_labeled_node("Person", "josh", {"age": 32})
    ripple = g.add_labeled_node("Software", "ripple", {"lang": "java"})
    peter = g.add_labeled_node("Person", "peter", {"age": 35})

    g.add_labeled_edge(marko, vadas, "knows", {"weight": 0.5})
    g.add_labeled_edge(marko, josh, "knows", {"weight": 1.0})
    g.add_labeled_edge(marko, lop, "created", {"weight": 0.4})
    g.add_labeled_edge(josh, ripple, "created", {"weight": 1.0})
    g.add_labeled_edge(josh, lop, "created", {"weight": 0.4})
    g.add_labeled_edge(peter, lop, "created", {"weight": 0.2})
    return g

def get_crew() -> MogwaiGraph:
    g = MogwaiGraph()
    def t(startTime:int, endTime:int=None):
        d = dict()
        d["startTime"] = startTime
        if endTime is not None:
            d["endTime"] = endTime
        return d
    marko = g.add_labeled_node("person", "marko", {"location":{"san diego":t(1997,2001), "santa cruz":t(2001,2004), "brussels":t(2004,2005), "santa fe":t(2005)}})
    stephen = g.add_labeled_node("person", "stephen", {"location":{"centreville":t(1990,2000),"dulles":t(2000,2006), "purcellvilee":t(2006)}})
    matthias = g.add_labeled_node("person", "matthias", {"location":{"bremen":t(2004,2007), "baltimore":t(2007,2011), "oakland":t(2011,2014), "seattle":t(2014)}})
    daniel = g.add_labeled_node("person", "daniel", {"location":{"spremberg":t(1982,2005), "kaiserslautern":t(2005,2009), "aachen":t(2009)}})
    gremlin = g.add_labeled_node("software", "gremlin")
    tinkergraph = g.add_labeled_node("software", "tinkergraph")

    g.add_labeled_edge(marko, gremlin, "uses", {"skill":4})
    g.add_labeled_edge(stephen, gremlin, "uses", {"skill":5})
    g.add_labeled_edge(matthias, gremlin, "uses", {"skill":3})
    g.add_labeled_edge(daniel, gremlin, "uses", {"skill":5})
    g.add_labeled_edge(marko, tinkergraph, "uses", {"skill":5})
    g.add_labeled_edge(stephen, tinkergraph, "uses", {"skill":4})
    g.add_labeled_edge(matthias, tinkergraph, "uses", {"skill":3})
    g.add_labeled_edge(daniel, tinkergraph, "uses", {"skill":3})
    g.add_labeled_edge(gremlin, tinkergraph, "traverses")
    g.add_labeled_edge(marko, tinkergraph, "develops", {"since":2010})
    g.add_labeled_edge(stephen, tinkergraph, "develops", {"since":2011})
    g.add_labeled_edge(marko, gremlin, "develops", {"since":2009})
    g.add_labeled_edge(stephen, gremlin, "develops", {"since":2010})
    g.add_labeled_edge(matthias, gremlin, "develops", {"since":2012})
    return g

