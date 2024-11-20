from dataclasses import dataclass
from typing import Any, Hashable, Optional

import networkx

from mogwai.core.hd_index import IndexConfigs, Quad, SPOGIndex

from .exceptions import MogwaiGraphError


@dataclass
class MogwaiGraphConfig:
    """
    configuration of a MogwaiGraph
    """

    name_field: str = "name"
    label_field: str = "labels"
    edge_label_field: str = "labels"
    default_node_label: str = "Node"
    default_edge_label: str = "Edge"
    index_config: str = "off"
    single_label: bool = True


class MogwaiGraph(networkx.DiGraph):
    """
    networkx based directed graph
    see https://networkx.org/documentation/stable/reference/classes/digraph.html
    """

    def __init__(
        self, incoming_graph_data=None, config: MogwaiGraphConfig = None, **attr
    ):
        """Initialize a MogwaiGraph with optional data and configuration.

        Args:
            incoming_graph_data: Graph data in NetworkX compatible format
            config (MogwaiGraphConfig): Configuration for field names and defaults
            **attr: Graph attributes as key=value pairs
        """
        super().__init__(incoming_graph_data, **attr)
        self.counter = 0
        self.config = config or MogwaiGraphConfig()
        # Initialize SPOG index based on config
        index_config = IndexConfigs[self.config.index_config.upper()].get_config()
        self.spog_index = SPOGIndex(index_config)

    def get_next_node_id(self) -> str:
        """
        get the next node_id
        """
        node_id = self.counter
        self.counter += 1
        node_id_str = str(node_id)
        return node_id_str

    def add_to_index(
        self,
        element_type: str,
        subject_id: Hashable,
        label: str,
        name: str,
        properties: dict,
    ):
        """
        Add labels, name, and properties to the SPOG index for a
        given subject and element_type

        Args:
            element_type: (str): node or edge
            subject_id (Hashable): The ID of the subject (node or edge).
            label (str): the label for the subject.
            name (str): Name of the subject.
            properties (dict): Dictionary of additional properties to index.
        """
        # only index if the config calls for it
        if self.config.index_config == "off":
            return
        # Add quads for label with g="label"
        label_quad = Quad(s=subject_id, p="label", o=label, g=f"{element_type}-label")
        self.spog_index.add_quad(label_quad)

        # Add quad for name with g="name"
        name_quad = Quad(s=subject_id, p="name", o=name, g=f"{element_type}-name")
        self.spog_index.add_quad(name_quad)

        # Add quads for each property with g="property"
        for prop_name, prop_value in properties.items():
            if not isinstance(prop_value, Hashable):
                prop_value = str(prop_value)  # Ensure property value is hashable
            property_quad = Quad(
                s=subject_id, p=prop_name, o=prop_value, g=f"{element_type}-property"
            )
            self.spog_index.add_quad(property_quad)

    def add_labeled_node(
        self,
        label: str,
        name: str,
        properties: dict = None,
        node_id: Optional[str] = None,
        **kwargs,
    ) -> Any:
        """
        Add a labeled node to the graph.

        we can only insert a node by hashable value and as names and ids
        might occur multiple times we use incremented node ids if no node_id is provided

        Args:
            label (str): The label for the node.
            name (str): The name of the node.
            properties (dict, optional): Additional properties for the node. Defaults to None.
            node_id (Optional[int], optional): The ID for the node. If not provided, a new ID will be generated. Defaults to None.
            kwargs (): further property values
        Returns:
            Any: The ID of the newly added node - will be an integer if node_id was kept as default None

        Raises:
            MogwaiGraphError: If a node with the provided ID already exists in the graph.
        """
        if node_id is None:
            node_id = self.get_next_node_id()
        properties = properties or {}
        properties.update(kwargs)
        if self.config.name_field in properties:
            raise MogwaiGraphError(
                f"The '{self.config.name_field}' property is reserved for the node name."
            )
        elif self.config.label_field in properties:
            raise MogwaiGraphError(
                f"The '{self.config.label_field}' property is reserved for the node labels."
            )
        node_props = {
            self.config.name_field: name,
            self.config.label_field: label,
            **properties,
        }
        super().add_node(node_id, **node_props)
        # Use add_to_index to add label, name, and properties as quads
        self.add_to_index("node", node_id, label, name, properties)
        return node_id

    def add_labeled_edge(
        self, srcId: int, destId: int, edgeLabel: str, properties: dict = None, **kwargs
    ):
        """
        add a labeled edge
        """
        if self.has_node(srcId) and self.has_node(destId):
            properties = properties or {}
            properties.update(kwargs)
            if self.config.edge_label_field in properties:
                raise MogwaiGraphError(
                    f"The '{self.config.edge_label_field}' property is reserved for the edge label."
                )
            elif self.config.label_field in properties:
                raise MogwaiGraphError(
                    f"The '{self.config.label_field}' property is reserved for the node labels."
                )
            edge_props = {self.config.edge_label_field: edgeLabel, **properties}
            super().add_edge(srcId, destId, **edge_props)
            # Add a quad specifically for the edge connection
            edge_quad = Quad(s=srcId, p=edgeLabel, o=destId, g="edge-link")
            self.spog_index.add_quad(edge_quad)

            # Use add_to_index to add label, name, and properties as quads
            self.add_to_index("edge", srcId, edgeLabel, edgeLabel, properties)
        else:
            raise MogwaiGraphError(
                f"Node with srcId {srcId} or destId {destId} is not in the graph."
            )

    def add_node(self, *args, **kwargs):
        """Add a node with default or explicit labels"""
        if len(args) > 0:
            node_id = args[0]
        else:
            node_id = self.get_next_node_id()

        label = kwargs.pop("labels", self.config.default_node_label)
        name = kwargs.pop("name", str(node_id))
        return self.add_labeled_node(label, name, properties=kwargs, node_id=node_id)

    def add_edge(self, *args, **kwargs):
        """Add an edge with default or explicit label"""
        if len(args) < 2:
            raise MogwaiGraphError("add_edge() requires source and target node ids")
        src, dst = args[0:2]
        label = kwargs.pop(self.config.edge_label_field, self.config.default_edge_label)
        return self.add_labeled_edge(src, dst, label, properties=kwargs)

    def _get_nodes_set(self, label: set, name: str):
        n_none = name is None
        if n_none:
            return [n for n in self.nodes(date=True) if label.issubset(n[1]["labels"])]
        if not n_none:
            return [
                n
                for n in self.nodes(data=True)
                if label.issubset(n[1]["labels"]) and n[1]["name"] == name
            ]
        return self.nodes

    def get_nodes(self, label: str, name: str):
        """
        @FIXME - this is ugly code
        """
        l_none, n_none = label is None, name is None
        if not l_none and not n_none:
            return [
                n
                for n in self.nodes(data=True)
                if label in n[1]["labels"] and n[1]["name"] == name
            ]
        if l_none and not n_none:
            return [n for n in self.nodes(data=True) if n[1]["name"] == name]
        if not l_none and n_none:
            return [n for n in self.nodes(date=True) if label in n[1]["labels"]]
        return self.nodes

    def merge_subgraph(
        self, other: "MogwaiGraph", srcId: int, targetId: int, edgeLabel: str
    ):
        mapping = {k: self.get_next_node_id() for k in other.nodes}
        relabeled = networkx.relabel_nodes(other, mapping, copy=True)
        self.add_nodes_from(relabeled.nodes(data=True))
        self.add_edges_from(relabeled.edges(data=True))
        self.add_labeled_edge(
            srcId=srcId, destId=mapping[targetId], edgeLabel=edgeLabel
        )

    def join(
        self,
        from_label: str,
        to_label: str,
        join_field: str,
        target_key: str,
        edge_label: str,
    ):
        """Joins two node types by field values and creates edges between them."""
        node_lookup = self.spog_index.get_lookup("P", "O")
        if not node_lookup:
            raise ValueError("No SPOG index available")

        field_values = node_lookup.get(join_field)
        if field_values is None:
            raise ValueError(f"Join field {join_field} not found in index")

        target_values = node_lookup.get(target_key)
        if target_values is None:
            raise ValueError(f"Target key {target_key} not found in index")

        os_lookup = self.spog_index.get_lookup("O", "S")
        for source_id in os_lookup.get(from_label):
            source_value = self.nodes[source_id][join_field]
            target_ids = os_lookup.get(source_value)
            for target_id in target_ids:
                if to_label in self.nodes[target_id].get("labels", []):
                    self.add_labeled_edge(source_id, target_id, edge_label)

    def draw(self, outputfile, title: str = "MogwaiGraph", **kwargs):
        """
        Draw the graph using graphviz
        Parameters
        ----------
        outputfile : str
            the file to save the graph to
        title : str, default 'MogwaiGraph'
            the title of the graph
        kwargs : dict
            additional parameters used to configure the drawing style.
            For more details see `MogwaiGraphDrawer`
        """
        MogwaiGraphDrawer(self, title=title, **kwargs).draw(outputfile)

    @classmethod
    def modern(cls, index_config="off") -> "MogwaiGraph":
        """
        create the modern graph
        see https://tinkerpop.apache.org/docs/current/tutorials/getting-started/
        """
        config = MogwaiGraphConfig
        config.index_config = index_config
        g = MogwaiGraph(config=config)
        marko = g.add_labeled_node("Person", name="marko", age=29)
        vadas = g.add_labeled_node("Person", name="vadas", age=27)
        lop = g.add_labeled_node("Software", name="lop", lang="java")
        josh = g.add_labeled_node("Person", name="josh", age=32)
        ripple = g.add_labeled_node("Software", name="ripple", lang="java")
        peter = g.add_labeled_node("Person", name="peter", age=35)

        g.add_labeled_edge(marko, vadas, "knows", weight=0.5)
        g.add_labeled_edge(marko, josh, "knows", weight=1.0)
        g.add_labeled_edge(marko, lop, "created", weight=0.4)
        g.add_labeled_edge(josh, ripple, "created", weight=1.0)
        g.add_labeled_edge(josh, lop, "created", weight=0.4)
        g.add_labeled_edge(peter, lop, "created", weight=0.2)
        return g

    @classmethod
    def crew(cls) -> "MogwaiGraph":
        """
        create the TheCrew example graph
        see TinkerFactory.createTheCrew() in https://tinkerpop.apache.org/docs/current/reference/
        """
        g = MogwaiGraph()

        def t(startTime: int, endTime: int = None):
            d = dict()
            d["startTime"] = startTime
            if endTime is not None:
                d["endTime"] = endTime
            return d

        marko = g.add_labeled_node(
            "Person",
            name="marko",
            location={
                "san diego": t(1997, 2001),
                "santa cruz": t(2001, 2004),
                "brussels": t(2004, 2005),
                "santa fe": t(2005),
            },
        )
        stephen = g.add_labeled_node(
            "Person",
            name="stephen",
            location={
                "centreville": t(1990, 2000),
                "dulles": t(2000, 2006),
                "purcellvilee": t(2006),
            },
        )
        matthias = g.add_labeled_node(
            "Person",
            name="matthias",
            location={
                "bremen": t(2004, 2007),
                "baltimore": t(2007, 2011),
                "oakland": t(2011, 2014),
                "seattle": t(2014),
            },
        )
        daniel = g.add_labeled_node(
            "Person",
            name="daniel",
            location={
                "spremberg": t(1982, 2005),
                "kaiserslautern": t(2005, 2009),
                "aachen": t(2009),
            },
        )
        gremlin = g.add_labeled_node("Software", name="gremlin")
        tinkergraph = g.add_labeled_node("Software", name="tinkergraph")

        g.add_labeled_edge(marko, gremlin, "uses", skill=4)
        g.add_labeled_edge(stephen, gremlin, "uses", skill=5)
        g.add_labeled_edge(matthias, gremlin, "uses", skill=3)
        g.add_labeled_edge(daniel, gremlin, "uses", skill=5)
        g.add_labeled_edge(marko, tinkergraph, "uses", skill=5)
        g.add_labeled_edge(stephen, tinkergraph, "uses", skill=4)
        g.add_labeled_edge(matthias, tinkergraph, "uses", skill=3)
        g.add_labeled_edge(daniel, tinkergraph, "uses", skill=3)
        g.add_labeled_edge(gremlin, tinkergraph, "traverses")
        g.add_labeled_edge(marko, tinkergraph, "develops", since=2010)
        g.add_labeled_edge(stephen, tinkergraph, "develops", since=2011)
        g.add_labeled_edge(marko, gremlin, "develops", since=2009)
        g.add_labeled_edge(stephen, gremlin, "develops", since=2010)
        g.add_labeled_edge(matthias, gremlin, "develops", since=2012)
        return g


class MogwaiGraphDrawer:
    """
    helper class to draw MogwaiGraphs
    """

    def __init__(self, g: MogwaiGraph, title: str, **kwargs):
        """
        Parameters
        ----------
        g : MogwaiGraph
            the graph to draw
        title : str
            the title of the graph
        kwargs : dict
            additional parameters used to configure the drawing style
            * *fontname* : str, default 'arial'
                the font to use
            * *fillcolor* : str, default '#ADE1FE'
                the fill color of the vertices
            * *edge_line_width* : int, default 3
                the width of the edges
            * *dash_width* : int, default 5
                number of dashess in the head/properties delimiter
            * *v_limit* : int, default 10
                the maximum number of vertices to show
            * *e_limit* : int, default 10
                the maximum number of edges to show
            * *vertex_properties* : list, default None
                the properties to display for vertices, if `None` all properties are shown
            * *edge_properties* : list, default None
                the properties to display for edges, if `None` all properties are shown
            * *prog* : str, default 'dot'
                the layout program to use
        """
        self.g = g
        self.title = title
        self.config = kwargs or {}
        self.vertex_keys = self.config.get("vertex_properties", None)
        self.edge_keys = self.config.get("edge_properties", None)

        self.v_drawn = set()
        self.e_drawn = set()

    def _draw_vertex(self, n):
        if len(self.v_drawn) >= self.config.get("v_limit", 10):
            return False
        if n[0] in self.v_drawn:
            return None
        id, properties = n
        head = f"{id}, {properties.pop('name')}\n{', '.join(properties.pop('labels'))}"
        if self.vertex_keys:
            properties = {k: v for k, v in properties.items() if k in self.vertex_keys}
        body = "\n".join([f"{k}: {v}" for k, v in properties.items()])
        label = f"{head}\n" + ("-" * self.config.get("dash_width", 5)) + f"\n{body}"

        self.gviz.add_node(
            id,
            label=label,
            fillcolor=self.config.get("fillcolor", "#ADE1FE"),
            style="filled",
            fontname=self.config.get("fontname", "arial"),
        )
        self.v_drawn.add(id)
        return True

    def _draw_edge(self, e, with_vertices: bool = True):
        if len(self.e_drawn) > self.config.get("e_limit", 10):
            return False
        if e[:-1] in self.e_drawn:
            return None
        if with_vertices:
            self._draw_vertex((e[0], self.g.nodes[e[0]]))
            self._draw_vertex((e[1], self.g.nodes[e[1]]))
        head = f"{e[2].pop('labels')}"
        body = "\n".join([f"{k}: {v}" for k, v in e[2].items()])
        label = f"{head}\n" + ("-" * self.config.get("dash_width", 5)) + f"\n{body}"

        self.gviz.add_edge(
            e[0],
            e[1],
            label=label,
            style=f"setlinewidth({self.config.get('edge_line_width', 3)})",
            fontname=self.config.get("fontname", "arial"),
        )
        self.e_drawn.add(e[:-1])

    def draw(self, outputfile: str):
        """
        draw the given graphviz markup from the given output file using
        the graphviz "dot" software
        """
        try:
            import pygraphviz
        except ImportError:
            raise ImportError("Please install pygraphviz to draw graphs.")

        self.gviz: pygraphviz.AGraph = networkx.nx_agraph.to_agraph(self.g)
        for n in self.g.nodes(data=True):
            if self._draw_vertex(n) == False:
                break
        for e in self.g.edges(data=True):
            if self._draw_edge(e) == False:
                break
        self.gviz.layout(prog=self.config.get("prog", "dot"))
        self.gviz.draw(outputfile)
