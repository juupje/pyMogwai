"""
Created on 2024-10-22

@author: wf
"""

from datetime import datetime
from typing import List

import networkx as nx
from rdflib import XSD
from rdflib import Graph as RDFGraph
from rdflib import Literal, Namespace, URIRef

from mogwai.schema.graph_schema import GraphSchema


class NetworkXToRDFConverter:
    """
    A converter for converting a NetworkX graph to RDF based on the given GraphSchema.
    """

    def __init__(self, schema: GraphSchema, namespaces: List[str]):
        """
        Initialize the converter with the given schema.

        Args:
            schema (GraphSchema): The graph schema containing the node type configurations and base URI.
            namespaces (List[str]): A list of namespaces used for managing prefixes in the graph.
        """
        self.schema = schema
        self.base_uri = schema.base_uri
        self.rdf_graph = RDFGraph()
        self.ns = Namespace(self.base_uri)

        # Bind the namespaces for prettier output
        for namespace in namespaces:
            self.rdf_graph.bind(namespace, self.ns)
        self.rdf_graph.bind("xsd", XSD)

    def _get_rdf_literal(self, value):
        """
        Convert Python values to appropriate RDF literals
        """
        if isinstance(value, bool):
            return Literal(value, datatype=XSD.boolean)
        elif isinstance(value, int):
            return Literal(value, datatype=XSD.integer)
        elif isinstance(value, datetime):
            return Literal(value.isoformat(), datatype=XSD.dateTime)
        else:
            return Literal(str(value))

    def convert_node(self, node_id, node_data):
        """
        Convert a NetworkX node to RDF and add it to the RDFLib graph.

        Args:
            node_id: The node identifier
            node_data: The data associated with the node
        """
        node_uri = URIRef(f"{self.base_uri}{node_id}")

        # Add all node attributes as properties
        for key, value in node_data.items():
            if value is not None:  # Skip None values
                predicate = URIRef(f"{self.base_uri}{key}")
                self.rdf_graph.add((node_uri, predicate, self._get_rdf_literal(value)))

    def convert_edge(self, source_id, target_id, edge_data):
        """
        Convert a NetworkX edge to RDF and add it to the RDFLib graph.

        Args:
            source_id: The source node identifier
            target_id: The target node identifier
            edge_data: The data associated with the edge
        """
        source_uri = URIRef(f"{self.base_uri}{source_id}")
        target_uri = URIRef(f"{self.base_uri}{target_id}")

        if edge_data and "labels" in edge_data:
            predicate = URIRef(f"{self.base_uri}{edge_data['labels']}")
            self.rdf_graph.add((source_uri, predicate, target_uri))

    def convert_graph(self, nx_graph: nx.Graph):
        """
        Convert the entire NetworkX graph to RDF.

        Args:
            nx_graph (nx.Graph): The NetworkX graph to convert
        """
        # Convert all nodes
        for node_id, node_data in nx_graph.nodes(data=True):
            self.convert_node(node_id, node_data)

        # Convert all edges
        for source_id, target_id, edge_data in nx_graph.edges(data=True):
            self.convert_edge(source_id, target_id, edge_data)

    def serialize(self, rdf_format: str = "turtle") -> str:
        """
        Serialize the RDF graph to the specified format.

        Args:
            rdf_format (str): The RDF format to serialize to (e.g., 'turtle', 'xml').

        Returns:
            str: The serialized RDF graph.
        """
        return self.rdf_graph.serialize(format=rdf_format)
