"""
Created on 2024-10-22

@author: wf
"""

import os
import platform

import lodstorage.sample2
from rdflib import Graph
from mogwai.examples.schema import MogwaiExampleSchema
from mogwai.core import MogwaiGraph
from mogwai.core.traversal import MogwaiGraphTraversalSource
from mogwai.schema.graph_schema import GraphSchema, NodeTypeConfig
from mogwai.schema.nx_to_rdf import NetworkXToRDFConverter
from tests.basetest import BaseTest


class Royals(lodstorage.sample2.Royals):
    """
    extended royals example
    """

    @classmethod
    def from_royals(cls, royals: lodstorage.sample2.Royals) -> "Royals":
        """
        Create extended Royals instance from base Royals

        Args:
            royals: Base Royals instance to copy from

        Returns:
            Royals: New instance of extended Royals
        """
        instance = cls()
        instance.members = royals.members
        return instance

    def add_to_graph(self, graph: MogwaiGraph):
        """
        Add the royal members to the graph
        """
        nodes = {}
        # Add nodes for each royal member
        for royal in self.members:
            # Convert Royal instance to dict and use all fields
            props = royal.__dict__.copy()
            props["full_name"] = props.pop("name", None)
            # Create node with all properties from dict
            node = graph.add_labeled_node(
                label="Royal", name=royal.identifier, properties=props
            )
            nodes[royal.identifier] = node

        # Add succession edges between members based on number_in_line
        for i in range(len(self.members) - 1):
            current = self.members[i]
            next_in_line = self.members[i + 1]
            if (
                current.number_in_line is not None
                and next_in_line.number_in_line is not None
                and current.number_in_line < next_in_line.number_in_line
            ):
                graph.add_labeled_edge(
                    srcId=nodes[current.identifier],
                    destId=nodes[next_in_line.identifier],
                    edgeLabel="succeeds",
                )


class TestGraphSchema(BaseTest):
    """
    test graph schema
    """

    def setUp(self, debug=True, profile=True):
        BaseTest.setUp(self, debug=debug, profile=profile)

    def get_schema(self) -> GraphSchema:
        """
        Helper function to create a GraphSchema for tests.

        Returns:
            GraphSchema: A new GraphSchema instance for testing.
        """
        # Create schema for conversion
        schema = GraphSchema(
            node_id_type_name="str",
            base_uri="http://royal.example.org/",
            node_type_configs={
                "Royal": NodeTypeConfig(
                    label="Royal",
                    icon="person",
                    key_field="identifier",
                    dataclass_name="lodstorage.sample2.Royal",
                    display_name="Royal Family Member",
                    description="Member of the royal family",
                )
            },
        )
        return schema

    def get_royals_graph(self):
        """
        get the royals graph
        """
        # Get sample royal data
        base_royals = lodstorage.sample2.Royals.get_samples()[
            "QE2 heirs up to number in line 5"
        ]
        royals = Royals.from_royals(base_royals)

        # Create graph and add royals
        graph = MogwaiGraph()
        royals.add_to_graph(graph)
        return graph

    def test_schema_loading(self):
        """
        test loading the schema
        """
        yaml_path=MogwaiExampleSchema.get_yaml_path()
        schema = GraphSchema.load(yaml_path)
        self.assertIsNotNone(schema)
        self.assertTrue(len(schema.node_type_configs) >= 1)
        node_type = schema.node_type_configs["NodeTypeConfig"]
        self.assertEqual("NodeTypeConfig", node_type.label)
        self.assertEqual("schema", node_type.icon)
        # Check node_id_type is str
        self.assertEqual(schema.node_id_type, str)

    def test_royals_graph(self):
        """
        Test drawing the royals graph
        """
        # Create the graph from the sample royals
        graph = self.get_royals_graph()

        # Define the path to save the SVG file
        output_path = "/tmp/royal_family_graph.svg"

        # Draw the graph to an SVG file using dot layout
        if platform.system() == "Darwin":  # Darwin represents macOS
            os.environ["PATH"] += os.pathsep + "/opt/local/bin"
        graph.draw(output_path, prog="dot")

        # Ensure the file was created
        self.assertTrue(os.path.exists(output_path), "Graph SVG file was not created.")

    def test_mogwai_queries(self):
        """
        test mogwai queries
        """
        # Create the graph from the sample royals
        graph = self.get_royals_graph()

        # Initialize the traversal source
        g = MogwaiGraphTraversalSource(graph)

        # 1. Test that 7 royal nodes were created
        royal_nodes_query = g.V().hasLabel("Royal").to_list()
        royal_nodes = royal_nodes_query.run()  # Execute the query
        self.assertEqual(
            7, len(royal_nodes), "There should be exactly 7 Royal nodes in the graph."
        )

        # 2. Test that there are at least 5 succession edges
        edges_query = g.E().hasLabel("succeeds").to_list()  # Create the query for edges
        edges = edges_query.run()  # Run the query
        self.assertGreaterEqual(
            len(edges), 5, "There should be at least 5 succession relationships."
        )

        # 3. Test specific member - Queen Elizabeth II using full_name
        queen_query = (
            g.V().has("full_name", "Elizabeth Alexandra Mary Windsor").to_list()
        )  # Create the query
        queen_id = queen_query.run()[0]  # Run the query and get the Queen node
        queen_name_query = (
            g.V(queen_id).values("full_name").next()
        )  # Query for Queen's full name
        queen_name = queen_name_query.run()  # Run the query
        self.assertEqual("Elizabeth Alexandra Mary Windsor", queen_name)

        # 4. Test succession path from William to his successor(s)
        william_query = (
            g.V().has("full_name", "William, Duke of Cambridge").to_list()
        )  # Query by "name" attribute
        william_id = william_query.run()[0]  # Run the query and get William node
        successors_query = (
            g.V(william_id).out("succeeds").values("name").to_list()
        )  # Query for successors of William
        successors = successors_query.run()  # Run the query and get the results
        self.assertEqual(
            1, len(successors), "William should have exactly one successor."
        )
        self.assertEqual("Prince-George-of-Wales-Q13590412", successors[0])

    def test_save_and_reload_schema_with_str_type(self):
        """
        Test saving and reloading the schema with node_id_type as str via /tmp.
        """
        # Create a schema with node_id_type as str
        schema = self.get_schema()
        self.assertIsNotNone(schema)
        self.assertEqual(schema.node_id_type, str)

        # Save the schema to a temporary location
        temp_file = "/tmp/test_graph_schema.yaml"
        schema.save_to_yaml_file(temp_file)

        # Reload the schema from the saved file
        reloaded_schema = GraphSchema.load_from_yaml_file(temp_file)
        self.assertIsNotNone(reloaded_schema)

        # Assert that the reloaded node_id_type is str
        self.assertEqual(reloaded_schema.node_id_type, str)

        # Check that the node_type_configs match between original and reloaded schema
        self.assertEqual(
            schema.node_type_configs.keys(), reloaded_schema.node_type_configs.keys()
        )

        # Cleanup: remove the temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)

    def test_rdf_conversion(self):
        """
        test converting the royal family graph to RDF
        """
        # Setup graph with royals
        graph = self.get_royals_graph()

        # Create schema for conversion
        schema = self.get_schema()
        # Convert to RDF
        converter = NetworkXToRDFConverter(schema, namespaces=["royal"])
        converter.convert_graph(graph)
        rdf_turtle = converter.serialize()

        # Save RDF to a temporary file
        rdf_path = "/tmp/royal_family.ttl"
        with open(rdf_path, "w") as f:
            f.write(rdf_turtle)

        # Debug output if needed
        if self.debug:
            print(f"RDF Output:\n{rdf_turtle}")

        # Basic validation of RDF content
        self.assertIn("royal.example.org", rdf_turtle)
        self.assertIn("Elizabeth Alexandra Mary Windsor", rdf_turtle)
        self.assertIn("succeeds", rdf_turtle)
        self.assertIn("wikidata_id", rdf_turtle)

        # Verify RDF readability with rdflib
        rdf_graph = Graph()
        try:
            rdf_graph.parse(data=rdf_turtle, format="turtle")
            # Check if parsing was successful
            self.assertGreater(
                len(rdf_graph), 0, "Parsed RDF graph should contain triples."
            )
        except Exception as e:
            self.fail(f"RDF parsing failed: {e}")
