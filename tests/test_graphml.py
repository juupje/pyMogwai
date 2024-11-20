import os
import time
import uuid

import networkx as nx

from mogwai.core.mogwaigraph import MogwaiGraph
from mogwai.parser.graphml_converter import graphml_to_mogwaigraph
from tests.basetest import BaseTest


class TestGraphml(BaseTest):
    """
    test graphml handling
    """

    def setUp(self):
        super().setUp()
        # Initialize a sample file system structure for testing
        self.gml_file_name = os.path.join(self.documents_path, "test_gml.graphml")

    def test_gml(self):
        """
        test graph ml conversion handling
        """
        g = graphml_to_mogwaigraph(
            self.gml_file_name,
            node_label_key="color",
            node_name_key="color",
            edge_label_key="edge",
            include_id=True,
        )
        self.assertTrue(
            len(g.nodes) == 6, f"Incorrect number of graphml nodes {len(g.nodes)}"
        )
        self.assertTrue(
            len(g.edges) == 7, f"Incorrect number of graphml edges {len(g.edges)}"
        )
        if self.debug:
            print(g.nodes(data=True))
        self.assertTrue(
            g.nodes(data=True)["0"]["id"] == "n0",
            "Incorrect import of graphml node id",
        )
        self.assertTrue(
            g.nodes(data=True)["2"]["name"] == "blue",
            "Incorrect import of graphml node property",
        )
        g.draw(os.path.join(self.root_path, "tests", "test_gml.svg"), prog="dot")

    def test_graphml_serialization_round_trip(self):
        """
        Test that demonstrates the GraphML serialization
        the Modern graph example, using a round-trip test.

        should fix #15 issue with sets by using string labels
        """
        # Create the Modern graph instance
        graph = MogwaiGraph.modern()
        output_path = os.path.join("/tmp", f"modern_mogwai_{uuid.uuid4().hex}.graphml")

        # Write the graph to GraphML format
        nx.write_graphml(
            graph,
            output_path,
            encoding="utf-8",
            prettyprint=True,
            infer_numeric_types=True,
        )

        # Assert that the file was created
        self.assertTrue(os.path.exists(output_path), "GraphML file was not created.")

        # Assert that the file size is greater than zero
        file_size = os.path.getsize(output_path)
        self.assertGreater(
            file_size, 1900, "GraphML file size is zero, indicating an empty file."
        )

        # Verify the file's creation timestamp is after the recorded time
        creation_time = os.path.getmtime(output_path)
        current_time = time.time()
        self.assertLessEqual(
            current_time - creation_time,
            1,
            "GraphML file timestamp for {output_path} is incorrect, indicating it wasn't created during the test.",
        )

        # Read the graph back from the file
        loaded_graph = nx.read_graphml(output_path)

        # Check that node properties are preserved correctly
        for node in graph.nodes:
            for key in graph.nodes[node]:
                original_value = graph.nodes[node][key]
                loaded_value = loaded_graph.nodes[node][key]

                self.assertEqual(
                    original_value,
                    loaded_value,
                    f"Mismatch in node property '{key}' for node '{node}'",
                )
