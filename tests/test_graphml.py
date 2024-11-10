import os

from mogwai.parser.graphml_converter import graphml_to_mogwaigraph

from .basetest import BaseTest


class TestGraphml(BaseTest):
    """
    test graphml handling
    """

    def setUp(self):
        super().setUp()
        # Initialize a sample file system structure for testing
        self.gml_file_name = os.path.join(self.documents_path, "test_gml.graphml")

    def test_gml(self):
        g = graphml_to_mogwaigraph(
            self.gml_file_name,
            node_label_key=lambda x: "color",
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
        print(g.nodes(data=True))
        self.assertTrue(
            g.nodes(data=True)[0]["id"] == "n0",
            "Incorrect import of graphml node id",
        )
        self.assertTrue(
            g.nodes(data=True)[2]["name"] == "blue",
            "Incorrect import of graphml node property",
        )
        g.draw(os.path.join(self.root_path, "tests", "test_gml.svg"), prog="dot")


if __name__ == "__main__":
    import unittest

    unittest.main()
