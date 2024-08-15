import os

from mogwai.parser.powerpoint_converter import PPGraph

from .basetest import BaseTest


class TestGraph(BaseTest):
    def setUp(self):
        super().setUp()
        # Initialize a sample file system structure for testing
        self.filename = os.path.join(self.documents_path, "test_pp1.pptx")
        self.midterm = os.path.join(self.documents_path, "midterm.pptx")

    def test_pp(self):
        graph = PPGraph(self.filename)
        nodes = graph.get_nodes("PPFile", "test_pp1.pptx")
        self.assertTrue(len(nodes) == 1, "Incorrect number of test_pp1.pptx nodes")
        self.assertTrue(
            nodes[0][1]["properties"]["metadata"]["author"].startswith("Musselman"),
            "'author' field of metadata incorrect.",
        )
        self.assertTrue(
            graph.get_nodes("PPPage", "page2")[0][1]["properties"]["title"]
            == "Resources",
            "Title wrong",
        )
        graph.draw(os.path.join(self.root_path, "tests", "pp_test.svg"), "dot")
        g = PPGraph(self.midterm)
        g.draw(os.path.join(self.root_path, "tests", "midterm.svg"), "dot")


if __name__ == "__main__":
    import unittest

    unittest.main()
