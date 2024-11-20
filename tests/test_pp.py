import os
from mogwai.parser.powerpoint_converter import PPGraph
from tests.basetest import BaseTest

class TestPowerPoint(BaseTest):
    def setUp(self):
        super().setUp()
        # Initialize a sample file system structure for testing
        self.filename = os.path.join(self.documents_path, "test_pp1.pptx")
        self.midterm = os.path.join(self.documents_path, "midterm.pptx")

    def test_pp(self):
        graph = PPGraph(self.filename)
        nodes = graph.get_nodes("PPFile", "test_pp1.pptx")
        self.assertEqual(len(nodes), 1, "Incorrect number of test_pp1.pptx nodes")
        self.assertTrue(
            nodes[0][1]["metadata"]["author"].startswith("Musselman"),
            "'author' field of metadata incorrect.",
        )
        nodes = graph.get_nodes("PPPage", "page2")
        self.assertGreater(len(nodes), 0, "No slide named 'Resources'")
        self.assertEqual(nodes[0][1]["title"], "Resources", "Title wrong")
        self.assertEqual(nodes[0][1]["page"], 2, "Page number wrong")
        graph.draw(os.path.join(self.root_path, "tests", "pp_test.svg"), "dot")
        g = PPGraph(self.midterm)
        g.draw(os.path.join(self.root_path, "tests", "midterm.svg"), "dot")
