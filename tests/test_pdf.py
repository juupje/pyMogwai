import os

from mogwai.parser import PDFGraph
from tests.basetest import BaseTest


class TestPDF(BaseTest):
    def setUp(self):
        super().setUp()
        # Initialize a sample file system structure for testing
        self.filename = os.path.join(self.documents_path, "lorem.pdf")

    def test_thesis(self):
        graph = PDFGraph(self.filename)
        nodes = graph.get_nodes("PDFFile", "lorem.pdf")
        self.assertTrue(len(nodes) == 1, "Incorrect number of lorem.pdf nodes")
        self.assertTrue(
            nodes[0][1]["metadata"]["creator"].startswith("LaTeX"),
            "'Creator' field of metadata incorrect.",
        )
        self.assertTrue(
            graph.get_nodes("PDFTitle", "discussion")[0][1]["page_number"] == 3,
            "Discussion at wrong place",
        )
        graph.draw(os.path.join(self.root_path, "tests", "pdf_test.svg"), prog="dot")
