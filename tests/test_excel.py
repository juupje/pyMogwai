import os

from mogwai.parser.excel_converter import EXCELGraph
from tests.basetest import BaseTest


class TestExcel(BaseTest):
    def setUp(self):
        super().setUp()
        # Initialize a sample file system structure for testing
        self.filename = os.path.join(self.documents_path, "test_excel.xlsx")

    def test_excel(self):
        graph = EXCELGraph(self.filename)
        nodes = graph.get_nodes("EXCELFile", "test_excel.xlsx")
        self.assertTrue(len(nodes) == 1, "Incorrect number of test_excel.xlsx nodes")
        self.assertTrue(nodes[0][1]["metadata"]["title"] == "BspTitel", "Title wrong")
        self.assertTrue(
            graph.get_nodes("EXCELSheet", "sheet1")[0][1]["column2"]["2"] == "b4",
            "Wrong column element",
        )
        graph.draw(os.path.join(self.root_path, "tests", "excel_test.svg"), prog="dot")
