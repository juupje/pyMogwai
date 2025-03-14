'''
Created on 2024-11-24

@author: wf
'''
from tests.basetest import BaseTest
from mogwai.core import MogwaiGraph
from mogwai.utils.graph_summary import GraphSummary
import unittest

@unittest.skip("The correct behavior of this test is not yet implemented in the current version of the library.")
class TestGraphSummary(BaseTest):
    """Test the graph summary functionality."""

    def setUp(self, debug=True, profile=True):
        BaseTest.setUp(self, debug=debug, profile=profile)

    def test_section_formats(self):
        """
        test the section formats
        """
        header="Test"
        level=1
        test_cases = [
            ("latex", "\\section{Test}"),
            ("mediawiki", "= Test ="),
            ("github", "# Test"),
        ]

        for fmt, expected_header, in test_cases:
            with self.subTest(fmt=fmt):
                # Test section header formatting
                gs = GraphSummary(None, fmt=fmt)
                header_markup=gs.format_section_header(header, level)
                self.assertEqual(header_markup, expected_header)


    def test_graph_summary(self):
        """Test the graph summary."""
        graph = MogwaiGraph.modern()
        test_cases = [
            ("latex", "\\section{Test}"),
            ("mediawiki", "= Test ="),
            ("github", "# Test"),
        ]
        for fmt, expected_header, in test_cases:
            with self.subTest(fmt=fmt):
                # Test section header formatting
                gs = GraphSummary(graph, fmt=fmt)

                # Test full dump
                dump_result = gs.dump(limit=3)
                if self.debug:
                    print(dump_result)
                self.assertIn("Total Nodes", dump_result)
                self.assertIn("Total Edges", dump_result)
