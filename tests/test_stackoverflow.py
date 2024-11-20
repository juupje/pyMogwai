"""
Created on 2024-11-10

@author: wf
"""

import json

from mogwai.core.mogwaigraph import MogwaiGraph, MogwaiGraphConfig
from mogwai.core.traversal import MogwaiGraphTraversalSource
from tests.basetest import BaseTest


class TestSpogJoin(BaseTest):
    """Test SPOG index based joins"""

    def setUp(self, debug=False):
        BaseTest.setUp(self, debug=debug)

    def test_spog_join(self):
        """Test joining vertices by property values using SPOG index"""
        # Create test graph with CC and KK vertices having same names
        graph = MogwaiGraph(
            config=MogwaiGraphConfig(name_field="_node_name", index_config="minimal")
        )

        # Add test vertices
        v1 = graph.add_labeled_node("CC", "t1", {"name": "t1"})
        v2 = graph.add_labeled_node("CC", "t2", {"name": "t2"})
        v3 = graph.add_labeled_node("KK", "k1", {"name": "t1"})
        v4 = graph.add_labeled_node("KK", "k2", {"name": "t3"})

        # Use SPOG join
        graph.join("CC", "KK", "name", "name", "same_name")

        # Verify joins
        g = MogwaiGraphTraversalSource(graph)
        joins = g.E().has_label("same_name").count().next().run()
        self.assertEqual(joins, 1)

        if self.debug:
            self.show_indices(graph.spog_index)

    def show_indices(self, spog_index, limit=3):
        """Show SPOG index contents"""
        for index_name in spog_index.config.active_indices:
            if index := spog_index.indices.get(index_name):
                print(f"Index: {index_name}")
                keys = list(index.lookup.keys())[:limit]
                lookup = {k: list(index.lookup[k])[:limit] for k in keys}
                print(json.dumps(lookup, indent=2, default=str))
