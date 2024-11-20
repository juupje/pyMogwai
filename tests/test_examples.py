"""
Created on 2024-08-17

@author: wf
"""

from mogwai.parser.graphml_converter import graphml_to_mogwaigraph
from tests.basetest import BaseTest


class TestExamples(BaseTest):
    """
    test examples handling
    """

    def setUp(self, debug=True, profile=True):
        BaseTest.setUp(self, debug=debug, profile=profile)
        self.airroutes0 = graphml_to_mogwaigraph(
            f"{self.examples_path}/air-routes-small-latest.graphml",
            node_label_key="labelV",
            edge_label_key="labelE",
            node_name_key=lambda x: (
                x.pop("code") if x["type"] == "airport" else x.pop("desc")
            ),
        )

        self.airroutes = self.examples.get("air-routes-small")

    def graph_diff(self, G1, G2):
        result = {
            "nodes_added": [],
            "nodes_removed": [],
            "edges_added": [],
            "edges_removed": [],
            "attributes_changed": [],
        }

        # Use node identifiers for set operations
        nodes_G1 = set(G1.nodes())
        nodes_G2 = set(G2.nodes())
        result["nodes_added"] = list(nodes_G2 - nodes_G1)
        result["nodes_removed"] = list(nodes_G1 - nodes_G2)

        # Use edge identifiers for set operations
        edges_G1 = set(G1.edges())
        edges_G2 = set(G2.edges())
        result["edges_added"] = list(edges_G2 - edges_G1)
        result["edges_removed"] = list(edges_G1 - edges_G2)

        # Check for changes in attributes for nodes
        for node in nodes_G1.intersection(nodes_G2):
            attrs_G1 = G1.nodes[node]
            attrs_G2 = G2.nodes[node]
            for key in set(attrs_G1) | set(attrs_G2):
                if attrs_G1.get(key) != attrs_G2.get(key):
                    result["attributes_changed"].append(
                        ("node", node, key, attrs_G1.get(key), attrs_G2.get(key))
                    )

        # Check for changes in attributes for edges
        for edge in edges_G1.intersection(edges_G2):
            attrs_G1 = G1[edge[0]][edge[1]]
            attrs_G2 = G2[edge[0]][edge[1]]
            for key in set(attrs_G1) | set(attrs_G2):
                if attrs_G1.get(key) != attrs_G2.get(key):
                    result["attributes_changed"].append(
                        ("edge", edge, key, attrs_G1.get(key), attrs_G2.get(key))
                    )

        return result

    def testAirRoutes(self):
        self.assertIsNotNone(self.airroutes0)
        self.assertIsNotNone(self.airroutes)
        # Compute the differences
        differences = self.graph_diff(self.airroutes0, self.airroutes)
        # Print or assert based on differences
        print("Differences:", differences)
        # Optionally, you can add assertions based on expected differences
        # For example:
        # self.assertEqual(len(differences['nodes_added']), 0)
        # This would assert no new nodes should be added between graphs
        pass
