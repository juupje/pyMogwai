from mogwai.core import MogwaiGraph
from mogwai.core.steps.statics import *
from mogwai.core.traversal import MogwaiGraphTraversalSource

from .basetest import BaseTest


class TestQueries(BaseTest):
    def setUp(self):
        super().setUp()

    def test_shortest_path(self):
        graph = MogwaiGraph()
        nodes = []
        for i in range(5):
            nodes.append(graph.add_labeled_node("node", str(i + 1)))
        graph.add_labeled_edge(nodes[0], nodes[1], "knows")
        graph.add_labeled_edge(nodes[1], nodes[3], "knows")
        graph.add_labeled_edge(nodes[3], nodes[4], "knows")
        graph.add_labeled_edge(nodes[1], nodes[2], "knows")
        graph.add_labeled_edge(nodes[2], nodes[3], "knows")
        g = MogwaiGraphTraversalSource(graph)
        query = (
            g.V(nodes[0])
            .repeat(out().simple_path())
            .until(has_id(nodes[-1]))
            .path()
            .by("name")
            .as_("p")
            .count(Scope.local)
            .as_("length")
            .order()
            .limit(1)
            .select("p", "length")
            .to_list()
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertEqual(res, [[["1", "2", "4", "5"], 4]], "Incorrect result")


if __name__ == "__main__":
    import unittest

    unittest.main()
