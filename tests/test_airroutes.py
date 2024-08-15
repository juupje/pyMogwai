import unittest

from mogwai.core.traversal import MogwaiGraphTraversalSource
from tests.basetest import BaseTest


class TestSteps(BaseTest):
    """
    test Steps
    """

    def setUp(self):
        from mogwai.parser import graphml_to_mogwaigraph

        super().setUp()
        self.airroutes = graphml_to_mogwaigraph(
            "tests/documents/air-routes-latest.graphml",
            node_label_key="labelV",
            edge_label_key="labelE",
            node_name_key=lambda x: (
                x.pop("code") if x["type"] == "airport" else x.pop("desc")
            ),
        )

    @unittest.skipIf(not BaseTest.inPublicCI(), "slow test")
    def test_speed(self):
        g = MogwaiGraphTraversalSource(self.airroutes)
        query = (
            g.V()
            .has_label("country")
            .has_name("United States")
            .out("contains")
            .out("route")
            .count()
            .next()
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result", res)
        self.assertEqual(res, 9119, "Incorrect number of routes to US.")

    @unittest.skipIf(not BaseTest.inPublicCI(), "very slow test")
    def test_monster(self):
        from mogwai.core.steps.statics import lte, outE, select

        g = MogwaiGraphTraversalSource(self.airroutes)
        query = (
            g.V()
            .has_label("airport")
            .has_name("LAX")
            .as_("start")
            .repeat(
                outE("route")
                .as_("e")
                .inV()
                .filter_(select("e").values("dist").is_(lte(5000)))
                .simple_path()
            )
            .times(3)
            .as_path()
            .by("name")
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result length", len(res))
        self.assertTrue(len(res) == 555028, "Incorrect result, expected 555028")
