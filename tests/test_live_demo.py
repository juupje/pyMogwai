import unittest

from mogwai.core.traversal import MogwaiGraphTraversalSource
from mogwai.core.steps.enums import Order
from tests.basetest import BaseTest

class TestLiveDemo(BaseTest):
    def setUp(self):
        from mogwai.parser import graphml_to_mogwaigraph

        super().setUp()
        self.airroutes = graphml_to_mogwaigraph(
            f"{self.documents_path}/air-routes-latest.graphml",
            node_label_key="labelV",
            edge_label_key="labelE",
            node_name_key=lambda x: (
                x.pop("code") if x["type"] == "airport" else x.pop("desc")
            ),
        )
        self.g = MogwaiGraphTraversalSource(self.airroutes)

    def test_away_from_germany(self):
        query = (
            self.g.V()
            .has_label("country")
            .has_name("Germany")
            .out("contains")
            .out("route")
            .in_("contains")
            .has_label("country")
            .name()
            .dedup()
            .to_list()
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res[:10], "total: ", len(res))
        self.assertEqual(len(res), 110, "Incorrect number of results")

    # does not work in gremlin and gremlinpython
    def test_furthest(self):
        query = (
            self.g.V()
            .has_label("country")
            .has_name("Germany")
            .out("contains")
            .outE("route")
            .as_("e")
            .inV()
            .in_("contains")
            .has_label("country")
            .as_("dest")
            .select("e")
            .values("dist")
            .max_()
            .select("dest")
            .by("name")
            .next()
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertEqual(res, "Argentina", "Incorrect result")

    # does not work in gremlin and gremlinpython
    def test_furthest_with_origin(self):
        query = (
            self.g.V()
            .has_label("country")
            .has_name("Germany")
            .out("contains")
            .as_("start")
            .outE("route")
            .as_("e")
            .inV()
            .in_("contains")
            .has_label("country")
            .as_("dest")
            .select("e")
            .values("dist")
            .as_("dist")
            .max_()
            .select("start", "dest", "dist")
            .to_list()
            .by("name")
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertEqual(res[0], ["FRA", "Argentina", 7141], "Incorrect result")

    def test_from_nearby_airports(self):
        from mogwai.core.steps.statics import select
        query = (
            self.g.V()
            .has_label("airport")
            .within(
                "city",
                ["Brussels", "Maastricht", "Aachen", "Dusseldorf"],
            )
            .as_("start")
            .outE("route")
            .as_("e")
            .inV()
            .in_("contains")
            .has_label("country")
            .as_("dest")
            .order(Order.desc)
            .by(select("e").values("dist"))
            .limit(5)
            .select("start", "dest")
            .to_list()
            .by("name")
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)

    @unittest.skipIf(not BaseTest.inPublicCI(), "slow test")
    def test_reachability(self):
        from mogwai.core.steps.statics import Scope, lte, outE, select

        g = MogwaiGraphTraversalSource(self.airroutes)
        query = (
            g.V()
            .has_label("airport")
            .has("city", "Maastricht")
            .as_("start")
            .repeat(
                outE("route")
                .as_("e")
                .inV()
                .filter_(select("e").values("dist").is_(lte(2000)))
                .simple_path()
            )
            .times(3)
            .emit()
            .as_("dest")
            .path()
            .as_("p")
            .count(Scope.local)
            .order(Order.asc)
            .as_("length")
            .dedup()
            .select("dest", "length")
            .to_list()
            .by("city")
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result length", len(res))
        # self.assertTrue(len(res)==555028, "Incorrect result, expected 555028")
