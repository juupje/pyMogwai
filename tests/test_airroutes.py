import unittest

from mogwai.core.traversal import MogwaiGraphTraversalSource
from tests.basetest import BaseTest

class TestSteps(BaseTest):
    """
    test Steps
    """

    def setUp(self, debug=True, profile=True):
        BaseTest.setUp(self, debug=debug, profile=profile)
        self.airroutes = self.examples.get("air-routes-latest")
    def test_speed(self):
        """
        tests speed - 9119 results in 1.3 s on a 2.4 GHz 8 Core Intel Core i9
        """
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

    @unittest.skipIf(not BaseTest.inPublicCI(), "slow >90s test")
    def test_monster(self):
        """
        test half a million nodes as a result in over 90 secs
        """
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
