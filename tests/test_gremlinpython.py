import os
import unittest
from timeit import default_timer as timer

from mogwai.core.steps.statics import Scope as MogwaiScope
from tests.basetest import BaseTest


@unittest.skip("Skipping tests")
class TestGremlinPython(BaseTest):
    def setUp(self):
        super().setUp()
        self.air_routes_small = os.path.join(
            self.documents_path, "air_routes_small.graphml"
        )
        self.air_routes_latest = os.path.join(
            self.documents_path, "air_routes_latest.graphml"
        )

    def test_mogwai(self):
        from mogwai.core.steps.statics import has_name, out
        from mogwai.core.traversal import MogwaiGraph, MogwaiGraphTraversalSource

        print("mogwai graph test")
        graph = MogwaiGraph()
        nodes = []
        for i in "abcde":
            nodes.append(graph.add_labeled_node("node", i))
        graph.add_labeled_edge(nodes[0], nodes[1], "knows")
        graph.add_labeled_edge(nodes[1], nodes[3], "knows")
        graph.add_labeled_edge(nodes[3], nodes[4], "knows")
        graph.add_labeled_edge(nodes[1], nodes[2], "knows")
        graph.add_labeled_edge(nodes[2], nodes[3], "knows")
        g = MogwaiGraphTraversalSource(graph)

        start = timer()
        query = (
            g.V()
            .has_name("a")
            .repeat(out().simple_path())
            .until(has_name("e"))
            .path()
            .by("name")
            .as_("p")
            .count(MogwaiScope.local)
            .as_("length")
            .order()
            .limit(1)
            .select("p", "length")
            .to_list()
        )
        res = query.run()
        end = timer()
        diff = end - start
        print("Result:", res)
        print(f"mogwai graph has the output in {diff} seconds")

    """     def test_mogwai(self):
        #from mogwai.core.steps.statics import *
        from mogwai.core.traversal import *
        print("mogwai graph test")
        air_routes_mog = graphml_to_mogwaigraph(self.air_routes_small, "node_label", "node_name")
        g = Trav.MogwaiGraphTraversalSource(air_routes_mog)

        start = timer()
        output =g.V(0).repeat(out().simple_path()).until(has_name(4)).path().by('name').as_('p').count(local=True).as_('length').order().limit(1).select('p', 'length').to_list().by('name')
        end = timer()

        diff = end-start
        print(output)
        print (f"mogwai graph has the output in {diff} seconds") """

    def test_gremlinpython(self):
        import warnings

        from gremlin_python.driver.aiohttp.transport import (
            AiohttpHTTPTransport,
            AiohttpTransport,
        )
        from gremlin_python.driver.client import Client
        from gremlin_python.driver.driver_remote_connection import (
            DriverRemoteConnection,
        )
        from gremlin_python.process.anonymous_traversal import traversal
        from gremlin_python.process.graph_traversal import __
        from gremlin_python.process.traversal import Scope
        from gremlin_python.structure.graph import Graph

        warnings.filterwarnings("ignore", category=DeprecationWarning)
        print("gremlinpython graph test")
        try:
            connection = DriverRemoteConnection("ws://localhost:8182/gremlin", "g")
            g = traversal().withRemote(connection)
            # needed as connection is not checked for validity in the DriverRemoteConnection
            g.V().iterate()
        except:
            print("Could not connect to server")
            return

        # small_file = open(self.air_routes_small, "r")
        g.V().drop().iterate()
        # g.io(self.air_routes_small).read().iterate()
        g.addV().property("name", "a").as_("1").addV().property("name", "b").as_(
            "2"
        ).addV().property("name", "c").as_("3").addV().property("name", "d").as_(
            "4"
        ).addV().property(
            "name", "e"
        ).as_(
            "5"
        ).addE(
            "knows"
        ).from_(
            "1"
        ).to(
            "2"
        ).addE(
            "knows"
        ).from_(
            "2"
        ).to(
            "4"
        ).addE(
            "knows"
        ).from_(
            "4"
        ).to(
            "5"
        ).addE(
            "knows"
        ).from_(
            "2"
        ).to(
            "3"
        ).addE(
            "knows"
        ).from_(
            "3"
        ).to(
            "4"
        ).iterate()
        # print(g.V().to_list())
        # print(g.E().to_list())
        # print(g.V().has("name", "a").to_list())

        start = timer()
        output = (
            g.V()
            .has("name", "a")
            .repeat(__.out().simple_path())
            .until(__.has("name", "e"))
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
        # g.V(1).repeat(__.out().simple_path()).until(has_name(5)).as_('p').count(Scope.local).as_('length').order().limit(1).select('p', 'length').to_list()

        end = timer()
        diff = end - start

        print(output)
        print(f"gremlinpython graph got output in {diff} seconds")
        # small_file.close()
        connection.close()
