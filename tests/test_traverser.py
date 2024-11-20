import mogwai.core.traversal as Trav
from mogwai.core import MogwaiGraph, Traverser
from mogwai.parser.filesystem import FileSystemGraph as FSG
from tests.basetest import BaseTest


class TestTraverser(BaseTest):
    """
    test for Traverser
    """

    def setUp(self):
        super().setUp()
        self.file_system = FSG(self.root_path)
        self.modern = MogwaiGraph.modern()

    def test_creation_of_traversal(self):
        g = Trav.MogwaiGraphTraversalSource(self.file_system)
        query = g.V().count()
        print(f"Query: {query.print_query()}")
        print(f"Result: {query.run()}")

    def test_has(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().has_label("Person").to_list(include_data=True)
        print(f"Query: {query.print_query()}")
        print(f"Result: {query.run()}")

    def test_within(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = (
            g.V()
            .has_label("Person")
            .within("name", ["marko", "vadas"])
            .to_list()
            .by("name")
        )
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue(
            ("marko" in res and "vadas" in res and len(res) == 2), "Incorrect output"
        )

    def test_out(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = (
            g.V()
            .has_label("Person")
            .has_name("marko")
            .out("created")
            .to_list(by="name")
        )
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue("lop" in res and len(res) == 1, "Incorrect query result")

    def test_both(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().has_label("Person").has_name("josh").both().to_list(by="name")
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue(
            set(res) == {"marko", "lop", "ripple"}, "Incorrect query result"
        )

    def test_values(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().out().out().values("name").to_list()
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue("ripple" in res and "lop" in res, "Query result incorrect")

        query2 = g.V().out().out().properties("name").value().to_list()
        res2 = query2.run()
        self.assertEqual(set(res2), set(res), "Query result incorrect")

    def test_cache_and_select(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().out().as_("a").out().select("a", by="name").to_list()
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue(res[0] == "josh" and res[1] == "josh", "Query result incorrect")

    def test_repeat(self):
        from mogwai.core.steps import flatmap_steps as fms

        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().repeat(do=fms.out(), times=2).to_list()
        reference_query = g.V().out().out().to_list()
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue(
            set(res) == set(reference_query.run()),
            "Query result incorrect! Should be " + str(reference_query.run()),
        )

        from mogwai.core.steps import filter_steps as fs

        query = (
            g.V().repeat(until=fs.has_name("ripple"), do=fms.out()).as_path(by="name")
        )
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue(
            all(
                x in res
                for x in [["josh", "ripple"], ["marko", "josh", "ripple"], ["ripple"]]
            ),
            "Query result incorrect!",
        )

    def test_sideeffect(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)

        def side_effect_fn(traverser: Traverser):
            traverser.set("age", 30)

        query = g.V().side_effect(side_effect_fn)
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        for traverser in res:
            age_value = traverser.get_cache("age")
            print(f"Age value: {age_value}")
            self.assertEqual(
                age_value,
                30,
                f"Traverser {traverser} did not have 'age' attribute set to 30",
            )

        print("Setting city to aachen!")
        query = g.V().has_label("Person").property("City", "Aachen").run()
        for node in g.V().has_label("Person").to_list().run():
            data = self.modern.nodes(data=True)[node]
            print(f"{data['name']} -> City: {data['City']}")
            self.assertEqual(
                data["City"],
                "Aachen",
                "SideEffect did not set city to Aachen",
            )

    def test_generator(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().has_label("Person").values("age").iter()
        print("Printing results from generator")
        for res in query.run():
            print(res)

    def test_logic(self):
        from mogwai.core.steps.statics import (
            and_,
            gte,
            has_label,
            has_name,
            not_,
            or_,
            values,
        )

        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = (
            g.V()
            .filter_(and_(has_label("Person"), values("age").is_(gte(30))))
            .to_list(by="name")
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertEqual(set(res), {"josh", "peter"})

        query = (
            g.V()
            .filter_(or_(has_name("marko"), has_label("Software")))
            .to_list(by="name")
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertEqual(set(res), {"marko", "lop", "ripple"})

        query = (
            g.V()
            .has_label("Person")
            .filter_(not_(or_(has_name("marko"), has_name("peter"))))
            .to_list(by="name")
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertEqual(set(res), {"josh", "vadas"})

    """
    def test_has_properties(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().out().has_property("age",32).value("name").to_list()
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue(res[0] == "josh", "Query result incorrect")


        #g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().has_property("lang").value("name").to_list()
        print(f"Query: {query.print_query()}")
        res = query.run()
        print(f"Result: {res}")
        self.assertTrue(res[0]  in ["lop", "ripple"], "Query result incorrect")
    """

    def test_addV_and_addE(self):
        """
        test add Vertices and Edges
        """
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.addV("Person", name="john", age=30).next()
        if self.debug:
            print(f"Query: {query.print_query()}")
        john = query.run()
        if self.debug:
            print(f"Result: {john}")
        self.assertEqual(g.V().count().next().run(), 7, "Node not added to graph")

        vadas = g.V().has_name("vadas").next().run()
        query = g.addE("knows").from_(john).to_(vadas).property("likes", True).iterate()
        if self.debug:
            print(f"Query: {query.print_query()}")
        query.run()
        query = g.E().properties("likes").next()
        if self.debug:
            print(f"Query: {query.print_query()}")
        res = query.run()
        if self.debug:
            print(f"Result: {res}")
        self.assertEqual(res, {"likes": True}, "Node not added to graph")
