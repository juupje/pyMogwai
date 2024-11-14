import unittest

import mogwai.core.traversal as Trav
from mogwai.core import MogwaiGraph
from mogwai.core.steps.flatmap_steps import out
from mogwai.parser.filesystem import FileSystemGraph as FSG
from tests.basetest import BaseTest


class TestSteps(BaseTest):
    """
    test steps
    """

    def setUp(self, debug=True, profile=True):
        BaseTest.setUp(self, debug=debug, profile=profile)
        self.file_system = FSG(self.root_path)
        self.modern = MogwaiGraph.modern()
        self.crew = MogwaiGraph.crew()

    def test_filter(self):
        g = Trav.MogwaiGraphTraversalSource(self.file_system)
        query = (
            g.V()
            .has_label("Directory")
            .filter_(out("HAS_FILE").has_label("PDFFile"))
            .to_list(by="name")
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertIn("documents", res, "'documents' not in query result")
        self.assertTrue(len(res) > 0, "Are there folders with pdf documents?")

    def test_is(self):
        from mogwai.core.steps.map_steps import values
        from mogwai.core.steps.predicates import gte

        g = Trav.MogwaiGraphTraversalSource(self.modern)

        # We want software written by >30 year olds
        query = (
            g.V()
            .has_label("Person")
            .filter_(values("age").is_(gte(30)))
            .out("created")
            .to_list(by="name")
        )

        print("Query:", query)
        res = query.run()
        print("Result:", res)

    def test_count(self):
        print("Checking count!")
        from mogwai.core.steps.flatmap_steps import outE
        from mogwai.core.steps.predicates import gt

        g = Trav.MogwaiGraphTraversalSource(self.modern)
        # we want persons who contributed to >1 softwares
        query = (
            g.V()
            .has_label("Person")
            .filter_(outE("created").count().is_(gt(1)))
            .name()
            .to_list()
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        print(g.V().has_label("Person").outE("created").count().to_list().run())

    def test_in(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = (
            g.V()
            .has_label("Software")
            .as_("a")
            .in_("created")
            .has_name("peter")
            .select("a", by="name")
            .to_list()
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertTrue("lop" in res and len(res) == 1, "Incorrect query result!")

    def test_local(self):
        g = Trav.MogwaiGraphTraversalSource(self.crew)
        query = (
            g.V().has_label("Person").local(out("develops").limit(1).name()).to_list()
        )
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertTrue(len(res) == 3, "Incorrect query result!")

    def test_range(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V().has_label("Person").range(1, 3).name().to_list()
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertTrue(len(res) == 2, "Incorrect query result!")

        query = g.V().has_label("Person").skip(2).name().to_list()
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertTrue(len(res) == 2, "Incorrect query result!")

        query = g.V().has_label("Person").limit(1).name().to_list()
        print("Query:", query.print_query())
        res = query.run()
        print("Result:", res)
        self.assertTrue(len(res) == 1, "Incorrect query result!")

    def test_element_map(self):
        """
        test the element_map step
        """
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        query = g.V("0").element_map().to_list()
        if self.debug:
            print("Query:", query.print_query())
        res = query.run()
        if self.debug:
            print("Result:", res)
        self.assertTrue(len(res) == 1, "Query should have returned only one result")
        self.assertEqual(res[0], {"labels": "Person", "name": "marko", "age": 29})
