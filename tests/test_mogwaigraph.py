import os

from mogwai.core.exceptions import MogwaiGraphError
from mogwai.core.mogwaigraph import MogwaiGraph, MogwaiGraphConfig

from tests.basetest import BaseTest


class TestMogwaiGraph(BaseTest):
    """
    tests for MogwaiGraph core functionality
    """
    def setUp(self, debug=True, profile=True):
        BaseTest.setUp(self, debug=debug, profile=profile)
        self.G = MogwaiGraph()
        if self.debug:
            print("Graph initialized")

    def construct(self):
        Gimli = self.G.add_labeled_node(
            label={"Character", "Dwarf"}, name="Gimli", properties={"Age": 140}
        )
        Aragorn = self.G.add_labeled_node(
            label={"Character", "Man"}, name="Aragorn", properties={"Age": 88}
        )
        Legolas = self.G.add_labeled_node(
            label={"Character", "Elf"}, name="Legolas", properties={"Age": 2931}
        )
        print("Nodes: \n")
        print(self.G.nodes(data=True))

        self.G.add_labeled_edge(Gimli, Legolas, "Friend")
        self.G.add_labeled_edge(Aragorn, Legolas, "Bestie")
        self.G.add_labeled_edge(Aragorn, Legolas, "Besties")

    def test_mogwaigraph(self):
        self.construct()
        if self.debug:
            print("Edges: \n")
            print(self.G.edges(data=True))
        self.G.draw(os.path.join(self.root_path, "tests", "test.svg"), prog="dot")

    def test_merge(self):
        self.construct()
        G2 = MogwaiGraph()
        Frodo = G2.add_labeled_node(
            label="Character", name="Frodo", properties={"Age": 56}
        )
        Sam = G2.add_labeled_node(label="Character", name="Sam", properties={"Age": 28})
        G2.add_labeled_edge(Frodo, Sam, "Companions")
        self.G.merge_subgraph(G2, 0, Frodo, "Guide")
        self.G.draw(os.path.join(self.root_path, "tests", "merge_test.svg"), prog="dot")

    def test_missing_node(self):
        print("Testing for missing edge")
        gimli = self.G.add_labeled_node(
            label="Character", name="Gimli", properties={"Age": 140}
        )
        with self.assertRaises(MogwaiGraphError):
            self.G.add_labeled_edge(gimli, -1, "something")

    def test_modern(self):
        g = MogwaiGraph.modern()
        g.draw(os.path.join(self.root_path, "tests", "modern.svg"), prog="dot")

    def test_crew(self):
        g = MogwaiGraph.crew()
        g.draw(os.path.join(self.root_path, "tests", "crew.svg"), prog="dot")

    def test_networkx_compatibility_issue12(self):
        """
        see https://github.com/juupje/pyMogwai/issues/12
        """
        g=MogwaiGraph()
        # Should work with standard networkx API when lenient
        node_id = g.add_node(1, weight=0.4)
        self.assertEqual(g.nodes[node_id][g.config.label_field], {g.config.default_node_label})

        # Should work with explicit labels too
        labeled_node = g.add_node(2, labels={'Person'}, name='Bob')
        self.assertEqual(g.nodes[labeled_node][g.config.label_field], {'Person'})

    def test_configurable_reserved_names(self):
        """
        see https://github.com/juupje/pyMogwai/issues/11
        Test combinations of field configurations and property usage with expected outcomes
        """
        test_cases = [
            # (config name_field, property to use, should_pass)
            ('_name', 'name', True),     # non-reserved name should work
            ('_name', '_name', False),   # using reserved name should fail
            ('name', '_name', True),     # when name is reserved, _name should work
            ('name', 'name', False),     # using reserved name should fail
            ('name2', '_name', True),    # neither reserved, should work
            ('name2', 'name', True)      # neither reserved, should work
        ]

        for i, (name_field, prop_name, should_pass) in enumerate(test_cases):
            with self.subTest(f"config:{name_field} using:{prop_name} expect:{'pass' if should_pass else 'fail'}"):
                config = MogwaiGraphConfig(name_field=name_field)
                g = MogwaiGraph(config=config)
                props = {prop_name: f"value{i}"}
                if should_pass:
                    node_id = g.add_labeled_node('TestLabel', 'test', properties=props)
                    self.assertEqual(g.nodes[node_id][prop_name], f"value{i}")
                else:
                    with self.assertRaises(MogwaiGraphError):
                        g.add_labeled_node('TestLabel', 'test', properties=props)