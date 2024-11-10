import os
import json
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
        g = MogwaiGraph()
        # Should work with standard networkx API when lenient
        node_id = g.add_node(1, weight=0.4)
        self.assertEqual(
            g.nodes[node_id][g.config.label_field], {g.config.default_node_label}
        )

        # Should work with explicit labels too
        labeled_node = g.add_node(2, labels={"Person"}, name="Bob")
        self.assertEqual(g.nodes[labeled_node][g.config.label_field], {"Person"})

    def test_configurable_reserved_names(self):
        """
        see https://github.com/juupje/pyMogwai/issues/11
        Test combinations of field configurations and property usage with expected outcomes
        """
        test_cases = [
            # (config name_field, property to use, should_pass)
            ("_name", "name", True),  # non-reserved name should work
            ("_name", "_name", False),  # using reserved name should fail
            ("name", "_name", True),  # when name is reserved, _name should work
            ("name", "name", False),  # using reserved name should fail
            ("name2", "_name", True),  # neither reserved, should work
            ("name2", "name", True),  # neither reserved, should work
        ]

        for i, (name_field, prop_name, should_pass) in enumerate(test_cases):
            with self.subTest(
                f"config:{name_field} using:{prop_name} expect:{'pass' if should_pass else 'fail'}"
            ):
                config = MogwaiGraphConfig(name_field=name_field)
                g = MogwaiGraph(config=config)
                props = {prop_name: f"value{i}"}
                if should_pass:
                    node_id = g.add_labeled_node("TestLabel", "test", properties=props)
                    self.assertEqual(g.nodes[node_id][prop_name], f"value{i}")
                else:
                    with self.assertRaises(MogwaiGraphError):
                        g.add_labeled_node("TestLabel", "test", properties=props)

    def test_spog_index_issue13(self):
        """
        Test the index handling.
        """
        g = MogwaiGraph.modern()
        ps_lookup = g.spog_index.get_lookup("P", "S")
        self.assertIsNotNone(ps_lookup)

        debug=self.debug
        debug=True
        if debug:
            for index_name in g.spog_index.config.active_indices:
                index = g.spog_index.indices.get(index_name)
                if index:
                    print(f"Index: {index_name}")
                    print(json.dumps(index.lookup,indent=2,default=str))

        # Check that `label` in `PS` has references to all nodes
        self.assertEqual(ps_lookup.get("label"), {0, 1, 2, 3, 4, 5})

        # Check that specific labels and names exist in `PO`
        po_lookup = g.spog_index.get_lookup("P", "O")
        self.assertEqual(po_lookup.get("label"), {"{'Person'}", "{'Software'}"})
        self.assertEqual(po_lookup.get("name"), {"marko", "vadas", "josh", "peter", "lop", "ripple"})

        # Check `OS` for expected mappings
        os_lookup = g.spog_index.get_lookup("O", "S")
        self.assertEqual(os_lookup.get("{'Person'}"), {0, 1, 3, 5})
        self.assertEqual(os_lookup.get("{'Software'}"), {2, 4})
        self.assertEqual(os_lookup.get("java"), {2, 4})

        # Check `SP` for node attributes
        sp_lookup = g.spog_index.get_lookup("S", "P")
        self.assertEqual(sp_lookup.get(0), {"label", "name", "age"})
        self.assertEqual(sp_lookup.get(2), {"label", "name", "lang"})