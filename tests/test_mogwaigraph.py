import os

from mogwai.core.exceptions import MogwaiGraphError
from mogwai.core.mogwaigraph import MogwaiGraph

from .basetest import BaseTest


class TestNetworkX(BaseTest):
    def setUp(self):
        super().setUp()
        self.G = MogwaiGraph()
        print("Graph intitialized")

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
