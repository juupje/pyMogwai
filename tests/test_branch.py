import mogwai.core.traversal as Trav
from mogwai.core import MogwaiGraph
from mogwai.parser.filesystem import FileSystemGraph as FSG

from .basetest import BaseTest


class TestBranch(BaseTest):
    """
    test the traverser
    """

    def setUp(self):
        super().setUp()
        self.file_system = FSG(self.root_path)
        self.modern = MogwaiGraph.modern()

    # test branch(hasname("josh"),option(has("property")))
    def test_branch(self):
        g = Trav.MogwaiGraphTraversalSource(self.modern)
        print(g.connector.nodes(data=True))
        print(g.connector.edges())
        from mogwai.core.steps import map_steps as ms

        assertlist = ["ripple", "vadas", "josh", "peter", "lop", 29]
        query = (
            g.V()
            .branch(ms.name())
            .option("marko", ms.values(["age"]))
            .option(None, ms.name())
            .to_list()
        )
        print("Query:", query.print_query())
        reslist = query.run()
        print("Result:", reslist)
        for i in range(len(reslist)):
            if reslist[i] in assertlist:
                assertlist.remove(reslist[i])
            else:
                raise ValueError(
                    "Found Value "
                    + str(reslist[i])
                    + " which is not in the searched for list"
                )
        if assertlist != list():
            raise ValueError("Values: " + str(assertlist) + "not found")
