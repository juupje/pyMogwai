import os

from ngwidgets.basetest import Basetest

from mogwai.graphs import Graphs


class BaseTest(Basetest):
    """
    specialized Basetest for mogwai package
    """

    @classmethod
    def setUpClass(cls) -> None:
        super(BaseTest, cls).setUpClass()
        cls.examples = Graphs()

    def setUp(self, debug=True, profile=True):
        Basetest.setUp(self, debug=debug, profile=profile)
        self.script_dir = os.path.dirname(__file__)
        self.documents_path = os.path.join(self.script_dir, "documents")
        self.examples_path = os.path.join(
            os.path.dirname(self.script_dir), "mogwai_examples"
        )
        self.root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
