import os

from ngwidgets.basetest import Basetest


class BaseTest(Basetest):
    """
    specialized Basetest for mogwai package
    """

    def setUp(self, debug=True, profile=True):
        Basetest.setUp(self, debug=debug, profile=profile)
        self.documents_path = os.path.join(os.path.dirname(__file__), "documents")
        self.root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
