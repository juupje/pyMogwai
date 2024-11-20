"""
Created on 2021-08-19

@author: wf
"""

import getpass
import os
import unittest

from mogwai.graphs import Graphs
from tests.profiler import Profiler


class BaseTest(unittest.TestCase):
    """
    mogwai base test case
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.examples = Graphs()

    def setUp(self, debug=False, profile=True):
        """
        setUp test environment
        """
        unittest.TestCase.setUp(self)
        self.debug = debug
        self.profile = profile
        msg = f"test {self._testMethodName}, debug={self.debug}"
        self.profiler = Profiler(msg, profile=self.profile)
        self.script_dir = os.path.dirname(__file__)
        self.documents_path = os.path.join(self.script_dir, "documents")
        self.examples_path = os.path.join(
            os.path.dirname(self.script_dir), "mogwai_examples"
        )
        self.root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.profiler.time()

    @staticmethod
    def inPublicCI():
        """
        are we running in a public Continuous Integration Environment?
        """
        publicCI = getpass.getuser() in ["travis", "runner"]
        jenkins = "JENKINS_HOME" in os.environ
        return publicCI or jenkins

    @staticmethod
    def isUser(name: str):
        """Checks if the system has the given name"""
        return getpass.getuser() == name


if __name__ == "__main__":
    unittest.main()
