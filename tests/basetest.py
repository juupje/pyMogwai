import os
from unittest import TestCase


class BaseTest(TestCase):
    def setUp(self):
        self.documents_path = os.path.join(os.path.dirname(__file__), "documents")
        self.root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
