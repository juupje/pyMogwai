"""
Created on 2024-11-14

@author: wf
"""

from mogwai.parser.json_converter import JsonGraph
from tests.basetest import BaseTest


class TestJsonConverter(BaseTest):
    """
    test json conversion
    """

    def test_json_converter(self):
        """
        test converting a sample json file to a graph
        """
