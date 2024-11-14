"""
Created on 2024-11-14

@author: wf
"""

import os

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
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Set yaml_path to the "mogwai_examples" directory located in the parent directory of the script's location
        json_path = os.path.join(
            script_dir, "..", "mogwai_examples", "royal-family.jsons"
        )
