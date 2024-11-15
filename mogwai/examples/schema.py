"""
Created on 15.11.2024

@author: wf
"""

import os


class MogwaiExampleSchema:
    """
    the mogwai examples schema
    """

    @classmethod
    def get_yaml_path(cls) -> str:
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Set yaml_path to the "mogwai_examples" directory located in the parent directory of the script's location
        yaml_path = os.path.join(
            script_dir, "../..", "mogwai_examples", "modern-schema.yaml"
        )

        # Normalize the path to remove any redundant components
        yaml_path = os.path.normpath(yaml_path)
        return yaml_path
