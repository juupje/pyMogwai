import os

from mogwai.parser.filesystem import FileSystemGraph as FSG

from .basetest import BaseTest


class TestFileSystem(BaseTest):
    def setUp(self):
        super().setUp()
        self.file_system = FSG(self.root_path)

    def test_readme(self):
        readme_nodes = self.file_system.get_nodes(
            label="File", name="README.md"
        )  # list of tuples (id:int, node:dict)
        self.assertTrue(len(readme_nodes) == 1, "Incorrect number of readme's")

        readme_node = readme_nodes[0]  # select the first tuple
        print("Readme node:", type(readme_node))
        properties = readme_node[1]["properties"]
        self.assertTrue(
            "File" in readme_node[1]["labels"],
            "Incorrect or missing label for README.md",
        )
        self.assertTrue("filesize" in properties, "No filesize property")
        sizes = [2084, 2172]
        self.assertTrue(properties["filesize"] in sizes, "Filesize is not correct")
        self.file_system.draw(
            os.path.join(self.root_path, "tests", "filesystem_test.svg"), prog="dot"
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
