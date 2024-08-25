import os

from mogwai.parser.filesystem import FileSystemGraph as FSG

from .basetest import BaseTest


class TestFileSystem(BaseTest):
    """
    test filesystem handling
    """

    def setUp(self, debug=True, profile=True):
        BaseTest.setUp(self, debug=debug, profile=profile)
        self.file_system = FSG(self.root_path)

    def test_readme(self):
        """
        test README.md in Filesystem access
        """
        readme_nodes = self.file_system.get_nodes(
            label="File", name="README.md"
        )  # list of tuples (id:int, node:dict)
        self.assertTrue(len(readme_nodes) == 1, "Incorrect number of readme's")

        readme_node = readme_nodes[0]  # select the first tuple
        print("Readme node:", type(readme_node))
        properties = readme_node[1]
        self.assertTrue(
            "File" in readme_node[1]["labels"],
            "Incorrect or missing label for README.md",
        )
        self.assertTrue("filesize" in properties, "No filesize property")
        if self.debug:
            print(properties["filesize"])
        self.assertTrue(properties["filesize"] > 2000, "Filesize is not correct")
        if not self.inPublicCI():
            self.file_system.draw(
                os.path.join(self.root_path, "tests", "filesystem_test.svg"), prog="dot"
            )
