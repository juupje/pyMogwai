"""
Created on 2024-08-15

@author: wf
"""

import sys
from argparse import ArgumentParser

from ngwidgets.cmd import WebserverCmd

from mogwai.web.server import MogwaiWebServer


class MogwaiCmd(WebserverCmd):
    """
    command line handling for nicesprinkler
    """

    def __init__(self):
        """
        constructor
        """
        config = MogwaiWebServer.get_config()
        WebserverCmd.__init__(self, config, MogwaiWebServer, DEBUG)

    def getArgParser(self, description: str, version_msg) -> ArgumentParser:
        """
        override the default argparser call
        """
        parser = super().getArgParser(description, version_msg)

        return parser


def main(argv: list = None):
    """
    main call
    """
    cmd = MogwaiCmd()
    exit_code = cmd.cmd_main(argv)
    return exit_code


DEBUG = 0
if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("-d")
    sys.exit(main())
