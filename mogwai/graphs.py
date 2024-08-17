"""
Created on 2024-08-17

@author: wf
"""

import logging
import os
from typing import Dict, List

from mogwai.core import MogwaiGraph
from mogwai.graph_config import GraphConfig, GraphConfigs
from mogwai.parser.graphml_converter import graphml_to_mogwaigraph


class Graphs:
    """
    Manage MogwaiGraphs
    """

    def __init__(
        self, config_file: str = None, lazy: bool = False, debug: bool = False
    ):
        self.debug = debug
        self.logger = self.get_logger()
        self.examples_dir = os.path.join(
            os.path.dirname(__file__), "..", "mogwai_examples"
        )
        if config_file is None:
            config_file = os.path.join(self.examples_dir, "example_graph_configs.yaml")
        self.config_file = config_file
        self.graphs: Dict[str, MogwaiGraph] = {}

        self.log(f"Loading configurations from: {self.config_file}")
        self.configs = GraphConfigs.load_from_yaml_file(self.config_file)
        self.log(f"Loaded configurations: {self.configs.configs}")

        if not lazy:
            self.load_examples()

    def get_logger(self):
        return logging.getLogger(self.__class__.__name__)

    def log(self, msg: str):
        if self.debug:
            self.logger.debug(msg)

    def load_examples(self):
        """Load all example graphs based on configurations"""
        self.log("Loading default examples")
        for name, config in self.configs.configs.items():
            if config.is_default:
                self.log(f"Loading default graph: {name}")
                self.get(name)  # This will load the graph using the existing get method

    def _load_graph(self, file_path: str, config: GraphConfig) -> MogwaiGraph:
        """Load a single graph from a .graphml file using the provided configuration"""
        self.log(f"Loading graph from file: {file_path}")
        return graphml_to_mogwaigraph(
            file_path,
            node_label_key=config.node_label_key,
            edge_label_key=config.edge_label_key,
            node_name_key=config.get_node_name_key(),
        )

    def get_names(self) -> List[str]:
        """Get a list of available graph names"""
        names = list(self.configs.configs.keys())
        self.log(f"Available graph names: {names}")
        return names

    def get(self, name: str) -> MogwaiGraph:
        """Get a graph by name, loading it if necessary"""
        if name not in self.configs.configs:
            error_msg = f"Graph '{name}' not found in configurations"
            self.log(error_msg)
            raise ValueError(error_msg)

        if name not in self.graphs:
            config = self.configs.configs[name]
            if config.custom_loader:
                self.log(f"Using custom loader for graph '{name}'")
                # Assuming custom_loader is a string representing a method name in MogwaiGraph
                loader = getattr(MogwaiGraph, config.custom_loader, None)
                if loader and callable(loader):
                    self.graphs[name] = loader()
                else:
                    error_msg = f"Invalid custom loader {config.custom_loader} for graph '{name}'"
                    self.log(error_msg)
                    raise ValueError(error_msg)
            elif config.file_path:
                file_path = os.path.join(self.examples_dir, config.file_path)
                self.log(f"Loading graph '{name}' from file: {file_path}")
                self.graphs[name] = self._load_graph(file_path, config)
            else:
                error_msg = f"No loader or file path specified for graph '{name}'"
                self.log(error_msg)
                raise ValueError(error_msg)

        return self.graphs[name]


# Usage example:
if __name__ == "__main__":
    examples = Graphs(debug=True)
    print("Available graphs:", examples.get_names())

    try:
        graph_name = "air-routes-latest"
        graph = examples.get(graph_name)
        print(
            f"Loaded graph '{graph_name}': {len(graph.nodes)} nodes, {len(graph.edges)} edges"
        )
    except Exception as e:
        print(f"Failed to load graph: {str(e)}")
