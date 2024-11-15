"""
Created on 2024-10-22

@author: wf
"""

import builtins
import importlib
import logging
import os
from dataclasses import field
from pathlib import Path
from typing import Dict, List, Optional, Type

from mogwai.core import MogwaiGraph
from mogwai.lod.yamlable import lod_storable


@lod_storable
class NodeTypeConfig:
    """Configuration for a node type in the graph"""

    label: str  # Label used in the graph database
    # https://fonts.google.com/icons?icon.set=Material+Icons
    icon: str  # Material icon name
    key_field: str  # Primary identifier field
    dataclass_name: (
        str  # module.class name string for the dataclass to be used for this node type
    )
    display_name: str  # Human-readable name for UI
    display_order: int = 1000  # order for display - default to high number
    viewclass_name: Optional[str] = (
        None  # module.class name string for the viewclass to be used for this node type
    )
    description: Optional[str] = None
    _dataclass: Type = field(init=False)
    _viewclass: Type = field(init=False)

    def get_class(self, class_name_attr: str) -> None:
        """
        retrievw a class from its module path string.

        Args:
            class_name_attr: The attribute name containing the class path string

        Raises:
            ValueError: If the class initialization fails
        """
        class_path = getattr(self, class_name_attr)
        if not class_path:
            return None
        try:
            module_path, class_name = class_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except Exception as ex:
            raise ValueError(f"Invalid {class_name_attr}: {class_path}: {str(ex)}")

    def __post_init__(self):
        """Initialize the dataclass and view class types"""
        class_configs = [
            ("dataclass_name", "_dataclass"),
            ("viewclass_name", "_viewclass"),
        ]
        for class_name_attr, target_attr in class_configs:
            clazz = self.get_class(class_name_attr)
            setattr(self, target_attr, clazz)

    def as_view_dict(self) -> Dict:
        view_dict = {"description": self.description, "icon": self.icon}
        return view_dict


@lod_storable
class GraphSchema:
    """registry of node types and their configurations"""

    base_uri: Optional[str] = "http://example.org"
    node_id_type_name: Type = int
    node_type_configs: Dict[str, NodeTypeConfig] = field(default_factory=dict)

    @property
    def node_id_type(self) -> Type:
        """
        Property to convert the node_id_type_name to an actual Python type.
        """
        return getattr(builtins, self.node_id_type_name)

    def get_node_config(self, node_data: dict) -> NodeTypeConfig | None:
        """
        Get the NodeTypeConfig for the node based on its labels.

        Args:
            node_data (dict): The data of the node containing labels

        Returns:
            NodeTypeConfig or None: The NodeTypeConfig for the given node if found, otherwise None.
        """
        if node_data:
            labels = node_data.get("labels", set())
            if labels:
                node_label = next(iter(labels))  # Get the first label
            return self.node_type_configs.get(node_label)
        return None

    def add_to_graph(self, graph: MogwaiGraph):
        """
        add my node type configurations to the given graph

        Args:
            graph(MogwaiGraph): the graph to add the configurations to
        """
        for node_type in self.node_type_configs.values():
            props = node_type.__dict__.copy()
            graph.add_labeled_node(
                "NodeTypeConfig", name=node_type.label, properties=props
            )

    @classmethod
    def yaml_path(cls) -> str:
        """Default path for schema YAML file"""
        module_path = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.join(module_path, "resources", "schema.yaml")
        return yaml_path

    @classmethod
    def load(cls, yaml_path: str = None) -> "GraphSchema":
        """
        Load schema from a YAML file, ensuring the file exists.

        Args:
            yaml_path: Optional path to YAML file. If None, uses default path.

        Returns:
            GraphSchema: Loaded schema or empty schema if file doesn't exist
        """
        if yaml_path is None:
            yaml_path = cls.yaml_path()

        if not Path(yaml_path).exists():
            err_msg = f"Schema YAML file not found: {yaml_path}"
            logging.error(err_msg)
            return cls()

        return cls.load_from_yaml_file(yaml_path)
