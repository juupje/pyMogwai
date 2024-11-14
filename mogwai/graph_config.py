"""
Created on 2024-08-17

@author: wf
"""

from dataclasses import field
from typing import Any, Callable, Dict, Optional

from mogwai.lod.yamlable import lod_storable


@lod_storable
class GraphConfig:
    """
    Configuration for a graph in the Examples class
    """

    name: str
    file_path: Optional[str] = None
    is_default: bool = False
    node_label_key: str = "labelV"
    edge_label_key: str = "labelE"
    node_name_key: Optional[str] = None
    custom_loader: Optional[str] = None  # Changed to str to store function name

    def get_node_name_key(self) -> Callable[[Dict[str, Any]], Any]:
        if self.node_name_key is None:
            return lambda x: x  # Return identity function if no key specified
        elif isinstance(self.node_name_key, str):
            return lambda x: x.pop(self.node_name_key, None)
        else:
            raise ValueError(f"Invalid node_name_key for graph {self.name}")


@lod_storable
class GraphConfigs:
    """Manages a collection of GraphConfig instances"""

    configs: Dict[str, GraphConfig] = field(default_factory=dict)
