"""
Created on 2024-10-21

@author: wf
"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import networkx as nx
from dacite import from_dict
from mogwai.core import MogwaiGraph
from ngwidgets.dict_edit import DictEdit, FieldUiDef, FormUiDef
from ngwidgets.lod_grid import ListOfDictsGrid
from ngwidgets.webserver import WebSolution
from ngwidgets.widgets import Link
from nicegui import background_tasks, ui

from mogwai.schema.graph_schema import GraphSchema, NodeTypeConfig


@dataclass
class NodeViewConfig:
    """
    parameters for the node views
    """

    solution: WebSolution
    graph: MogwaiGraph
    schema: GraphSchema
    node_type: str
    node_type_config: NodeTypeConfig = field(init=False)

    def __post_init__(self):
        self.node_type_config = self.schema.node_type_configs.get(self.node_type)


class BaseNodeView:
    """
    Base class for viewing and interacting with nodes in a graph.
    """

    def __init__(self, config: NodeViewConfig):
        """
        Base constructor for initializing the NodeView.

        Args:
        """
        self.solution = config.solution
        self.graph = config.graph
        self.schema = config.schema
        self.node_type = config.node_type
        self.node_type_config = config.node_type_config
        self.node_data_class = config.node_type_config._dataclass
        self.key = config.node_type_config.key_field

    def editable_properties(self, props: dict[str, Any]) -> dict[str, Any]:
        """
        Filter the properties to exclude hidden keys (those starting with '_') and non-string iterables.

        Args:
            props (dict[str, Any]): The dictionary of properties to filter.

        Returns:
            dict[str, Any]: The filtered properties dictionary.
        """
        editable_props = {}
        if props:
            for key, value in props.items():
                if not key.startswith("_") and (
                    not isinstance(value, Iterable) or isinstance(value, str)
                ):
                    editable_props[key] = value
        return editable_props


class NodeView(BaseNodeView):
    """
    A view for displaying and editing a single node of a NetworkX graph.
    """

    def __init__(self, config: NodeViewConfig, node_id: Any):
        """
        Construct the NodeView with the given configuration and node ID.

        Args:
            config (NodeViewConfig): The configuration dataclass for the view.
            node_id (Any): The identifier of the node to view/edit.
        """
        super().__init__(config)
        self.node_id = node_id
        self.dict_edit = None
        node_id = self.node_id
        # type coercion
        node_id_type = self.schema.node_id_type
        if not isinstance(node_id, node_id_type):
            self.node_id = node_id_type(node_id)

        self.node_data = self.graph.nodes.get(self.node_id)
        pass

    def setup_ui(self):
        try:
            self.get_dict_edit()
        except Exception as ex:
            self.solution.handle_exception(ex)

    def open(self):
        """
        Show the details of the dict edit
        """
        if self.dict_edit:
            self.dict_edit.expansion.open()

    def close(self):
        """
        Hide the details of the dict edit
        """
        if self.dict_edit:
            self.dict_edit.expansion.close()

    def get_dict_edit(self) -> DictEdit:
        """
        Return a DictEdit instance for editing node attributes.
        """
        # Initialize edit_props and ui_fields together
        edit_props = self.editable_properties(self.node_data)
        ui_fields = {}
        for key, value in edit_props.items():
            field_uidef = FieldUiDef.from_key_value(key, value)
            ui_fields[key] = field_uidef
        # Use get_node_config from GraphSchema
        node_config = self.schema.get_node_config(self.node_data)
        if node_config:
            key_value = edit_props.get(node_config.key_field)
            key_str = f" {key_value}" if key_value else ""
            title = f"{node_config.label}{key_str}"
            icon = node_config.icon
        else:
            title = f"Node: {self.node_id}"
            icon = "account_tree"  # Default icon
        # Define a custom form definition for the title "Node Attributes"
        form_ui_def = FormUiDef(title=f"{title}", icon=icon, ui_fields=ui_fields)

        self.dict_edit = DictEdit(data_to_edit=edit_props, form_ui_def=form_ui_def)
        self.open()
        return self.dict_edit

    def update_node(self, updated_data: dict):
        """
        Update the node in the graph with the edited data

        Args:
            updated_data (dict): The updated node attributes
        """
        nx.set_node_attributes(self.graph, {self.node_id: updated_data})


class NodeTableView(BaseNodeView):
    """
    A view for displaying and interacting with nodes of the same type in a MogwaiGraph.
    """

    def __init__(self, config: NodeViewConfig):
        """
        Initialize the NodeTableView.

        Args:
            config (NodeViewConfig): The configuration dataclass for the view.
        """
        super().__init__(config)
        self.lod_grid = None
        self.node_view = None
        self.log = config.solution.log

    def setup_ui(self):
        """
        Set up the user interface for the NodeTableView
        """
        with ui.column().classes("w-full"):
            msg = f"loading {self.node_type}s ..."
            self.status_label = ui.label(msg).classes("text-h5")
            self.grid_container = ui.row().classes("w-full")
            self.node_view_container = ui.row().classes("w-full")

        with self.status_label:
            ui.spinner(size="sm")
        self.grid_container = ui.row().classes("w-full")
        self.node_view_container = ui.row().classes("w-full")

        # Start load in background
        background_tasks.create(self.load_and_show_nodes())

    async def load_and_show_nodes(self):
        """
        Load nodes in background and update UI
        """
        try:
            nodes_lod = self.get_lod_of_nodes(node_label=self.node_type)
            self.node_items = {}
            self.node_ids = {}
            view_lod = []
            for record in nodes_lod:
                key_value = record.get(self.key)
                item = None
                try:
                    item = from_dict(data_class=self.node_data_class, data=record)
                    self.node_items[key_value] = item
                except Exception as ex:
                    self.log.log("❌", "from_dict", f"{key_value}:{str(ex)}")
                    pass
                node_id = record.get("node_id")
                self.node_ids[key_value] = node_id
                if item and hasattr(item, "as_view_dict"):
                    view_dict = item.as_view_dict()
                else:
                    view_dict = record
                node_link = Link.create(
                    f"/node/{self.node_type}/{node_id}", str(key_value)
                )
                view_dict[self.key] = node_link
                view_lod.append(view_dict)
            self.grid_container.clear()
            with self.grid_container:
                self.lod_grid = ListOfDictsGrid()
                self.lod_grid.load_lod(view_lod)
                # self.lod_grid.ag_grid.options["rowSelection"] = "single"
                # self.lod_grid.ag_grid.on("rowSelected", self.on_row_selected)
            with self.status_label:
                self.status_label.clear()
                msg = f"{len(view_lod)} {self.node_type}s"
                self.status_label.text = msg
        except Exception as ex:
            self.solution.handle_exception(ex)

    def get_lod_of_nodes(self, node_label: str):
        """
        Retrieve a list of dictionaries containing the properties of nodes with the given node_label from the graph.

        Args:
            node_label (str): The label of the nodes to retrieve.

        Returns:
            list: A list of dictionaries containing the properties of the matching nodes, with 'id' included.
        """
        lod = []
        for node_id, node in self.graph.nodes(data=True):
            labels = node.get("labels", set())
            if node_label in labels:
                props = self.editable_properties(node)
                props["node_id"] = node_id
                lod.append(props)
        return lod
