from dataclasses import dataclass
from tabulate import tabulate
from typing import Optional, List, Set, Dict
from logging import log

@dataclass
class SectionFormat:
    level: int = 1
    format_str: str = "{header}"

    def format_section_header(self, header: str) -> str:
        """
        format a given header with my format string
        """
        text= self.format_str.format(header=header)
        return text


class GraphSummary:
    """A class to generate formatted summaries of graph structures."""

    def __init__(
        self,
        graph,
        fmt: str = "github",
        section_formats: Optional[Dict[str, List[SectionFormat]]] = None,
    ):
        """ constructor å"""
        self.graph = graph
        self.fmt = fmt
        self.section_formats = section_formats or {
            "github": [
                SectionFormat(1, "# {header}")],
            "mediawiki": [SectionFormat(1, "= {header} =")],
            "latex": [SectionFormat(1, "\\section{{{header}}}")],
        }


    def summary(self,limit: int = 3) -> str:
        """Generate a summary of the graph."""
        summary=self.dump(limit=limit)
        return summary

    def format_section_header(self, header: str, level: int = 1) -> str:
        """Format the section header using the format string for the current output format."""
        text=header
        if self.fmt in self.section_formats:
            section_formats = self.section_formats.get(self.fmt)
            if level<len(section_formats):
                section_format=section_formats[level]
                text= section_format.format_section_header(header)
        return text


    def dump(self, node_types=None, limit: int = 10) -> str:
        """
        Dump the content of the graph for investigation.

        Args:
            node_types (list): List of node types to dump. If None, dump all types.
            limit (int): Maximum number of nodes to dump for each node type. Default is 10.

        Returns:
            str: Formatted string containing the graph summary, nodes, and edges.
        """
        output = []
        output.append(self.format_section_header("Graph Summary"))
        output.append(self._get_graph_summary())

        node_types = self._get_node_types_to_dump(node_types)
        output.append(self.format_section_header("Nodes"))
        output.append(self._get_nodes_summary(node_types, limit))

        output.append(self.format_section_header("Edges"))
        output.append(self._get_edges_summary(limit))

        return "\n\n".join(filter(None, output))

    def _get_graph_summary(self) -> str:
        summary_data = [
            ["Total Nodes", len(self.graph.nodes)],
            ["Total Edges", len(self.graph.edges)],
            ["Node Types", ", ".join(self._get_all_node_types())],
        ]
        markup= tabulate(summary_data, headers=["Metric", "Value"], tablefmt=self.fmt)
        return markup

    def _get_all_node_types(self) -> Set[str]:
        return {
            data.get("type", "Unknown")
            for _, data in self.graph.nodes(data=True)
        }

    def _get_node_types_to_dump(self, requested_types) -> Set[str]:
        all_node_types = self._get_all_node_types()
        if requested_types is None:
            return all_node_types
        return set(requested_types) & all_node_types

    def _get_nodes_summary(self, node_types, limit) -> str:
        output = []
        for node_type in node_types:
            output.append(self.format_section_header(f"Node Type: {node_type}", level=2))
            output.append(self._get_nodes_of_type_summary(node_type, limit))
        return "\n".join(filter(None, output))

    def _get_nodes_of_type_summary(self, node_type, limit) -> str:
        rows = []
        for node, data in self.graph.nodes(data=True):
            if data.get("type") == node_type:
                rows.append([node] + [f"{k}: {v}" for k, v in data.items() if k != "type"])
                if len(rows) >= limit:
                    break
        if rows:
            table = tabulate(rows, headers=["Node", "Details"], tablefmt=self.fmt)
            if len(rows) == limit:
                remaining = sum(
                    1
                    for _, d in self.graph.nodes(data=True)
                    if d.get("type") == node_type
                ) - limit
                if remaining > 0:
                    table += f"\n... and {remaining} more"
            return table
        return "No nodes found for this type."

    def _get_edges_summary(self, limit) -> str:
        rows = []
        for i, (u, v, data) in enumerate(self.graph.edges(data=True)):
            rows.append([f"{u} -> {v}"] + [f"{k}: {v}" for k, v in data.items()])
            if len(rows) >= limit:
                break
        if rows:
            table = tabulate(rows, headers=["Edge", "Details"], tablefmt=self.fmt)
            if len(rows) == limit:
                remaining = len(self.graph.edges) - limit
                if remaining > 0:
                    table += f"\n... and {remaining} more"
            return table
        return "No edges in the graph."
