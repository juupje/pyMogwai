import os

from mogwai.core.mogwaigraph import MogwaiGraph


def get_file_stats(file) -> dict:
    res = os.stat(file)
    stats = {
        "filesize": res.st_size,
        "full_path": os.path.abspath(file),
        "modified": res.st_mtime,
        "accessed": res.st_atime,
    }
    try:
        stats["created"] = res.st_birthtime
    except:
        pass
    return stats


def get_subgraph(file: str) -> MogwaiGraph:
    if file.endswith(".pdf"):
        from mogwai.parser.pdfgraph import PDFGraph

        return PDFGraph(file)
    elif file.endswith(".pptx"):
        from mogwai.parser.powerpoint_converter import PPGraph

        return PPGraph(file)
    elif file.endswith(".xlsx"):
        from mogwai.parser.excel_converter import EXCELGraph

        return EXCELGraph(file)
    else:
        raise ValueError("Wrong Format, cant get a subgraph")


IGNORED_DIRECTORIES = [".git", "__pycache__"]
FILETYPES_TO_UNFOLD = (".pdf", ".pptx", ".xlsx")


class FileSystemGraph(MogwaiGraph):

    def __init__(self, root_path):
        super().__init__()
        self.root_path = root_path
        self.extract_graph(self.root_path)
        # self._extract_graph_structure()

    def extract_graph(self, current_path, parent_node=None):
        if not os.path.exists(current_path):
            raise ValueError(f"Path {current_path} does not exist.")
        if os.path.isfile(current_path):
            if current_path.endswith(FILETYPES_TO_UNFOLD):
                if parent_node is None:
                    raise ValueError("A filesystem graph can not be just one file")
                subgraph = get_subgraph(current_path)
                self.merge_subgraph(subgraph, parent_node, subgraph.root, "HAS_FILE")
            else:
                node = self.add_labeled_node(
                    name=os.path.basename(current_path),
                    **get_file_stats(current_path),
                    label="File",
                )
                if parent_node is not None:
                    self.add_labeled_edge(parent_node, node, "HAS_FILE")

        elif os.path.isdir(current_path):
            current_node = self.add_labeled_node(
                name=os.path.basename(current_path), label="Directory"
            )
            if parent_node is not None:
                self.add_labeled_edge(parent_node, current_node, "HAS_FOLDER")
            for item in os.listdir(current_path):
                if not item in IGNORED_DIRECTORIES:
                    item_path = os.path.join(current_path, item)
                    self.extract_graph(item_path, current_node)
