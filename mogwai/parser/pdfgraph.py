import os

from mogwai.core import MogwaiGraph

from .filesystem import get_file_stats


class PDFGraph(MogwaiGraph):
    LABEL = "PDFFile"

    def __init__(self, file: str = None, name: str = None, **kwargs):
        super().__init__(**kwargs)
        if file is not None:
            if not os.path.exists(file):
                raise FileNotFoundError(f"No such file or directory: '{file:s}'")
            self.file = file
            name = name or os.path.basename(file)
            self.root = self.add_labeled_node(
                label=PDFGraph.LABEL, name=name, **get_file_stats(self.file)
            )
            self.construct()

    def construct(self):
        from pypdf import PdfReader

        reader = PdfReader(self.file)
        meta = reader.metadata
        meta_dict = {
            "producer": meta.producer,
            "creator": meta.creator,
            "subject": meta.subject,
            "title": meta.title,
        }
        self.nodes[self.root].update(
            {"metadata": meta_dict, "number_of_pages": len(reader.pages)}
        )
        titles = []

        def collect_titles(obj):
            for child in obj:
                if isinstance(child, dict):
                    titles.append(
                        (child["/Title"], reader.get_page_number(child["/Page"]) + 1)
                    )
                else:
                    collect_titles(child)

        collect_titles(reader.outline)
        for title in titles:
            node = self.add_labeled_node(
                label="PDFTitle", name=title[0], page_number=title[1]
            )
            self.add_labeled_edge(self.root, node, "HAS_CONTENT")
