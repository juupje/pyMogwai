import logging
import os

from slides.slidewalker import PPT

from mogwai.core import MogwaiGraph

from .filesystem import get_file_stats

logger = logging.getLogger("Mogwai")


def ppt_to_dic(filepath: str):
    ppt = PPT(filepath)
    ppt.open()
    if ppt.error:
        logger.error("ppt to json failed")
        return {}
    ppt.getSlides()
    ppt_dic = ppt.asDict()
    ppt_dic["slides"] = [slide.asDict() for slide in ppt.slides]
    return ppt_dic


class PPGraph(MogwaiGraph):
    LABEL = "PPFile"

    def __init__(self, file: str = None, name: str = None, **kwargs):
        super().__init__(**kwargs)
        if file is not None:
            if not os.path.exists(file):
                raise FileNotFoundError(f"No such file or directory: '{file:s}'")
            self.file = file
            name = name or os.path.basename(file)
            self.root = self.add_labeled_node(
                label=PPGraph.LABEL, name=name, **get_file_stats(self.file)
            )
            self.construct()

    def construct(self):
        dic = ppt_to_dic(self.file)
        meta_dict = {
            "titel": dic["title"],
            "author": dic["author"],
            "created": dic["created"],
        }
        self.nodes[self.root].update(
            {"metadata": meta_dict, "number_of_slides": len(dic["slides"])}
        )

        for slide in dic["slides"]:
            if "name" in slide:
                name = slide["name"]
                if len(name) == 0:
                    name = "page" + str(slide["page"])
                del slide["name"]
            else:
                name = "page" + str(slide["page"])
            node = self.add_labeled_node(label="PPPage", name=name, **slide)
            self.add_labeled_edge(self.root, node, "HAS_PAGE")
