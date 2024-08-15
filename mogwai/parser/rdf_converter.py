from rdflib import Graph

def rdf_to_dict (path) -> dict:
    g = Graph()
    g.parse(path, format="xml")
    dic = {}
    for s, p, o in g:
        if s not in dic:
            dic[s] = {}
        if p not in dic[s]:
            dic[s][p] = []
        dic[s][p].append(o)
    return dic
