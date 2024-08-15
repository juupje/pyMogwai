# PyMogwai
PyMogwai is a Python-based implementation of the Gremlin graph traversal language, designed to create and handle knowledge graphs entirely in Python without the need for an external Gremlin server.

[![pypi](https://img.shields.io/pypi/pyversions/pyMogwai)](https://pypi.org/project/pyMogwai/)
[![Github Actions Build](https://github.com/juupje/pyMogwai/actions/workflows/build.yml/badge.svg)](https://github.com/juupje/pyMogwai/actions/workflows/build.yml)
[![PyPI Status](https://img.shields.io/pypi/v/pyMogwai.svg)](https://pypi.python.org/pypi/pyMogwai/)
[![GitHub issues](https://img.shields.io/github/issues/juupje/pyMogwai.svg)](https://github.com/juupje/pyMogwai/issues)
[![GitHub closed issues](https://img.shields.io/github/issues-closed/juupje/pyMogwai.svg)](https://github.com/juupje/pyMogwai/issues/?q=is%3Aissue+is%3Aclosed)
[![API Docs](https://img.shields.io/badge/API-Documentation-blue)](https://juupje.github.io/pyMogwai/)
[![License](https://img.shields.io/github/license/juupje/pyMogwai.svg)](https://www.apache.org/licenses/LICENSE-2.0)

## Features
* Supports knowledge graph creation and manipulation
* Supports the import of arbitrary knowledge graphs with GraphML
* Implements a variety of traversal steps
* Enables one to traverse a graph with these steps
* Ability to integrate data from various sources like Excel, PDF and PowerPoint
* Simple and Pythonic API for graph operations

## Demo
[nicegui based demo](https://mogwai.bitplan.com/)


## Getting started

### Creating a Knowledge Graph
To create a graph using PyMogwai
```python
from mogwai.core import MogwaiGraph
graph = MogwaiGraph()

# Add nodes and edges
n1 = graph.add_labeled_node("person", name="Alice", properties={"Age": 30})
n2 = graph.add_labeled_node("person", name="Bob", properties={"Age": 28})
graph.add_labeled_edge(n1, n2, "knows")
```
### Import graphs
```python
from mogwai.parser.graphml_converter import graphml_to_mogwaigraph

graph = graphml_to_mogwaigraph(path, node_label_key="node_label", node_name_key="node_name", edge_label_key="edge_label")
```

### Performing Traversals
To perform any traversal of the graph, create a TraversalSource. Traversals start with a start step (`V()` or `E()`), which is followed by a sequence of steps. Note that a traversal can be executed by calling `.run()` on it.


```python
from mogwai.core.traversal import MogwaiGraphTraversalSource

g = MogwaiGraphTraversalSource(graph)

# Example traversal that returns every person in the graph as a list
res = g.V().has_label("person").to_list().run()
print(res)
```

In order to use anonymous traversal in complex queries, import the statics module:

```python
from mogwai.core.traversal import MogwaiGraphTraversalSource
from mogwai.core.steps.statics import *

g = MogwaiGraphTraversalSource(graph)

# Example traversal that returns every person in the graph as a list
query = g.V().has_label("person").filter_(properties('age').is_(gte(30))).to_list().by('name')
res = query.run()
print(res)
```

# History
This project started as part of the  [RWTH Aachen i5 Knowledge Graph Lab SS2024](https://dbis.rwth-aachen.de/dbis/index.php/2023/knowledge-graph-lab-ss-2024/)
The original source is hosted at https://git.rwth-aachen.de/i5/teaching/kglab/ss2024/pymogwai
2024-08-15 the repository moved to github for better pypi integration