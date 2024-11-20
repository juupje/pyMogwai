"""
Created on 2024-11-07

@author: wf

base on
A. Harth and S. Decker, "Optimized index structures for querying RDF from the Web," Third Latin American Web Congress (LA-WEB'2005), Buenos Aires, Argentina, 2005, pp. 10 pp.-, doi: 10.1109/LAWEB.2005.25.
keywords: {Resource description framework;Data models;Semantic Web;Indexes;Java;Vocabulary;Database systems;Memory;Indexing;Information retrieval},
"""

from dataclasses import dataclass
from enum import Enum
from typing import Hashable, Set


@dataclass
class IndexConfig:
    """Configuration of which SPOG indices to use"""

    active_indices: Set[str]


class IndexConfigs(Enum):
    """Standard index configurations"""

    OFF = "off"  # Use no indices
    ALL = "all"  # Use all 16 indices
    MINIMAL = "minimal"  # Use minimal required set

    def get_config(self) -> IndexConfig:
        """Get the index configuration for this enum value"""
        if self == IndexConfigs.OFF:
            return IndexConfig(set())

        if self == IndexConfigs.ALL:
            positions = ["S", "P", "O", "G"]
            indices = {
                f"{from_pos}{to_pos}"
                for from_pos in positions
                for to_pos in positions
                if from_pos != to_pos
            }
            return IndexConfig(indices)

        if self == IndexConfigs.MINIMAL:
            return IndexConfig(
                {
                    # Core indices for basic node relationships
                    "PS",  # Predicate -> Subject: links predicates to subjects (e.g., labels or properties to nodes)
                    "PO",  # Predicate -> Object: maps predicates to values (e.g., property values)
                    "SO",  # Subject -> Object: links source nodes to target nodes in relationships
                    "OS",  # Object -> Subject: reverse lookup for values back to nodes
                    # Graph-based indices for context-specific associations
                    "PG",  # Predicate -> Graph: associates predicates with graph contexts
                    "SG",  # Subject -> Graph: associates subjects with graph contexts
                    "GO",  # Graph -> Object: maps graph contexts to objects for grouped retrieval
                    "GP",  # Graph -> Predicate: links graph contexts to predicates
                }
            )

        raise ValueError(f"Unknown index configuration: {self}")


@dataclass(frozen=True)
class Quad:
    """A quad of hashable values (Subject-Predicate-Object-Graph)"""

    s: Hashable  # Subject
    p: Hashable  # Predicate
    o: Hashable  # Object
    g: Hashable | None = None  # Graph context


class Index:
    """A Single index in the SPOG matrix as explained in
    identified by from/to positions"""

    def __init__(self, from_pos: str, to_pos: str):
        """
        Args:
            from_pos: First position (S,P,O,G)
            to_pos: Second position (S,P,O,G)
        """
        self.from_pos = from_pos
        self.to_pos = to_pos
        self.lookup = {}

    @property
    def name(self) -> str:
        """Full quad index name based on Harth/Decker SPOG ordering"""
        index_name = f"{self.from_pos}{self.to_pos}"
        return index_name

    def add_quad(self, quad: Quad) -> None:
        """Add a quad to this index's lookup using quad positions"""
        from_val = getattr(quad, self.from_pos.lower())
        to_val = getattr(quad, self.to_pos.lower())
        if not isinstance(from_val, Hashable):
            pass

        if from_val not in self.lookup:
            self.lookup[from_val] = set()
        self.lookup[from_val].add(to_val)


class SPOGIndex:
    """
    all 16 possible indices based on SPOG matrix

    see http://harth.org/andreas/ YARS and the paper
    """

    def __init__(self, config: IndexConfig):
        self.config = config
        positions = ["S", "P", "O", "G"]
        self.indices = {}
        self.indices = {}
        for from_pos in positions:
            for to_pos in positions:
                if from_pos != to_pos:
                    index = Index(from_pos, to_pos)
                    self.indices[index.name] = index

    def get_lookup(self, from_pos: str, to_pos: str) -> dict | None:
        """
        Get lookup dict for from->to positions if active

        Args:
            from_pos: From position (S,P,O,G)
            to_pos: To position (S,P,O,G)
        Returns:
            Lookup dict if index active in current config, None otherwise
        """
        index_name = f"{from_pos}{to_pos}"
        if index_name in self.config.active_indices:
            return self.indices[index_name].lookup
        return None

    def add_quad(self, quad: Quad) -> None:
        """Add quad only to configured active indices"""
        for index_name in self.config.active_indices:
            self.indices[index_name].add_quad(quad)
