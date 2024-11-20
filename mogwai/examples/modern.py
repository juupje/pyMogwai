"""
Created on 2024-11-14

@author: wf
"""

from typing import Optional

from mogwai.lod.yamlable import lod_storable


@lod_storable
class Person:
    """
    a person
    """

    name: str
    age: int


@lod_storable
class Software:
    """
    a software
    """

    name: str
    lang: Optional[str] = None
