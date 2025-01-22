"""
Created on 2024-08-15

@author: wf
"""

from dataclasses import dataclass

import mogwai


@dataclass
class Version(object):
    """
    Version handling for pyMogwai
    """

    name = "pymogwai"
    version = mogwai.__version__
    date = "2024-08-15"
    updated = "2024-11-14"
    description = "python native gremlin implementation"

    authors = "Wolfgang Fahl"

    chat_url = "https://github.com/juupje/pyMogwai/discussions"
    doc_url = "https://cr.bitplan.com/index.php/pyMogwai"
    cm_url = "https://github.com/juupje/pyMogwai"

    license = f"""Copyright 2024 contributors. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied."""
    longDescription = f"""{name} version {version}
{description}

  Created by {authors} on {date} last updated {updated}"""
