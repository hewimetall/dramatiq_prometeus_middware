"""Sphinx configuration.

Layout matches ``sphinx-quickstart`` (``docs/conf.py``, ``index``, ``_static``,
``_templates``). Pages are grouped with the Diátaxis template:
https://diataxis.fr/ — tutorial, how-to, reference (what), explanation (why).
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "dramatiq-prometheus-fix"
author = "dramatiq-prometheus-fix contributors"
copyright = "2026, dramatiq-prometheus-fix contributors"
release = "0.1.0"
version = release

extensions = ["myst_parser"]
source_suffix = {".md": "markdown"}
myst_heading_anchors = 3
myst_enable_extensions = ["colon_fence", "deflist"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "tutorials/examples"]

language = "ru"
html_theme = "alabaster"
html_static_path = ["_static"]
html_title = "dramatiq-prometheus-fix"
root_doc = "index"
