"""Lock prometheus_client into in-process mode for this test runner.

The client decides mmap-or-memory the first time it is imported. Import it
here, before any test sets ``PROMETHEUS_MULTIPROC_DIR``. Multiprocess file
behaviour is covered by fresh interpreters in ``test_multiproc_e2e.py``.
"""

from __future__ import annotations

import logging
import os

os.environ.pop("PROMETHEUS_MULTIPROC_DIR", None)
os.environ.pop("prometheus_multiproc_dir", None)

import prometheus_client  # noqa: F401

logging.getLogger("dramatiq").setLevel(logging.WARNING)
