"""Helpers copied from Dramatiq's ``tests/common.py``.

The context manager is the one Dramatiq uses when a test starts the worker
itself (shutdown, pause) instead of taking the ``stub_worker`` fixture.
"""

from __future__ import annotations

from contextlib import contextmanager

from dramatiq import Worker


@contextmanager
def worker(*args, **kwargs):
    try:
        worker = Worker(*args, **kwargs)
        worker.start()
        yield worker
    finally:
        worker.stop()
