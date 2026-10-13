from __future__ import annotations

import time
from collections.abc import Callable


def samples(metric) -> dict[tuple, float]:
    """Read labeled children. A labeled metric with no observations has none."""

    if getattr(metric, "_labelnames", ()):
        children = getattr(metric, "_metrics", None) or {}
        return {
            tuple(labels): float(child._value.get())
            for labels, child in children.items()
        }
    return {(): float(metric._value.get())}


def wait_until(
    predicate: Callable[[], bool], timeout: float = 5.0, detail: str = ""
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(detail or "condition was not met")
