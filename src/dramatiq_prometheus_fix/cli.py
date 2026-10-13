"""Wipe the multiprocess directory, then replace this process with Dramatiq.

The stock middleware's module comment says the Prometheus directory is cleared
before every run. The code only creates the directory. Files from the previous
process group stay there, and a reused pid merges with them. A worker must not
delete the directory itself: every worker imports the app and would race the
others.

This command runs in the parent, deletes ``*.db`` once, exports the directory,
and execs ``dramatiq`` so every child inherits the same path.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Sequence

from .multiproc import prepare_multiproc_dir


def main(
    argv: Sequence[str] | None = None,
    *,
    execvp: Callable[[str, list[str]], None] = os.execvp,
) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if _should_prepare(args):
        path = prepare_multiproc_dir(clean=_clean_enabled())
        print(f"dramatiq-prometheus: PROMETHEUS_MULTIPROC_DIR={path}", file=sys.stderr)
    try:
        execvp("dramatiq", ["dramatiq", *args])
    except FileNotFoundError:
        print(
            "dramatiq-prometheus: dramatiq executable was not found on PATH",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


def _should_prepare(args: Sequence[str]) -> bool:
    return not any(arg in {"-h", "--help"} for arg in args)


def _clean_enabled() -> bool:
    value = os.environ.get("DRAMATIQ_PROM_CLEAN", "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


if __name__ == "__main__":
    main()
