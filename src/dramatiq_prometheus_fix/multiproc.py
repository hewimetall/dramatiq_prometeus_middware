"""Multiprocess Prometheus directory helpers for Dramatiq workers.

``prometheus_client`` stores one mmap file per process. ``livesum`` gauges are
summed from every file that is still on disk. ``mark_process_dead`` deletes
only ``gauge_live*_<pid>.db`` files; it does not look at ``/proc`` by itself,
and it leaves ``multiprocess_mode="all"`` files in place. A scrape therefore
keeps a dead worker's gauge until something deletes that file.
"""

from __future__ import annotations

import glob
import os
import tempfile

_ENV_KEYS = (
    "DRAMATIQ_PROM_DB",
    "dramatiq_prom_db",
    "PROMETHEUS_MULTIPROC_DIR",
    "prometheus_multiproc_dir",
)


def resolve_db_path(path: str | os.PathLike[str] | None = None) -> str:
    """Return the multiprocess directory shared by workers and the scraper.

    Explicit ``path`` wins. Otherwise the first non-empty variable among
    ``DRAMATIQ_PROM_DB``, ``dramatiq_prom_db``, ``PROMETHEUS_MULTIPROC_DIR``
    and ``prometheus_multiproc_dir`` is used. The default is
    ``$TMPDIR/dramatiq-prometheus``, the same location stock Dramatiq uses.
    """

    if path is not None and str(path):
        return os.fspath(path)
    for key in _ENV_KEYS:
        value = os.environ.get(key)
        if value:
            return value
    return os.path.join(tempfile.gettempdir(), "dramatiq-prometheus")


def publish_db_path(path: str) -> None:
    """Export ``path`` under every name Dramatiq and prometheus_client read."""

    for key in _ENV_KEYS:
        os.environ[key] = path


def prepare_multiproc_dir(
    path: str | os.PathLike[str] | None = None,
    *,
    clean: bool = False,
) -> str:
    """Create the multiprocess directory and optionally delete ``*.db`` files.

    Call ``clean=True`` once in the parent process, before workers and the
    web app start. Cleaning from inside a worker deletes sibling files.
    Cleaning a directory that is shared with an already-running process
    deletes that process's metrics too.
    """

    resolved = resolve_db_path(path)
    os.makedirs(resolved, exist_ok=True)
    if clean:
        for name in os.listdir(resolved):
            if name.endswith(".db"):
                file_path = os.path.join(resolved, name)
                try:
                    os.remove(file_path)
                except FileNotFoundError:
                    continue
    publish_db_path(resolved)
    return resolved


def pid_alive(pid: int) -> bool:
    """Return whether ``pid`` is a running process."""

    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _pids_in_db(path: str) -> set[int]:
    pids: set[int] = set()
    for file_name in glob.glob(os.path.join(path, "gauge_*_*.db")):
        stem = os.path.basename(file_name)[: -len(".db")]
        pid_text = stem.rsplit("_", 1)[-1]
        if pid_text.isdigit():
            pids.add(int(pid_text))
    return pids


def reap_dead_processes(path: str | os.PathLike[str] | None = None) -> list[int]:
    """Delete live-mode gauge files whose process is no longer running.

    ``mark_process_dead`` removes ``gauge_livesum_<pid>.db`` and the other
    ``live*`` gauge files. Counter and histogram files are left alone, so
    totals from a finished worker stay in the scrape. Pid reuse is not
    detected: if a new process has already taken the same pid, its file is
    kept. Wipe the directory at process-group start to cover that case.
    """

    resolved = resolve_db_path(path)
    if not os.path.isdir(resolved):
        return []

    from prometheus_client import multiprocess

    reaped: list[int] = []
    for pid in sorted(_pids_in_db(resolved)):
        if pid == os.getpid() or pid_alive(pid):
            continue
        multiprocess.mark_process_dead(pid, resolved)
        reaped.append(pid)
    return reaped


class ReapingMultiProcessCollector:
    """Collector that drops dead workers' live gauges, then merges mmap files.

    Registers itself on ``registry``. The wrapped ``MultiProcessCollector``
    is not registered, so each file is exported once.
    """

    def __init__(self, registry, path: str | os.PathLike[str] | None = None) -> None:
        from prometheus_client.multiprocess import MultiProcessCollector

        self._path = prepare_multiproc_dir(path, clean=False)
        self._collector = MultiProcessCollector(None, path=self._path)
        if registry is not None:
            registry.register(self)

    def collect(self):
        reap_dead_processes(self._path)
        return self._collector.collect()
