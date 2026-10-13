"""Multiprocess export, checked in a fresh Python so mmap mode is actually on."""

from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.multiprocess import MultiProcessCollector

from dramatiq_prometheus_fix import ReapingMultiProcessCollector, reap_dead_processes

CHILD = Path(__file__).with_name("multiproc_child.py")


def _spawn(command: str, path: Path, ready: Path) -> subprocess.Popen[str]:
    env = os.environ.copy()
    for key in (
        "PROMETHEUS_MULTIPROC_DIR",
        "prometheus_multiproc_dir",
        "DRAMATIQ_PROM_DB",
        "dramatiq_prom_db",
    ):
        env.pop(key, None)
    return subprocess.Popen(
        [sys.executable, str(CHILD), command, os.fspath(path), os.fspath(ready)],
        env=env,
        text=True,
    )


def _wait_ready(path: Path, proc: subprocess.Popen[str], timeout: float = 10) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            return path.read_text(encoding="utf-8").strip()
        if proc.poll() is not None:
            raise AssertionError(f"child exited early with {proc.returncode}")
        time.sleep(0.02)
    raise AssertionError("child did not become ready")


def _text(path: Path, *, reap: bool) -> str:
    registry = CollectorRegistry()
    if reap:
        ReapingMultiProcessCollector(registry, path=path)
    else:
        MultiProcessCollector(registry, path=os.fspath(path))
    return generate_latest(registry).decode()


def test_livesum_keeps_a_dead_process_until_reap(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    proc = _spawn("stick", tmp_path, ready)
    try:
        pid = int(_wait_ready(ready, proc))
        alive = _text(tmp_path, reap=False)
        assert (
            'dramatiq_messages_inprogress{actor_name="do_work",queue_name="billing"} 7.0'
            in alive
        )

        proc.kill()
        proc.wait(timeout=5)
        proc = None  # type: ignore[assignment]

        stuck = _text(tmp_path, reap=False)
        assert " 7.0" in stuck

        reaped = reap_dead_processes(tmp_path)
        assert pid in reaped
        cleared = _text(tmp_path, reap=False)
        assert "dramatiq_messages_inprogress" not in cleared
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def _get(url: str) -> tuple[int, str]:
    deadline = time.monotonic() + 5
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                return response.status, response.read().decode()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last = exc
            time.sleep(0.05)
    raise AssertionError(f"metrics endpoint did not respond: {last}")


def _sample(body: str, name: str, queue_name: str) -> str | None:
    needle = f"{name}{{"
    for line in body.splitlines():
        if (
            line.startswith(needle)
            and f'queue_name="{queue_name}"' in line
            and 'actor_name="do_work"' in line
        ):
            return line.split()[-1]
    return None


def test_exposition_server_exports_canonical_delayed_gauge(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    proc = _spawn("serve", tmp_path, ready)
    try:
        port_text, pid_text = _wait_ready(ready, proc).splitlines()
        status, body = _get(f"http://127.0.0.1:{port_text}")
        assert status == 200
        assert 'queue_name="billing.DQ"' not in body
        assert _sample(body, "dramatiq_messages_inprogress", "billing") == "4.0"
        assert _sample(body, "dramatiq_delayed_messages_inprogress", "billing") == "1.0"

        proc.kill()
        proc.wait(timeout=5)
        proc = None  # type: ignore[assignment]

        # The process is gone, but its livesum files are still on disk.
        stuck = _text(tmp_path, reap=False)
        assert (
            'dramatiq_messages_inprogress{actor_name="do_work",queue_name="billing"} 4.0'
            in stuck
        )

        cleared = _text(tmp_path, reap=True)
        assert "dramatiq_messages_inprogress" not in cleared
        assert "dramatiq_delayed_messages_inprogress" not in cleared
        assert int(pid_text) not in reap_dead_processes(tmp_path)
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
