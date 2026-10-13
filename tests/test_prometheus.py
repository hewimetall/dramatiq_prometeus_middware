"""HTTP exposition, in the shape of Dramatiq's ``tests/test_prometheus.py``.

Dramatiq starts ``_run_exposition_server`` on port 9191 and leaves the thread
running. This suite also boots the stock middleware, so the server binds a
free port on ``127.0.0.1`` and is shut down at the end of the test.
"""

from __future__ import annotations

import os
from threading import Thread
from urllib.request import urlopen

import pytest

from dramatiq_prometheus_fix import serve_exposition

_ENV_KEYS = (
    "PROMETHEUS_MULTIPROC_DIR",
    "prometheus_multiproc_dir",
    "DRAMATIQ_PROM_DB",
    "dramatiq_prom_db",
)


def test_prometheus_middleware_exposes_metrics(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DRAMATIQ_PROM_DB", os.fspath(tmp_path))

    # Given an instance of the exposition server
    httpd = serve_exposition(host="127.0.0.1", port=0)
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        # When I request metrics via HTTP
        host, port = httpd.server_address
        with urlopen(f"http://{host}:{port}", timeout=2) as resp:
            # Then the response should be successful
            assert resp.getcode() == 200
            assert "text/plain" in resp.headers.get("content-type", "")
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)
