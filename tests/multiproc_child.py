"""Fresh-interpreter entrypoints for multiprocess Prometheus checks.

Imported by ``python tests/multiproc_child.py``. Pytest does not collect it:
the module defines no ``test_`` functions.
"""

from __future__ import annotations

import os
import sys
import time


def _pin_directory(path: str) -> None:
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = path
    os.environ["prometheus_multiproc_dir"] = path
    os.environ["DRAMATIQ_PROM_DB"] = path
    os.environ["dramatiq_prom_db"] = path


def stick(path: str, ready: str) -> None:
    _pin_directory(path)
    from prometheus_client import Gauge

    gauge = Gauge(
        "dramatiq_messages_inprogress",
        "The number of messages in progress.",
        ["queue_name", "actor_name"],
        multiprocess_mode="livesum",
    )
    gauge.labels("billing", "do_work").set(7)
    with open(ready, "w", encoding="utf-8") as handle:
        handle.write(str(os.getpid()))
    time.sleep(60)


def serve(path: str, ready: str) -> None:
    _pin_directory(path)
    from dramatiq_prometheus_fix import Prometheus, serve_exposition

    prom = Prometheus(db_path=path, expose=False, multiprocess=True)
    prom.after_process_boot(None)
    prom.inprogress_messages.labels("billing", "do_work").set(4)
    # queue_name is the delay queue. The middleware must record the canonical name.
    prom.before_delay_message(None, _message("billing.DQ"))
    httpd = serve_exposition(host="127.0.0.1", port=0)
    port = httpd.server_address[1]
    with open(ready, "w", encoding="utf-8") as handle:
        handle.write(f"{port}\n{os.getpid()}\n")
    httpd.serve_forever()


def _message(queue_name: str):
    from dramatiq.message import Message

    return Message(
        queue_name=queue_name,
        actor_name="do_work",
        args=(),
        kwargs={},
        options={"eta": 1},
        message_id="served-delay",
    )


def main(argv: list[str]) -> None:
    command, path, ready = argv
    if command == "stick":
        stick(path, ready)
        return
    if command == "serve":
        serve(path, ready)
        return
    raise SystemExit(f"unknown command {command}")


if __name__ == "__main__":
    main(sys.argv[1:])
