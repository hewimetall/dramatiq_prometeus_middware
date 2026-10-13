"""Prometheus middleware with delayed-message accounting that matches memory.

Stock ``dramatiq.middleware.prometheus.Prometheus`` (still the case in
Dramatiq 2.2.1) does three things that make
``dramatiq_delayed_messages_inprogress`` unusable:

1. ``before_delay_message`` increments using ``message.queue_name``. While the
   message waits, that name is the delay queue (``billing.DQ``). The matching
   decrement in ``before_process_message`` uses the canonical name (``billing``),
   because ``Worker.handle_delayed_messages`` requeues with ``q_name()`` first.
   The ``.DQ`` series only grows. The canonical series goes negative.
   Reported in https://github.com/Bogdanp/dramatiq/issues/627.

2. The message id is stored in ``self.delayed_messages`` of the process that
   pulled the message into its delay queue. After the ETA, the requeued copy
   is processed by whichever worker consumes the canonical queue. Another
   process does not have the id, so it never decrements, and the original
   process keeps the id forever.
   Reported in https://github.com/Bogdanp/dramatiq/issues/672.

3. The delayed gauge is created without ``multiprocess_mode="livesum"``
   (the default is ``"all"``). ``mark_process_dead`` only deletes
   ``gauge_live*`` files, so a finished process's delayed series stays in the
   scrape. The in-progress gauge already uses ``livesum``.
   Reported in https://github.com/Bogdanp/dramatiq/issues/627.

Decrementing inside ``before_process_message`` is also the wrong moment.
The message has already left the in-memory delay queue when its ETA passes,
when the consumer requeues the queue on shutdown, or when a connection error
replaces the queue. Those paths do not process the message.

This middleware counts a message only while that process holds it in memory.
The gauge uses the canonical queue name on the way up and on the way down,
and both live gauges use ``livesum``.
"""

from __future__ import annotations

import os
import threading
from typing import Any

from dramatiq.common import current_millis, q_name
from dramatiq.middleware import Middleware

from . import tracking
from .multiproc import prepare_multiproc_dir, resolve_db_path

# Same buckets as dramatiq.middleware.prometheus.Prometheus so existing
# histograms stay comparable.
_DURATION_BUCKETS = (
    5,
    10,
    25,
    50,
    75,
    100,
    250,
    500,
    750,
    1000,
    2500,
    5000,
    7500,
    10000,
    30000,
    60000,
    600000,
    900000,
    float("inf"),
)


class Prometheus(Middleware):
    """Export Dramatiq stats for Prometheus.

    Use this instead of ``dramatiq.middleware.prometheus.Prometheus``.
    Metric names and labels are the same, except ``queue_name`` is always the
    canonical queue (``billing``, not ``billing.DQ`` or ``billing.XQ``).

    Parameters:
      db_path: Multiprocess directory. Defaults to ``resolve_db_path()``.
        Every process in the group must use the same directory. Set
        ``DRAMATIQ_PROM_DB`` in the parent environment so workers and the
        exposition process agree.
      expose: When true (the default), Dramatiq's CLI forks the exposition
        server, as the stock middleware does.
      host: Bind address of the exposition server. Overrides
        ``DRAMATIQ_PROM_HOST`` / ``dramatiq_prom_host`` (default ``0.0.0.0``).
      port: Bind port. Overrides ``DRAMATIQ_PROM_PORT`` / ``dramatiq_prom_port``
        (default ``9191``).
      multiprocess: Write mmap files and call ``mark_process_dead`` on
        shutdown. Disable only in single-process tests.
    """

    def __init__(
        self,
        *,
        db_path: str | os.PathLike[str] | None = None,
        expose: bool = True,
        host: str | None = None,
        port: int | None = None,
        multiprocess: bool = True,
    ) -> None:
        from dramatiq.logging import get_logger

        self.logger = get_logger(__name__, type(self))
        self.db_path = resolve_db_path(db_path)
        self.expose = expose
        self.multiprocess = multiprocess
        self._host = host
        self._port = port
        self._ready = False
        self._lock = threading.Lock()
        self._delayed: dict[str, tuple[str, str]] = {}
        self._message_start_times: dict[str, int] = {}
        self._metrics: Any = None

        if host is not None:
            os.environ["DRAMATIQ_PROM_HOST"] = host
            os.environ["dramatiq_prom_host"] = host
        if port is not None:
            os.environ["DRAMATIQ_PROM_PORT"] = str(port)
            os.environ["dramatiq_prom_port"] = str(port)

    @property
    def forks(self):
        if not self.expose:
            return []
        return [_run_exposition_server]

    @property
    def delayed_message_ids(self) -> frozenset[str]:
        """Message ids currently counted as delayed in this process."""

        with self._lock:
            return frozenset(self._delayed)

    def after_process_boot(self, broker: Any) -> None:
        del broker
        if self._ready:
            return

        if self.multiprocess:
            # Set the env before importing prometheus_client. The client
            # chooses mmap values the first time it is imported.
            self.db_path = prepare_multiproc_dir(self.db_path, clean=False)

        import prometheus_client as prom

        self.logger.debug("Setting up metrics...")
        registry = prom.CollectorRegistry()
        labels = ["queue_name", "actor_name"]
        self.total_messages = prom.Counter(
            "dramatiq_messages_total",
            "The total number of messages processed.",
            labels,
            registry=registry,
        )
        self.total_errored_messages = prom.Counter(
            "dramatiq_message_errors_total",
            "The total number of errored messages.",
            labels,
            registry=registry,
        )
        self.total_retried_messages = prom.Counter(
            "dramatiq_message_retries_total",
            "The total number of retried messages.",
            labels,
            registry=registry,
        )
        self.total_rejected_messages = prom.Counter(
            "dramatiq_message_rejects_total",
            "The total number of dead-lettered messages.",
            labels,
            registry=registry,
        )
        self.inprogress_messages = prom.Gauge(
            "dramatiq_messages_inprogress",
            "The number of messages in progress.",
            labels,
            registry=registry,
            multiprocess_mode="livesum",
        )
        self.inprogress_delayed_messages = prom.Gauge(
            "dramatiq_delayed_messages_inprogress",
            "The number of delayed messages in memory.",
            labels,
            registry=registry,
            multiprocess_mode="livesum",
        )
        self.message_durations = prom.Histogram(
            "dramatiq_message_duration_milliseconds",
            "The time spent processing messages.",
            labels,
            buckets=_DURATION_BUCKETS,
            registry=registry,
        )
        self._metrics = prom
        self._ready = True

    def before_worker_boot(self, broker: Any, worker: Any) -> None:
        del broker, worker
        tracking.register(self)

    def after_worker_shutdown(self, broker: Any, worker: Any) -> None:
        del broker, worker
        # close() has already requeued the delay queues. Anything still
        # tracked was missed by that path; it is no longer in this process.
        self._release_all()
        tracking.unregister(self)
        if self.multiprocess and self._ready:
            self.logger.debug("Marking process dead...")
            mark_dead(os.getpid(), self.db_path)

    def after_nack(self, broker: Any, message: Any) -> None:
        del broker
        self.total_rejected_messages.labels(*_labels(message)).inc()

    def after_enqueue(self, broker: Any, message: Any, delay: int | None) -> None:
        del broker
        # The worker re-enqueues a delayed message onto the canonical queue
        # with delay=None after the ETA. That copy still carries ``retries``,
        # but it is not a new retry. Count only the enqueue that applied one.
        if delay is not None and "retries" in message.options:
            self.total_retried_messages.labels(*_labels(message)).inc()

    def before_delay_message(self, broker: Any, message: Any) -> None:
        del broker
        self._require_ready()
        labels = _labels(message)
        with self._lock:
            if message.message_id in self._delayed:
                return
            self._delayed[message.message_id] = labels
        self.inprogress_delayed_messages.labels(*labels).inc()

    def before_ack(self, broker: Any, message: Any) -> None:
        """The delay-queue copy is acknowledged once its ETA has passed.

        ``handle_delayed_messages`` removes the message from the in-memory
        queue and then acknowledges that copy. The id is forgotten here, in
        the process that held it. The worker that later runs the requeued
        message does not have to be the same process.
        """

        del broker
        self._forget(message.message_id)

    def before_nack(self, broker: Any, message: Any) -> None:
        del broker
        self._forget(message.message_id)

    def before_process_message(self, broker: Any, message: Any) -> None:
        del broker
        self._require_ready()
        labels = _labels(message)
        # Do not touch the delayed gauge here. Processing happens after the
        # message has left the delay queue, often in another process.
        self.inprogress_messages.labels(*labels).inc()
        with self._lock:
            self._message_start_times[message.message_id] = current_millis()

    def after_process_message(
        self,
        broker: Any,
        message: Any,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        del broker, result
        self._require_ready()
        labels = _labels(message)
        with self._lock:
            started = self._message_start_times.pop(message.message_id, None)
        if started is None:
            started = current_millis()
        self.message_durations.labels(*labels).observe(current_millis() - started)
        self.inprogress_messages.labels(*labels).dec()
        self.total_messages.labels(*labels).inc()
        if exception is not None:
            self.total_errored_messages.labels(*labels).inc()

    after_skip_message = after_process_message

    def release_queue(self, queue: Any) -> None:
        """Forget messages still stored in an in-memory delay queue.

        Called when the queue is requeued (``close``) or discarded (the
        consumer replaces it after a connection error).
        """

        for message in tracking.messages_in_queue(queue):
            self._forget(message.message_id)

    def _release_all(self) -> None:
        with self._lock:
            leftover = list(self._delayed.items())
            self._delayed.clear()
        if not self._ready:
            return
        for labels in dict.fromkeys(labels for _, labels in leftover):
            count = sum(1 for _, item_labels in leftover if item_labels == labels)
            self.inprogress_delayed_messages.labels(*labels).dec(count)

    def _forget(self, message_id: str) -> None:
        with self._lock:
            labels = self._delayed.pop(message_id, None)
        if labels is None or not self._ready:
            return
        self.inprogress_delayed_messages.labels(*labels).dec()

    def _require_ready(self) -> None:
        if not self._ready:
            raise RuntimeError(
                "Prometheus middleware metrics are not set up. "
                "Emit after_process_boot before workers consume messages."
            )


def _labels(message: Any) -> tuple[str, str]:
    return (q_name(message.queue_name), message.actor_name)


def mark_dead(pid: int, path: str) -> None:
    from prometheus_client import multiprocess

    multiprocess.mark_process_dead(pid, path)


def exposition_address() -> tuple[str, int]:
    host = (
        os.environ.get("DRAMATIQ_PROM_HOST")
        or os.environ.get("dramatiq_prom_host")
        or "0.0.0.0"
    )
    port_text = (
        os.environ.get("DRAMATIQ_PROM_PORT")
        or os.environ.get("dramatiq_prom_port")
        or "9191"
    )
    return host, int(port_text)


def make_metrics_app():
    """Return a ``BaseHTTPRequestHandler`` class bound to the configured directory."""

    from http.server import BaseHTTPRequestHandler

    from dramatiq.logging import get_logger

    class _MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            from prometheus_client import (
                CONTENT_TYPE_LATEST,
                CollectorRegistry,
                generate_latest,
            )

            from .multiproc import ReapingMultiProcessCollector

            registry = CollectorRegistry()
            ReapingMultiProcessCollector(registry, path=resolve_db_path())
            output = generate_latest(registry)
            self.send_response(200)
            self.send_header("content-type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)

        def log_message(self, fmt: str, *args: Any) -> None:
            get_logger(__name__, type(self)).debug(fmt, *args)

    return _MetricsHandler


def serve_exposition(host: str | None = None, port: int | None = None):
    """Start the metrics HTTP server and return it.

    The returned server's ``server_address`` has the bound port. Port ``0``
    selects a free port.
    """

    from http.server import HTTPServer

    configured_host, configured_port = exposition_address()
    bind_host = configured_host if host is None else host
    bind_port = configured_port if port is None else port
    prepare_multiproc_dir(clean=False)
    httpd = HTTPServer((bind_host, bind_port), make_metrics_app())
    return httpd


def _run_exposition_server() -> int:
    """Entrypoint Dramatiq forks for the metrics HTTP server."""

    from dramatiq.logging import get_logger

    logger = get_logger(__name__, "_run_exposition_server")
    logger.debug("Starting exposition server...")
    httpd = serve_exposition()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.debug("Stopping exposition server...")
        httpd.shutdown()
    return 0
