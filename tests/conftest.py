"""Lock prometheus_client into in-process mode, then expose Dramatiq fixtures.

The client decides mmap-or-memory the first time it is imported. Import it
here, before any test sets ``PROMETHEUS_MULTIPROC_DIR``. Multiprocess file
behaviour is covered by fresh interpreters in ``test_multiproc_e2e.py``.

Broker and worker fixtures follow Dramatiq 2.2.1 ``tests/conftest.py``:
``StubBroker`` / ``RedisBroker`` / ``RabbitmqBroker``, ``dramatiq.set_broker``,
then ``Worker(..., worker_timeout=100, worker_threads=32)`` for the stub.
Redis and RabbitMQ tests skip when the server is unreachable. Dramatiq raises
in CI instead, because its workflow starts those servers. This repository does
not, so the skip stands in CI too.

One difference: Dramatiq emits ``process_boot`` before a test can add
middleware. Gauges are created in ``after_process_boot``, so these fixtures
add Prometheus and only then emit the signal.
"""

from __future__ import annotations

import logging
import os

os.environ.pop("PROMETHEUS_MULTIPROC_DIR", None)
os.environ.pop("prometheus_multiproc_dir", None)

import dramatiq
import dramatiq.broker
import prometheus_client  # noqa: F401
import pytest
from dramatiq import Worker
from dramatiq.brokers.stub import StubBroker

from dramatiq_prometheus_fix import Prometheus, tracking

logging.getLogger("dramatiq").setLevel(logging.WARNING)

_ENV_KEYS = (
    "PROMETHEUS_MULTIPROC_DIR",
    "prometheus_multiproc_dir",
    "DRAMATIQ_PROM_DB",
    "dramatiq_prom_db",
)


def _activate(broker, middleware):
    broker.add_middleware(middleware)
    broker.emit_after("process_boot")
    previous = dramatiq.broker.global_broker
    dramatiq.set_broker(broker)
    return previous


def _restore(previous) -> None:
    dramatiq.broker.global_broker = previous


@pytest.fixture()
def prometheus():
    # In-process gauges are readable with ``_value``. The CLI and the
    # multiprocess tests cover the mmap files.
    return Prometheus(expose=False, multiprocess=False)


@pytest.fixture()
def stub_broker(prometheus):
    broker = StubBroker()
    previous = _activate(broker, prometheus)
    yield broker
    broker.flush_all()
    broker.close()
    _restore(previous)


@pytest.fixture()
def stub_worker(stub_broker):
    worker = Worker(stub_broker, worker_timeout=100, worker_threads=32)
    worker.start()
    yield worker
    worker.stop()


@pytest.fixture()
def redis_broker(prometheus):
    redis = pytest.importorskip("redis")
    from dramatiq.brokers.redis import RedisBroker

    broker = RedisBroker()
    try:
        broker.client.ping()
    except redis.ConnectionError:
        pytest.skip("No connection to Redis server.")
    broker.client.flushall()
    previous = _activate(broker, prometheus)
    yield broker
    broker.client.flushall()
    broker.close()
    _restore(previous)


@pytest.fixture()
def redis_worker(redis_broker):
    worker = Worker(redis_broker, worker_threads=32)
    worker.start()
    yield worker
    worker.stop()


@pytest.fixture()
def rabbitmq_broker(prometheus):
    pytest.importorskip("pika")
    import pika
    from dramatiq.brokers.rabbitmq import RabbitmqBroker

    credentials = pika.credentials.PlainCredentials(
        os.getenv("RABBITMQ_USERNAME", "guest"),
        os.getenv("RABBITMQ_PASSWORD", "guest"),
    )
    broker = RabbitmqBroker(
        host="127.0.0.1",
        max_priority=10,
        credentials=credentials,
    )
    try:
        connection = broker.connection
    except (pika.exceptions.AMQPConnectionError, OSError):
        pytest.skip("No connection to RabbitMQ server.")
    assert connection.is_open
    previous = _activate(broker, prometheus)
    yield broker
    broker.flush_all()
    broker.close()
    _restore(previous)


@pytest.fixture()
def rabbitmq_worker(rabbitmq_broker):
    worker = Worker(rabbitmq_broker, worker_threads=32)
    worker.start()
    yield worker
    worker.stop()


@pytest.fixture()
def stock_prometheus(monkeypatch):
    """Stock middleware. The queue patch is installed first and has no listeners."""

    from dramatiq.middleware.prometheus import Prometheus as StockPrometheus

    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    tracking.install()
    return StockPrometheus()


@pytest.fixture()
def stock_stub_broker(stock_prometheus):
    broker = StubBroker()
    previous = _activate(broker, stock_prometheus)
    yield broker
    broker.flush_all()
    broker.close()
    _restore(previous)


@pytest.fixture()
def stock_stub_worker(stock_stub_broker):
    worker = Worker(stock_stub_broker, worker_timeout=100, worker_threads=32)
    worker.start()
    yield worker
    worker.stop()
