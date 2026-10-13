"""Worker end-to-end tests, written the way Dramatiq 2.2.1 writes them.

Same shape as ``tests/test_redis.py``, ``tests/test_retries.py`` and
``tests/test_worker.py``: a broker fixture, ``@dramatiq.actor`` on that
global broker, ``# Given`` / ``# When`` / ``# Then``, then
``broker.join(actor.queue_name)`` and ``worker.join()``.

``delay=1000`` on the delayed-message tests is the value Dramatiq uses in
``test_redis_actors_can_have_their_messages_delayed`` and
``test_rabbitmq_actors_can_have_their_messages_delayed``. Redis and RabbitMQ
copies skip when those servers are down.
"""

from __future__ import annotations

import time
from queue import PriorityQueue

import dramatiq
from dramatiq import Retry
from dramatiq.common import current_millis, dq_name

from common import worker
from helpers import samples, wait_until

BILLING = ("billing", "do_work")
RECORD = ("billing", "record")


def _positive(metric) -> dict[tuple, float]:
    return {labels: value for labels, value in samples(metric).items() if value != 0.0}


def _wait_until_held(prometheus, labels: tuple[str, str], amount: float = 1.0) -> dict:
    held: dict[tuple, float] = {}

    def holding() -> bool:
        current = samples(prometheus.inprogress_delayed_messages)
        if current.get(labels, 0.0) >= amount:
            held.clear()
            held.update(current)
            return True
        return False

    wait_until(holding, detail=f"{labels} was not held in worker memory")
    return held


def test_stub_actors_can_have_their_messages_delayed(
    stub_broker, stub_worker, prometheus
):
    # Given that I have a database
    start_time, run_time = current_millis(), None

    # And an actor that records the time it ran
    @dramatiq.actor(queue_name="billing")
    def record():
        nonlocal run_time
        run_time = current_millis()

    # If I send it a delayed message
    record.send_with_options(delay=1000)

    # When the worker is holding that message in memory
    held = _wait_until_held(prometheus, RECORD)

    # Then join on the queue
    stub_broker.join(record.queue_name, timeout=5000)
    stub_worker.join()

    # I expect that message to have been processed at least delayed milliseconds later
    assert run_time is not None
    assert run_time - start_time >= 1000

    # And the delayed gauge to have used the canonical queue, then returned to zero
    assert held == {RECORD: 1.0}
    assert ("billing.DQ", "record") not in samples(
        prometheus.inprogress_delayed_messages
    )
    assert _positive(prometheus.inprogress_delayed_messages) == {}
    assert _positive(prometheus.inprogress_messages) == {}
    assert samples(prometheus.total_messages)[RECORD] == 1.0
    assert prometheus.delayed_message_ids == frozenset()


def test_redis_actors_can_have_their_messages_delayed(
    redis_broker, redis_worker, prometheus
):
    # Given that I have a database
    start_time, run_time = current_millis(), None

    # And an actor that records the time it ran
    @dramatiq.actor(queue_name="billing")
    def record():
        nonlocal run_time
        run_time = current_millis()

    # If I send it a delayed message
    record.send_with_options(delay=1000)

    # When the worker is holding that message in memory
    held = _wait_until_held(prometheus, RECORD)

    # Then join on the queue
    redis_broker.join(record.queue_name, timeout=5000)
    redis_worker.join()

    # I expect that message to have been processed at least delayed milliseconds later
    assert run_time is not None
    assert run_time - start_time >= 1000
    assert held == {RECORD: 1.0}
    assert ("billing.DQ", "record") not in samples(
        prometheus.inprogress_delayed_messages
    )
    assert _positive(prometheus.inprogress_delayed_messages) == {}
    assert prometheus.delayed_message_ids == frozenset()


def test_rabbitmq_actors_can_have_their_messages_delayed(
    rabbitmq_broker, rabbitmq_worker, prometheus
):
    # Given that I have a database
    start_time, run_time = current_millis(), None

    # And an actor that records the time it ran
    @dramatiq.actor(queue_name="billing")
    def record():
        nonlocal run_time
        run_time = current_millis()

    # If I send it a delayed message
    record.send_with_options(delay=1000)

    # When the worker is holding that message in memory
    held = _wait_until_held(prometheus, RECORD)

    # Then join on the queue
    rabbitmq_broker.join(record.queue_name, timeout=5000)
    rabbitmq_worker.join()

    # I expect that message to have been processed at least delayed milliseconds later
    assert run_time is not None
    assert run_time - start_time >= 1000
    assert held == {RECORD: 1.0}
    assert ("billing.DQ", "record") not in samples(
        prometheus.inprogress_delayed_messages
    )
    assert _positive(prometheus.inprogress_delayed_messages) == {}
    assert prometheus.delayed_message_ids == frozenset()


def test_worker_e2e_delayed_message_returns_gauge_to_zero(
    stub_broker, stub_worker, prometheus
):
    # Given that I have a database
    calls: list[int] = []

    # And an actor that records its argument
    @dramatiq.actor(queue_name="billing")
    def do_work(value):
        calls.append(value)

    # When I send it two delayed messages
    do_work.send_with_options(delay=200, args=(7,))
    do_work.send_with_options(delay=200, args=(8,))
    held = _wait_until_held(prometheus, BILLING, amount=2.0)

    # Then join on the queue
    stub_broker.join(do_work.queue_name, timeout=5000)
    stub_worker.join()

    # I expect both messages to have run and the gauge to be back at zero
    assert sorted(calls) == [7, 8]
    assert held == {BILLING: 2.0}
    assert ("billing.DQ", "do_work") not in samples(
        prometheus.inprogress_delayed_messages
    )
    assert _positive(prometheus.inprogress_delayed_messages) == {}
    assert _positive(prometheus.inprogress_messages) == {}
    assert samples(prometheus.total_messages)[BILLING] == 2.0
    assert prometheus.delayed_message_ids == frozenset()


def test_actors_retry_on_failure(stub_broker, stub_worker, prometheus):
    # Given that I have a database
    failures, successes = [], []

    # And an actor that fails the first time it's called
    @dramatiq.actor(queue_name="billing", min_backoff=100, max_backoff=500)
    def do_work():
        if sum(failures) == 0:
            failures.append(1)
            raise RuntimeError("First failure.")
        successes.append(1)

    # If I send it a message
    do_work.send()

    # Then join on the queue
    stub_broker.join(do_work.queue_name, fail_fast=False, timeout=5000)
    stub_worker.join()

    # I expect successes, and the retry counted once on the canonical queue
    assert sum(successes) == 1
    assert samples(prometheus.total_retried_messages) == {BILLING: 1.0}
    assert ("billing.DQ", "do_work") not in samples(prometheus.total_retried_messages)
    assert _positive(prometheus.inprogress_delayed_messages) == {}
    assert prometheus.delayed_message_ids == frozenset()


def test_worker_e2e_retry_without_explicit_delay_returns_to_zero(
    stub_broker, stub_worker, prometheus
):
    # Given that I have a database
    calls: list[int] = []

    # And an actor that fails the first time it's called
    @dramatiq.actor(queue_name="billing", max_retries=3, min_backoff=20, max_backoff=20)
    def do_work():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("transient")

    # If I send it a message
    do_work.send()

    # Then join on the queue
    stub_broker.join(do_work.queue_name, fail_fast=False, timeout=5000)
    stub_worker.join()

    # I expect one retry. The ETA handoff enqueues again with delay=None and is not counted.
    assert len(calls) == 2
    assert samples(prometheus.total_retried_messages)[BILLING] == 1.0
    assert samples(prometheus.total_errored_messages)[BILLING] == 1.0
    assert samples(prometheus.total_messages)[BILLING] == 2.0
    assert ("billing.DQ", "do_work") not in samples(prometheus.total_retried_messages)
    assert _positive(prometheus.inprogress_delayed_messages) == {}
    assert _positive(prometheus.inprogress_messages) == {}
    assert prometheus.delayed_message_ids == frozenset()


def test_retry_exceptions_can_specify_a_delay(stub_broker, stub_worker, prometheus):
    # Given that I have an actor that raises Retry
    attempts = 0
    timestamps = [time.monotonic()]

    @dramatiq.actor(queue_name="billing", max_retries=1)
    def do_work():
        nonlocal attempts
        attempts += 1
        timestamps.append(time.monotonic())
        if attempts == 1:
            raise Retry(delay=100)

    # When I send that actor a message
    do_work.send()

    # And join on the queue
    stub_broker.join(do_work.queue_name, fail_fast=False, timeout=5000)
    stub_worker.join()

    # Then the actor should have been retried after 100ms
    assert 0.1 <= timestamps[-1] - timestamps[-2] < 0.5
    assert samples(prometheus.total_retried_messages)[BILLING] == 1.0
    assert _positive(prometheus.inprogress_delayed_messages) == {}
    assert prometheus.delayed_message_ids == frozenset()


def test_worker_e2e_explicit_retry_delay_returns_to_zero(
    stub_broker, stub_worker, prometheus
):
    # Given that I have an actor that raises Retry
    calls: list[int] = []

    @dramatiq.actor(queue_name="billing", max_retries=1, min_backoff=10, max_backoff=10)
    def do_work():
        calls.append(1)
        if len(calls) == 1:
            raise Retry("once", delay=30)

    # When I send that actor a message
    do_work.send()

    # And join on the queue
    stub_broker.join(do_work.queue_name, fail_fast=False, timeout=5000)
    stub_worker.join()

    # Then the delayed gauge is back at zero and the retry was counted once
    assert calls == [1, 1]
    assert samples(prometheus.total_retried_messages)[BILLING] == 1.0
    assert _positive(prometheus.inprogress_delayed_messages) == {}
    assert prometheus.delayed_message_ids == frozenset()


def test_worker_e2e_plain_message_does_not_touch_the_delayed_gauge(
    stub_broker, stub_worker, prometheus
):
    # Given that I have an actor that does not retry
    @dramatiq.actor(queue_name="billing", max_retries=0)
    def do_work():
        return "ok"

    # When I send that actor a message
    do_work.send()

    # And join on the queue
    stub_broker.join(do_work.queue_name, timeout=5000)
    stub_worker.join()

    # Then the delayed gauge has no series
    assert samples(prometheus.inprogress_delayed_messages) == {}
    assert _positive(prometheus.inprogress_messages) == {}
    assert samples(prometheus.total_messages)[BILLING] == 1.0


def test_worker_e2e_shutdown_forgets_messages_still_in_memory(stub_broker, prometheus):
    # Given that I have an actor that takes its time
    @dramatiq.actor(queue_name="billing")
    def do_work():
        raise AssertionError("eta is an hour away")

    # If I send it a delayed message
    do_work.send_with_options(delay=3_600_000)

    # Then start a worker and subsequently shut it down
    with worker(stub_broker, worker_timeout=100, worker_threads=1) as stub_worker:
        consumer = stub_worker.consumers[dq_name("billing")]
        wait_until(
            lambda: (
                consumer.delay_queue.qsize() == 1
                and bool(prometheus.delayed_message_ids)
            ),
            detail="delayed message was not pulled into worker memory",
        )
        assert samples(prometheus.inprogress_delayed_messages) == {BILLING: 1.0}

    # I expect the gauge to be released even though the actor never ran
    assert prometheus.delayed_message_ids == frozenset()
    assert _positive(prometheus.inprogress_delayed_messages) == {}


def test_worker_e2e_replacing_delay_queue_releases_the_gauge(
    stub_broker, stub_worker, prometheus
):
    """A connection error does ``self.delay_queue = PriorityQueue()``.

    ``pause()`` makes the consume loop exit while ``running`` is still true,
    and Dramatiq then treats that as a consumer restart: it calls ``close()``
    and sleeps. Stopping the thread leaves the delay queue in place so the
    replacement itself can be observed.
    """

    # Given that I have an actor
    @dramatiq.actor(queue_name="billing")
    def do_work():
        return None

    # When I send it a message whose ETA is an hour away
    do_work.send_with_options(delay=3_600_000)
    consumer = stub_worker.consumers[dq_name("billing")]
    wait_until(
        lambda: (
            consumer.delay_queue.qsize() == 1 and bool(prometheus.delayed_message_ids)
        ),
        detail="delayed message was not pulled into worker memory",
    )

    # And the consumer thread is stopped before the queue object is replaced
    consumer.stop()
    consumer.join(timeout=2)
    assert not consumer.is_alive()
    assert prometheus.delayed_message_ids

    # Then replacing the delay queue forgets the message
    consumer.delay_queue = PriorityQueue()
    assert prometheus.delayed_message_ids == frozenset()
    assert _positive(prometheus.inprogress_delayed_messages) == {}
