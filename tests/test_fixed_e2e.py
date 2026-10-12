"""End-to-end checks for the replacement Prometheus middleware.

The worker tests drive a real Dramatiq ``Worker`` and ``StubBroker``. The bug
lives in the middleware and the worker delay queue, so a stub broker exercises
the same hooks Redis and RabbitMQ workers emit.
"""

from __future__ import annotations

from queue import PriorityQueue

from dramatiq import Retry, actor
from dramatiq.common import dq_name
from dramatiq.message import Message
from helpers import running_worker, samples, wait_until

from dramatiq_prometheus_fix import Prometheus

BILLING = ("billing", "do_work")
BILLING_DQ = ("billing.DQ", "do_work")


def _prom() -> Prometheus:
    return Prometheus(expose=False, multiprocess=False)


def _boot(prom: Prometheus) -> None:
    prom.after_process_boot(None)


def _delayed(queue_name: str, message_id: str) -> Message:
    return Message(
        queue_name=queue_name,
        actor_name="do_work",
        args=(),
        kwargs={},
        options={"eta": 1},
        message_id=message_id,
    )


def _positive(metric) -> dict[tuple, float]:
    return {labels: value for labels, value in samples(metric).items() if value != 0.0}


def test_fixed_gauge_uses_livesum_and_canonical_names() -> None:
    prom = _prom()
    _boot(prom)
    assert prom.inprogress_delayed_messages._multiprocess_mode == "livesum"
    assert prom.inprogress_messages._multiprocess_mode == "livesum"

    delayed = _delayed("billing.DQ", "canonical")
    prom.before_delay_message(None, delayed)
    prom.before_delay_message(None, delayed)
    assert samples(prom.inprogress_delayed_messages) == {BILLING: 1.0}

    prom.before_ack(None, delayed)
    assert _positive(prom.inprogress_delayed_messages) == {}
    assert prom.delayed_message_ids == frozenset()


def test_fixed_other_process_does_not_have_to_process_the_message() -> None:
    holder = _prom()
    other = _prom()
    _boot(holder)
    _boot(other)
    delayed = _delayed("billing.DQ", "held-here")
    holder.before_delay_message(None, delayed)

    processed = delayed.copy(queue_name="billing")
    del processed.options["eta"]
    other.before_process_message(None, processed)
    other.after_process_message(None, processed)
    other.before_ack(None, processed)

    assert samples(holder.inprogress_delayed_messages) == {BILLING: 1.0}
    assert holder.delayed_message_ids == frozenset({"held-here"})
    assert _positive(other.inprogress_delayed_messages) == {}
    assert _positive(other.inprogress_messages) == {}

    holder.before_ack(None, delayed)
    assert _positive(holder.inprogress_delayed_messages) == {}
    assert holder.delayed_message_ids == frozenset()


def test_fixed_ack_is_idempotent_when_both_copies_share_the_message_id() -> None:
    prom = _prom()
    _boot(prom)
    delayed = _delayed("billing.DQ", "same-id")
    prom.before_delay_message(None, delayed)
    requeued = delayed.copy(queue_name="billing")
    del requeued.options["eta"]

    prom.before_ack(None, requeued)
    prom.before_ack(None, delayed)
    assert _positive(prom.inprogress_delayed_messages) == {}
    assert prom.delayed_message_ids == frozenset()


def test_fixed_release_queue_drops_messages_without_draining_it() -> None:
    prom = _prom()
    _boot(prom)
    delayed = _delayed("billing.DQ", "still-queued")
    prom.before_delay_message(None, delayed)

    class _Item:
        def __init__(self, message: Message) -> None:
            self.message = message

    queue: PriorityQueue = PriorityQueue()
    queue.queue.append(_Item(delayed))  # type: ignore[attr-defined]
    prom.release_queue(queue)

    assert _positive(prom.inprogress_delayed_messages) == {}
    assert list(queue.queue)  # type: ignore[attr-defined]
    prom.release_queue(queue)
    assert _positive(prom.inprogress_delayed_messages) == {}


def test_worker_e2e_delayed_message_returns_gauge_to_zero() -> None:
    calls: list[int] = []
    prom = _prom()

    def declare(broker):
        @actor(broker=broker, queue_name="billing")
        def do_work(value):
            calls.append(value)

    with running_worker(prom, declare) as (broker, worker):
        do_work = broker.get_actor("do_work")
        do_work.send_with_options(delay=40, args=(7,))
        do_work.send_with_options(delay=40, args=(8,))
        broker.join("billing", timeout=5000)
        worker.join()
        delayed = samples(prom.inprogress_delayed_messages)
        inprogress = samples(prom.inprogress_messages)
        total = samples(prom.total_messages)

    assert sorted(calls) == [7, 8]
    assert delayed.get(BILLING_DQ, 0.0) == 0.0
    assert delayed.get(BILLING, 0.0) == 0.0
    assert _positive(prom.inprogress_delayed_messages) == {}
    assert _positive_from(inprogress) == {}
    assert total[BILLING] == 2.0
    assert prom.delayed_message_ids == frozenset()


def test_worker_e2e_shutdown_forgets_messages_still_in_memory() -> None:
    prom = _prom()

    def declare(broker):
        @actor(broker=broker, queue_name="billing")
        def do_work():
            raise AssertionError("eta is an hour away")

    with running_worker(prom, declare) as (broker, worker):
        broker.get_actor("do_work").send_with_options(delay=3_600_000)
        consumer = worker.consumers[dq_name("billing")]
        wait_until(
            lambda: (
                consumer.delay_queue.qsize() == 1 and bool(prom.delayed_message_ids)
            ),
            detail="delayed message was not pulled into worker memory",
        )
        assert samples(prom.inprogress_delayed_messages)[BILLING] == 1.0

    assert prom.delayed_message_ids == frozenset()
    assert _positive(prom.inprogress_delayed_messages) == {}


def test_worker_e2e_replacing_delay_queue_releases_the_gauge() -> None:
    """A connection error does ``self.delay_queue = PriorityQueue()``.

    The consumer thread is stopped first. ``pause()`` makes the consume loop
    exit while ``running`` is still true, and Dramatiq then treats that as a
    consumer restart: it calls ``close()`` and sleeps. Stopping the thread
    leaves the delay queue in place so the replacement itself can be observed.
    """

    prom = _prom()

    def declare(broker):
        @actor(broker=broker, queue_name="billing")
        def do_work():
            return None

    with running_worker(prom, declare) as (broker, worker):
        broker.get_actor("do_work").send_with_options(delay=3_600_000)
        consumer = worker.consumers[dq_name("billing")]
        wait_until(
            lambda: (
                consumer.delay_queue.qsize() == 1 and bool(prom.delayed_message_ids)
            ),
            detail="delayed message was not pulled into worker memory",
        )
        consumer.stop()
        consumer.join(timeout=2)
        assert not consumer.is_alive()
        assert prom.delayed_message_ids
        consumer.delay_queue = PriorityQueue()
        assert prom.delayed_message_ids == frozenset()
        assert _positive(prom.inprogress_delayed_messages) == {}


def test_worker_e2e_retry_without_explicit_delay_returns_to_zero() -> None:
    calls: list[int] = []
    prom = _prom()

    def declare(broker):
        @actor(
            broker=broker,
            queue_name="billing",
            max_retries=3,
            min_backoff=20,
            max_backoff=20,
        )
        def do_work():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("transient")

    with running_worker(prom, declare) as (broker, worker):
        broker.get_actor("do_work").send()
        broker.join("billing", timeout=5000)
        worker.join()

    assert len(calls) == 2
    assert samples(prom.total_retried_messages)[BILLING] == 1.0
    assert samples(prom.total_errored_messages)[BILLING] == 1.0
    assert samples(prom.total_messages)[BILLING] == 2.0
    assert _positive(prom.inprogress_delayed_messages) == {}
    assert _positive(prom.inprogress_messages) == {}
    assert prom.delayed_message_ids == frozenset()


def test_worker_e2e_explicit_retry_delay_returns_to_zero() -> None:
    calls: list[int] = []
    prom = _prom()

    def declare(broker):
        @actor(
            broker=broker,
            queue_name="billing",
            max_retries=1,
            min_backoff=10,
            max_backoff=10,
        )
        def do_work():
            calls.append(1)
            if len(calls) == 1:
                raise Retry("once", delay=30)

    with running_worker(prom, declare) as (broker, worker):
        broker.get_actor("do_work").send()
        broker.join("billing", timeout=5000)
        worker.join()

    assert calls == [1, 1]
    assert samples(prom.total_retried_messages)[BILLING] == 1.0
    assert _positive(prom.inprogress_delayed_messages) == {}
    assert prom.delayed_message_ids == frozenset()


def test_worker_e2e_plain_message_does_not_touch_the_delayed_gauge() -> None:
    prom = _prom()

    def declare(broker):
        @actor(broker=broker, queue_name="billing", max_retries=0)
        def do_work():
            return "ok"

    with running_worker(prom, declare) as (broker, worker):
        broker.get_actor("do_work").send()
        broker.join("billing", timeout=5000)
        worker.join()

    assert samples(prom.inprogress_delayed_messages) == {}
    assert _positive(prom.inprogress_messages) == {}
    assert samples(prom.total_messages)[BILLING] == 1.0


def test_stock_label_bug_is_still_visible_after_the_queue_patch() -> None:
    """The queue patch must not hide the stock middleware's label split."""

    from dramatiq.middleware.prometheus import Prometheus as StockPrometheus

    calls: list[int] = []
    prom = StockPrometheus()

    def declare(broker):
        @actor(broker=broker, queue_name="billing")
        def do_work():
            calls.append(1)

    with running_worker(prom, declare) as (broker, worker):
        broker.get_actor("do_work").send_with_options(delay=40)
        broker.join("billing", timeout=5000)
        worker.join()
        observed = samples(prom.inprogress_delayed_messages)

    assert calls == [1]
    assert observed[BILLING_DQ] == 1.0
    assert observed[BILLING] == -1.0


def _positive_from(values: dict[tuple, float]) -> dict[tuple, float]:
    return {labels: value for labels, value in values.items() if value != 0.0}
