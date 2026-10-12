"""Reproduce the Prometheus delayed-gauge bugs in Dramatiq 2.2.1.

These tests describe the stock middleware. They fail if an upgrade fixes the
bugs, which is the signal to drop the corresponding workaround.
"""

from __future__ import annotations

import os

from dramatiq.message import Message
from dramatiq.middleware.prometheus import Prometheus
from helpers import running_worker, samples

BILLING = ("billing", "do_work")
BILLING_DQ = ("billing.DQ", "do_work")


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


def test_mark_process_dead_leaves_default_gauge_files(tmp_path) -> None:
    """livesum files are deleted. mode=all files, which stock uses, stay."""

    from prometheus_client import multiprocess

    dead_pid = 2_100_000_001
    live_file = tmp_path / f"gauge_livesum_{dead_pid}.db"
    all_file = tmp_path / f"gauge_all_{dead_pid}.db"
    counter_file = tmp_path / f"counter_{dead_pid}.db"
    for path in (live_file, all_file, counter_file):
        path.write_bytes(b"")

    multiprocess.mark_process_dead(dead_pid, os.fspath(tmp_path))

    assert not live_file.exists()
    assert all_file.exists()
    assert counter_file.exists()


def test_stock_delayed_gauge_is_not_livesum() -> None:
    prom = Prometheus()
    _boot(prom)
    assert prom.inprogress_messages._multiprocess_mode == "livesum"
    assert prom.inprogress_delayed_messages._multiprocess_mode == "all"


def test_stock_same_process_splits_queue_labels() -> None:
    """Increment lands on queue.DQ. Decrement lands on the canonical queue."""

    prom = Prometheus()
    _boot(prom)
    delayed = _delayed("billing.DQ", "same-process")
    prom.before_delay_message(None, delayed)

    processed = delayed.copy(queue_name="billing", options={"eta": None})
    del processed.options["eta"]
    prom.before_process_message(None, processed)

    assert samples(prom.inprogress_delayed_messages) == {BILLING_DQ: 1.0, BILLING: -1.0}
    assert prom.delayed_messages == set()


def test_stock_other_process_never_decrements_and_leaks_the_id() -> None:
    holder = Prometheus()
    other = Prometheus()
    _boot(holder)
    _boot(other)
    delayed = _delayed("billing.DQ", "other-process")
    holder.before_delay_message(None, delayed)

    processed = delayed.copy(queue_name="billing")
    del processed.options["eta"]
    other.before_process_message(None, processed)
    other.after_process_message(None, processed)

    assert samples(holder.inprogress_delayed_messages) == {BILLING_DQ: 1.0}
    assert samples(other.inprogress_delayed_messages) == {}
    assert holder.delayed_messages == {"other-process"}


def test_stock_retry_counter_includes_the_delay_queue_reenqueue() -> None:
    """The ETA handoff enqueues the same retry again, so the counter moves by 2."""

    calls: list[int] = []
    prom = Prometheus()

    def declare(broker):
        from dramatiq import actor

        @actor(
            broker=broker,
            queue_name="billing",
            max_retries=2,
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
    assert samples(prom.total_retried_messages)[("billing.DQ", "do_work")] == 1.0
    assert samples(prom.total_retried_messages)[("billing", "do_work")] == 1.0


def test_stock_worker_e2e_leaves_positive_dq_and_negative_canonical() -> None:
    calls: list[str] = []
    prom = Prometheus()

    def declare(broker):
        from dramatiq import actor

        @actor(broker=broker, queue_name="billing")
        def do_work():
            calls.append(do_work.actor_name)

        return do_work

    with running_worker(prom, declare) as (broker, worker):
        do_work = declare_again(broker)
        do_work.send_with_options(delay=50)
        broker.join(do_work.queue_name, timeout=5000)
        worker.join()
        observed = samples(prom.inprogress_delayed_messages)

    assert calls == ["do_work"]
    assert observed[BILLING_DQ] == 1.0
    assert observed[BILLING] == -1.0
    assert prom.delayed_messages == set()


def declare_again(broker):
    return broker.get_actor("do_work")
