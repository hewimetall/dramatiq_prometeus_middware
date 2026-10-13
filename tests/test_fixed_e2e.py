"""Hook-level checks for the replacement Prometheus middleware.

Worker tests live in ``test_prometheus_e2e.py`` and follow Dramatiq's
``stub_broker`` / ``stub_worker`` fixtures.
"""

from __future__ import annotations

from queue import PriorityQueue

from dramatiq.message import Message

from dramatiq_prometheus_fix import Prometheus
from helpers import samples

BILLING = ("billing", "do_work")


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
