"""Watch Dramatiq's in-memory delay queue.

A delayed message is counted while it sits in ``ConsumerThread.delay_queue``.
The worker removes it from that queue in three places that do not share one
middleware hook:

* the ETA elapsed and the original message is acknowledged;
* ``close()`` requeues whatever is still in the queue (shutdown and consumer
  restart);
* a ``BrokerConnectionError`` replaces the queue object and drops its contents
  on the floor (the broker redelivers the unacked messages later).

Acknowledgement is handled by the middleware hooks. The other two paths only
touch the queue object, so this module installs a data descriptor and wraps
``ConsumerThread.close``. Both notify the live middleware instances.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Protocol

logger = logging.getLogger(__name__)

_listeners: list[DelayQueueListener] = []
_patch_lock = threading.Lock()
_patched = False

_QUEUE_ATTR = "_dramatiq_prometheus_fix_delay_queue"


class DelayQueueListener(Protocol):
    def release_queue(self, queue: Any) -> None:
        """Drop in-memory delayed messages still stored in ``queue``."""


def register(listener: DelayQueueListener) -> None:
    install()
    if listener not in _listeners:
        _listeners.append(listener)


def unregister(listener: DelayQueueListener) -> None:
    try:
        _listeners.remove(listener)
    except ValueError:
        return


def install() -> None:
    """Patch ``ConsumerThread`` once so queue replacement and ``close`` are visible."""

    global _patched
    with _patch_lock:
        if _patched:
            return
        from dramatiq.worker import ConsumerThread

        original_close = ConsumerThread.close

        def close(self: Any) -> None:
            queue = self.delay_queue
            try:
                _notify(queue)
            finally:
                original_close(self)

        class _DelayQueue:
            def __get__(self, obj: Any, objtype: type | None = None) -> Any:
                if obj is None:
                    return self
                if _QUEUE_ATTR in obj.__dict__:
                    return obj.__dict__[_QUEUE_ATTR]
                return obj.__dict__.get("delay_queue")

            def __set__(self, obj: Any, value: Any) -> None:
                if _QUEUE_ATTR in obj.__dict__:
                    previous = obj.__dict__[_QUEUE_ATTR]
                else:
                    previous = obj.__dict__.get("delay_queue")
                if previous is not None and previous is not value:
                    _notify(previous)
                obj.__dict__[_QUEUE_ATTR] = value

        ConsumerThread.delay_queue = _DelayQueue()  # type: ignore[assignment]
        ConsumerThread.close = close  # type: ignore[method-assign]
        _patched = True


def _notify(queue: Any) -> None:
    # Copy: a listener may unregister itself while handling the queue.
    listeners = tuple(_listeners)
    for listener in listeners:
        try:
            listener.release_queue(queue)
        except Exception:
            logger.exception("Failed to release delayed messages from %r.", listener)


def messages_in_queue(queue: Any) -> list[Any]:
    """Return messages still sitting in a ``queue.PriorityQueue`` heap.

    The heap is peeked, not drained. ``ConsumerThread.close`` still needs the
    messages so it can requeue them to the broker.
    """

    if queue is None:
        return []
    heap = getattr(queue, "queue", None)
    if not heap:
        return []
    messages: list[Any] = []
    for item in list(heap):
        message = getattr(item, "message", None)
        if message is not None and getattr(message, "message_id", None):
            messages.append(message)
    return messages
