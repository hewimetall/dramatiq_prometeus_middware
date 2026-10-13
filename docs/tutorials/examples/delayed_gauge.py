"""Lesson: one delayed message, then the delayed gauge is back at zero.

Run from the repository root:

    uv run python docs/tutorials/examples/delayed_gauge.py
"""

from dramatiq import actor
from dramatiq.brokers.stub import StubBroker
from dramatiq.worker import Worker

from dramatiq_prometheus_fix import Prometheus


def main() -> None:
    prom = Prometheus(expose=False, multiprocess=False)
    broker = StubBroker()
    broker.add_middleware(prom)

    @actor(broker=broker, queue_name="billing")
    def do_work() -> None:
        print("actor ran")

    broker.emit_after("process_boot")
    worker = Worker(broker, worker_threads=2, worker_timeout=50)
    worker.start()
    try:
        do_work.send_with_options(delay=50)
        broker.join("billing", timeout=5000)
        worker.join()
        print("tracked ids:", sorted(prom.delayed_message_ids))
        children = prom.inprogress_delayed_messages._metrics.items()
        for labels, child in children:
            print("series", tuple(labels), "=", child._value.get())
        nonzero = [labels for labels, child in children if child._value.get() != 0]
        dq = [labels for labels, _child in children if str(labels[0]).endswith(".DQ")]
        if nonzero or dq:
            raise SystemExit(f"gauge did not settle: nonzero={nonzero} dq={dq}")
        print("delayed gauge settled on the canonical queue name")
    finally:
        worker.stop()


if __name__ == "__main__":
    main()
