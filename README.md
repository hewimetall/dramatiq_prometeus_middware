# dramatiq-prometheus-fix

Drop-in replacement for `dramatiq.middleware.prometheus.Prometheus`.

Same metric names. `queue_name` is always the canonical queue (`billing`, never
`billing.DQ`). `dramatiq_delayed_messages_inprogress` counts messages sitting
in that process's memory, and it returns to zero when they leave.

```python
from dramatiq.brokers.redis import RedisBroker
from dramatiq_prometheus_fix import Prometheus

broker = RedisBroker()
broker.add_middleware(Prometheus())
```

Start workers through the wrapper so the multiprocess directory is cleared
once, in the parent, before any worker is forked:

```bash
dramatiq-prometheus tasks:broker -p 4 -t 8
```

The wrapper prints the directory it is using. Override it with
`DRAMATIQ_PROM_DB`. `DRAMATIQ_PROM_CLEAN=0` skips the wipe. `-h` / `--help`
are passed through to Dramatiq and do not touch the directory.

Requires Python 3.12+ and Dramatiq 1.15+ (verified against 2.2.1).

## What the stock middleware does

Checked against Dramatiq 2.2.1 (`dramatiq/middleware/prometheus.py` and
`dramatiq/worker.py`). Issues
[#627](https://github.com/Bogdanp/dramatiq/issues/627) and
[#672](https://github.com/Bogdanp/dramatiq/issues/672) are still open.
Pull request #627 was not merged. Pull request #876 describes the label split
and was closed without a merge.

`dramatiq_delayed_messages_inprogress` is documented as "The number of delayed
messages in memory." A delayed message (an explicit `delay`, or a retry with
backoff) is stored on the broker's `<queue>.DQ`. The worker prefetches it
immediately, emits `before_delay_message`, and holds it in
`ConsumerThread.delay_queue` until `options["eta"]`. When the ETA passes,
`handle_delayed_messages` copies the message onto the canonical queue with
`q_name()`, deletes `eta`, enqueues that copy, and acknowledges the `.DQ`
copy. Some worker then processes the copy. The message id is unchanged.

### 1. Increment and decrement use different `queue_name` labels

`before_delay_message` does:

```python
labels = (message.queue_name, message.actor_name)  # ("billing.DQ", "do_work")
self.inprogress_delayed_messages.labels(*labels).inc()
```

`before_process_message` decrements with the labels of the message it is
about to run. That message was requeued with the canonical name, so the
decrement is `("billing", "do_work")`.

After one delayed message in a single process the gauge is:

```text
dramatiq_delayed_messages_inprogress{queue_name="billing.DQ"}  1
dramatiq_delayed_messages_inprogress{queue_name="billing"}    -1
```

The `.DQ` series never returns to zero. The canonical series drifts negative.
This is issue #627, and it happens even when the same process both delays and
runs the message. The in-memory id set is empty afterwards, so this case is
not the leak in #672.

### 2. Another process never decrements, and the id set leaks

The id is recorded in `self.delayed_messages` on the middleware instance that
ran `before_delay_message`. That object is process-local. `before_process_message`
decrements only when the id is in its own set:

```python
if message.message_id in self.delayed_messages:
    self.delayed_messages.remove(message.message_id)
    self.inprogress_delayed_messages.labels(*labels).dec()
```

The process that runs the requeued message often is not the process that held
it. Its set does not contain the id, so it does not decrement. The holding
process never sees `before_process_message` for that id either, because the
ack of the `.DQ` copy is not where the stock middleware decrements. The
`.DQ` series stays positive and `self.delayed_messages` keeps the id for the
life of the process. That is issue #672. The leaked set is a consequence of
the same decrement site, not a second independent bug.

Retries take this path too. `Retries` enqueues with `delay=...`, which is a
delayed message even when the actor was sent with no `delay`.

### 3. The delayed gauge is not a live multiprocess gauge

`dramatiq_messages_inprogress` is created with `multiprocess_mode="livesum"`.
`dramatiq_delayed_messages_inprogress` is not, so it uses the default `"all"`.

`prometheus_client.multiprocess.mark_process_dead` deletes only
`gauge_live*_<pid>.db`. A `gauge_all_<pid>.db` file is not deleted, and mode
`all` keeps that series in the scrape (with a `pid` label) after the process
is gone. `livesum` is not a liveness check either: `MultiProcessCollector`
adds up every `gauge_livesum_*.db` still on disk and does not call `os.kill`.
A crashed worker's in-progress gauge remains until something deletes the file.
Graceful shutdown does call `mark_process_dead`, which clears `livesum` and
does nothing for the delayed gauge.

### 4. Leaving memory is not what decrements the gauge

The number the metric claims to report is "delayed messages in memory." The
stock code decrements on `before_process_message` instead. Messages leave
`delay_queue` without being processed when:

* the worker shuts down and `ConsumerThread.close` requeues the delay queue;
* the consumer restarts and `close` requeues it;
* `BrokerConnectionError` replaces the queue (`self.delay_queue = PriorityQueue()`),
  dropping the messages. They are still unacked, so the broker delivers them
  again later.

None of those paths emit `before_process_message`. The gauge and the id set
stay at the old values, and a later redelivery increments again.

These last two paths are visible in `dramatiq/worker.py`. They are not
separate GitHub reports; they are the same accounting, triggered by shutdown
and reconnect.

### 5. The multiprocess directory is not cleared, and it is shared by default

The stock module says `DB_PATH` is cleared before every run. The code only
runs `os.makedirs`. The default path is `$TMPDIR/dramatiq-prometheus` for
every Dramatiq instance on the machine, so two deployments scrape each
other's files, and a restart keeps the previous run's gauges. Container
restarts that keep the same tmpfs do this too. Pid reuse then merges the new
process with the old file.

`mark_process_dead` on a clean exit does not fix mode `all`, and a `SIGKILL`
never reaches `after_worker_shutdown`.

Broker bugs (duplicate delivery, RabbitMQ `consumer_timeout`, Redis Lua stack,
bad ETA, pipelines, flush of `.DQ`, `join()` races) are not Prometheus
accounting bugs and this package does not change them.

## What this package changes

* Both the increment and the decrement use `q_name(message.queue_name)`.
* The holding process forgets the message when that message leaves its delay
  queue: on ack or nack of the `.DQ` copy, on `close()` (shutdown and
  consumer restart), and when the delay queue object is replaced after a
  connection error. A second ack of the same id, from the worker that runs
  the requeued copy, is a no-op.
* `before_process_message` does not touch the delayed gauge.
* `dramatiq_message_retries_total` increments when a retry is enqueued with
  a delay. The later enqueue from the delay queue back onto the canonical
  queue is the same retry, so it is not counted again. Stock Dramatiq
  increments on every `after_enqueue` that still carries `retries`, including
  that second hop.
* `dramatiq_delayed_messages_inprogress` uses `multiprocess_mode="livesum"`,
  same as `dramatiq_messages_inprogress`.
* `after_worker_shutdown` drops anything still tracked, then calls
  `mark_process_dead`.
* `dramatiq-prometheus` wipes `*.db` in the parent and execs `dramatiq`.
* `reap_dead_processes` and `ReapingMultiProcessCollector` delete live-gauge
  files whose pid is not running, so a scrape does not keep a crashed
  worker. Counters and histograms are left in place.

Wiping the directory resets counters for that process group. That is what the
Prometheus client documents: do it once before the group starts, not inside
each worker. Do not point the wipe at a directory that a live web process is
already using. If Dramatiq and the application share one directory, wipe once
in the entrypoint that starts both, and call `reap_dead_processes` (or use
`ReapingMultiProcessCollector`) on the application's metrics endpoint:

```python
from prometheus_client import CollectorRegistry, generate_latest
from dramatiq_prometheus_fix import ReapingMultiProcessCollector

def render_metrics() -> bytes:
    registry = CollectorRegistry()
    ReapingMultiProcessCollector(registry)
    return generate_latest(registry)
```

`reap_dead_processes` cannot see pid reuse. A pid that has already been
recycled looks alive, and its old file is kept. The parent wipe covers that.

Exposition defaults match Dramatiq: `0.0.0.0:9191`, overridable with
`DRAMATIQ_PROM_HOST` / `dramatiq_prom_host` and `DRAMATIQ_PROM_PORT` /
`dramatiq_prom_port`. The metrics server reaps dead workers before each scrape.

## Tests

```bash
uv run pytest
```

The suite is Python 3.12. It reproduces the stock label split and the
cross-process leak, runs a real `Worker` against `StubBroker` for delay,
retry, shutdown, and a replaced delay queue, and uses a second interpreter
for mmap files and the HTTP exposition server. The stub broker is enough:
Redis and RabbitMQ deliver the same `before_delay_message` / ack sequence
once the worker has the message.
