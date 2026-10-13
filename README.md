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

Документация собрана по шаблону [Diátaxis](https://diataxis.fr/map/)
(tutorial / how-to / reference / explanation) в каталоге Sphinx `docs/`.
Разбор штатного middleware туда перенесён целиком:

- [Что сломано](docs/reference/what.md) — серии, файлы, счётчики, номера issue, тесты.
- [Почему](docs/explanation/why.md) — путь через `delay_queue`, метки, чужой процесс, mmap.
- [Что сюда не входит](docs/explanation/out-of-scope.md) — отчёты брокера про `.DQ` и доставку.

```bash
uv run sphinx-build -b html docs docs/_build/html
uv run pytest
```
