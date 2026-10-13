# Как убрать gauge умершего воркера из scrape

Цель: после падения процесса его `livesum`-gauge не входит в ответ `/metrics`.

Штатный `mark_process_dead` вызывается из `after_worker_shutdown`. До `SIGKILL` этот хук не доходит. Сборщик Prometheus сам pid не проверяет.

На endpoint приложения:

```python
from prometheus_client import CollectorRegistry, generate_latest

from dramatiq_prometheus_fix import ReapingMultiProcessCollector


def render_metrics() -> bytes:
    registry = CollectorRegistry()
    ReapingMultiProcessCollector(registry)
    return generate_latest(registry)
```

`ReapingMultiProcessCollector` перед чтением файлов вызывает `reap_dead_processes`. Удаляются только `gauge_live*_<pid>.db` тех pid, которых уже нет. Счётчики и гистограммы остаются.

Тот же вызов есть у встроенного exposition-сервера middleware на каждом `GET`.

Отдельный вызов без HTTP:

```python
from dramatiq_prometheus_fix import reap_dead_processes

reap_dead_processes("/var/lib/dramatiq/prom")
```

Функция возвращает список снятых pid. Pid текущего процесса она не трогает.

Повторно занятый pid выглядит живым, старый файл при этом сохраняется. Это закрывает очистка каталога в родителе: {doc}`/how-to/clear-multiprocess-directory`.
