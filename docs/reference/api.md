# API

Импорт:

```python
from dramatiq_prometheus_fix import (
    Prometheus,
    ReapingMultiProcessCollector,
    prepare_multiproc_dir,
    reap_dead_processes,
    resolve_db_path,
    serve_exposition,
)
```

## `Prometheus`

Подкласс `dramatiq.middleware.Middleware`. В брокер добавляется вместо `dramatiq.middleware.prometheus.Prometheus`.

```python
Prometheus(
    *,
    db_path: str | os.PathLike[str] | None = None,
    expose: bool = True,
    host: str | None = None,
    port: int | None = None,
    multiprocess: bool = True,
)
```

| Параметр | Смысл |
| --- | --- |
| `db_path` | Каталог mmap. Пустое значение — `resolve_db_path()`. |
| `expose` | `True`: свойство `forks` содержит exposition-сервер, его поднимает CLI Dramatiq. |
| `host` | Адрес сервера. Пишется в `DRAMATIQ_PROM_HOST` и `dramatiq_prom_host`. |
| `port` | Порт сервера. Пишется в `DRAMATIQ_PROM_PORT` и `dramatiq_prom_port`. |
| `multiprocess` | Писать mmap и вызывать `mark_process_dead` в `after_worker_shutdown`. `False` — только для однопроцессных тестов. |

`delayed_message_ids` — `frozenset[str]`, id, которые этот процесс сейчас считает отложенными.

Хуки, которые меняют учёт delayed gauge:

| Хук | Действие |
| --- | --- |
| `after_process_boot` | Создаёт метрики. При `multiprocess=True` готовит каталог без удаления файлов и выставляет переменные окружения до импорта `prometheus_client`. |
| `before_worker_boot` | Подключает наблюдение за `ConsumerThread.delay_queue`. |
| `before_delay_message` | Запоминает id и увеличивает gauge. |
| `before_ack`, `before_nack` | Снимает id, если он есть. |
| `release_queue` | Снимает id сообщений, которые ещё лежат в куче `queue.PriorityQueue`, саму очередь не опустошает. |
| `after_worker_shutdown` | Снимает оставшиеся id, отключает наблюдение, вызывает `mark_process_dead` для pid этого процесса. |

Наблюдение за очередью ставится один раз на класс `dramatiq.worker.ConsumerThread`: подмена атрибута `delay_queue` и обёртка `close`. Замена объекта очереди и `close()` сообщают зарегистрированным экземплярам `Prometheus`.

`serve_exposition(host=None, port=None)` возвращает `HTTPServer`. Порт `0` выбирает свободный порт; фактический адрес — `server_address`.

## `resolve_db_path(path=None) -> str`

Явный `path`, иначе первая непустая переменная из списка: `DRAMATIQ_PROM_DB`, `dramatiq_prom_db`, `PROMETHEUS_MULTIPROC_DIR`, `prometheus_multiproc_dir`. Иначе `$TMPDIR/dramatiq-prometheus`.

## `prepare_multiproc_dir(path=None, *, clean=False) -> str`

Создаёт каталог. При `clean=True` удаляет в нём файлы с суффиксом `.db`. Затем записывает путь во все четыре переменные из `resolve_db_path`.

## `reap_dead_processes(path=None) -> list[int]`

Для pid из имён `gauge_*_<pid>.db`: если pid не текущий и `os.kill(pid, 0)` не находит процесс, вызывается `mark_process_dead`. Возвращает снятые pid по возрастанию. Каталога нет — пустой список.

## `ReapingMultiProcessCollector(registry, path=None)`

Регистрирует себя в `registry`. Перед `collect` вызывает `reap_dead_processes`. Внутренний `MultiProcessCollector` в registry не регистрируется.

## CLI

Точка входа `dramatiq-prometheus` и `python -m dramatiq_prometheus_fix`.

Аргументы после имени команды передаются `dramatiq` как есть. Своего разбора флагов Dramatiq нет, кроме пропуска подготовки каталога для `-h` и `--help`.
