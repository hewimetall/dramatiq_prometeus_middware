# Что сломано

Факты по коду и прогону Dramatiq **2.2.1**, Python 3.12. Это описание штатного `dramatiq.middleware.prometheus.Prometheus` и `prometheus_client` 0.26.0. Объяснение причин — в {doc}`/explanation/why`.

Источники:

- код `dramatiq/middleware/prometheus.py`, `dramatiq/worker.py`, `dramatiq/middleware/retries.py`;
- [issue 627](https://github.com/Bogdanp/dramatiq/issues/627), открыт, pull request 627 не влит;
- [issue 672](https://github.com/Bogdanp/dramatiq/issues/672), открыт, метка `prometheus`;
- pull request 876 закрыт без merge;
- тесты `tests/test_stock_bugs.py` и `tests/test_multiproc_e2e.py` этого репозитория.

## 1. Разные `queue_name` у увеличения и уменьшения

| | |
| --- | --- |
| Метрика | `dramatiq_delayed_messages_inprogress` |
| Увеличение | `Prometheus.before_delay_message`, метки `(message.queue_name, message.actor_name)` |
| Уменьшение | `Prometheus.before_process_message`, метки сообщения, которое уже переложено на каноническую очередь |
| Issue | [#627](https://github.com/Bogdanp/dramatiq/issues/627) |
| Тест | `test_stock_same_process_splits_queue_labels`, `test_stock_worker_e2e_leaves_positive_dq_and_negative_canonical` |

Наблюдение после одного сообщения актора `do_work` в очереди `billing`, один процесс, воркер доработал сообщение:

```text
dramatiq_delayed_messages_inprogress{queue_name="billing.DQ",actor_name="do_work"}  1
dramatiq_delayed_messages_inprogress{queue_name="billing",actor_name="do_work"}    -1
```

`self.delayed_messages` после обработки пуст.

В этом пакете обе операции пишут каноническое имя. Серия `.DQ` не создаётся. После обработки значение `("billing", "do_work")` равно `0`.

## 2. Уменьшение только в том процессе, который запомнил id

| | |
| --- | --- |
| Хранилище | `self.delayed_messages: set[str]` на экземпляре middleware |
| Условие уменьшения | `if message.message_id in self.delayed_messages` внутри `before_process_message` |
| Issue | [#672](https://github.com/Bogdanp/dramatiq/issues/672) |
| Тест | `test_stock_other_process_never_decrements_and_leaks_the_id` |

Наблюдение. Экземпляр A вызывает `before_delay_message` для id `other-process` с очередью `billing.DQ`. Экземпляр B вызывает `before_process_message` и `after_process_message` для того же id с очередью `billing`.

| Экземпляр | Gauge | Множество id |
| --- | --- | --- |
| A | `("billing.DQ", "do_work") = 1` | `{"other-process"}` |
| B | дочерних серий delayed нет | пусто |

В этом пакете процесс, который вызвал `before_delay_message`, снимает учёт в `before_ack` / `before_nack` этой копии и при уходе сообщения из локальной delay queue. `before_process_message` delayed gauge не меняет. Повторный ack того же id — пустая операция.

## 3. У delayed gauge режим multiprocess по умолчанию `all`

| Метрика | `multiprocess_mode` в 2.2.1 |
| --- | --- |
| `dramatiq_messages_inprogress` | `livesum` |
| `dramatiq_delayed_messages_inprogress` | `all` |

Тест: `test_stock_delayed_gauge_is_not_livesum`.

`prometheus_client.multiprocess.mark_process_dead` удаляет файлы `gauge_<live-mode>_<pid>.db`. Проверка на пустых файлах (`test_mark_process_dead_leaves_default_gauge_files`):

| Файл | После `mark_process_dead` |
| --- | --- |
| `gauge_livesum_<pid>.db` | удалён |
| `gauge_all_<pid>.db` | остаётся |
| `counter_<pid>.db` | остаётся |

В этом пакете обе gauge-метрики создаются с `multiprocess_mode="livesum"`.

## 4. `livesum` суммирует файлы, которые ещё лежат на диске

`MultiProcessCollector` в prometheus_client 0.26.0 читает `*.db` и для режима `livesum` складывает значения. Вызова `os.kill` в сборщике нет.

Наблюдение (`test_livesum_keeps_a_dead_process_until_reap`): процесс выставил `dramatiq_messages_inprogress{queue_name="billing",actor_name="do_work"} = 7`, затем процесс убит.

| Момент | Значение в выдаче |
| --- | --- |
| Процесс ещё жив | `7.0` |
| Процесс мёртв, файл на месте, без reap | `7.0` |
| После `reap_dead_processes` | серии нет |

`after_worker_shutdown` штатного middleware вызывает `mark_process_dead`. Для режима `all` это файл delayed gauge не удаляет. Для `SIGKILL` хук не вызывается.

## 5. Уменьшение стоит на `before_process_message`

Документация метрики в коде Dramatiq: «The number of delayed messages in memory.»

Штатное уменьшение выполняется в `before_process_message`.

Сообщение покидает `ConsumerThread.delay_queue` ещё в таких местах `dramatiq/worker.py`:

| Место | Что делает код | Хук `before_process_message` |
| --- | --- | --- |
| `ConsumerThread.close` | `requeue_messages` по содержимому delay queue. Вызов при остановке воркера и при рестарте consumer | нет |
| ветка `BrokerConnectionError` в `ConsumerThread.run` | `self.delay_queue = PriorityQueue()` | нет |

Отдельных issue на эти две ветки нет. Это тот же учёт, другие выходы из памяти.

В этом пакете списание есть на `before_ack`, `before_nack`, на `close()` и на подмену объекта `delay_queue`. Тесты: `test_worker_e2e_shutdown_forgets_messages_still_in_memory`, `test_worker_e2e_replacing_delay_queue_releases_the_gauge`.

## 6. Каталог multiprocess не очищается

Комментарий в `dramatiq/middleware/prometheus.py`: путь «cleared before every run».

Код модуля при импорте:

```python
DB_PATH = os.getenv("dramatiq_prom_db", "%s/dramatiq-prometheus" % tempfile.gettempdir())
os.makedirs(DB_PATH, exist_ok=True)
```

Удаления `*.db` нет. Каталог по умолчанию один на машину: `$TMPDIR/dramatiq-prometheus`. `PROMETHEUS_MULTIPROC_DIR` выставляется в `after_process_boot` уже внутри процесса воркера.

Следствия, которые из этого прямо следуют:

- файлы предыдущего запуска остаются;
- два инстанса с одним каталогом попадают в один scrape;
- повторное использование pid склеивает новый процесс со старым файлом.

В этом пакете стирание делает команда `dramatiq-prometheus` в родительском процессе. Воркер каталог не чистит.

## 7. `dramatiq_message_retries_total` увеличивается дважды на один retry

Штатный `after_enqueue`: если в `message.options` есть ключ `retries`, счётчик растёт. Задержка в условии не участвует.

Наблюдение одного retry актора `do_work` / очередь `billing` (`test_stock_retry_counter_includes_the_delay_queue_reenqueue`): актор выполнился дважды (ошибка, затем успех), счётчик:

```text
dramatiq_message_retries_total{queue_name="billing.DQ",actor_name="do_work"}  1
dramatiq_message_retries_total{queue_name="billing",actor_name="do_work"}      1
```

В этом пакете счётчик растёт, когда `delay is not None` и ключ `retries` есть. Перекладка после ETA идёт с `delay=None` и в счётчик не входит. На том же сценарии значение одно: `("billing", "do_work") = 1`. Тест: `test_worker_e2e_retry_without_explicit_delay_returns_to_zero`.

## Что этот пакет этим фактам сопоставляет

| Факт в 2.2.1 | Поведение `dramatiq_prometheus_fix.Prometheus` |
| --- | --- |
| Метки `.DQ` и каноническое имя | `q_name(message.queue_name)` на всех метриках |
| Уменьшение в процессе обработчика | Уменьшение в процессе, который держит сообщение в `delay_queue` |
| `multiprocess_mode="all"` у delayed gauge | `livesum` у delayed и у in-progress |
| Файл мёртвого процесса в scrape | `reap_dead_processes` и `ReapingMultiProcessCollector` |
| Каталог не стирается | `dramatiq-prometheus` стирает `*.db` до `exec` |
| Retry считается на двух enqueue | Считается enqueue, у которого `delay is not None` |
