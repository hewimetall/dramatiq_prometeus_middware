# Первое отложенное сообщение

В этом уроке мы подключаем `dramatiq_prometheus_fix.Prometheus` вместо штатного middleware, отправляем одно сообщение с `delay=50` и смотрим, что `dramatiq_delayed_messages_inprogress` вернулся к нулю на каноническом имени очереди.

К концу урока в выводе есть строка `actor ran`, множество отслеживаемых id пустое, серии `billing.DQ` нет.

Нужны Python 3.12 и зависимости этого репозитория (`uv sync`). Брокер здесь учебный `StubBroker`: баг сидит в middleware и в памяти воркера, отдельный Redis для этого шага не нужен.

## Шаг 1. Middleware и актор

`multiprocess=False` в уроке оставляет числа на объекте gauge, без каталога mmap. В рабочем процессе этот аргумент не передают: по умолчанию он `True`.

`emit_after("process_boot")` создаёт метрики. CLI Dramatiq делает это сам, до старта потоков. Без этого хуки метрик ещё нет.

```python
from dramatiq import actor
from dramatiq.brokers.stub import StubBroker
from dramatiq.worker import Worker

from dramatiq_prometheus_fix import Prometheus

prom = Prometheus(expose=False, multiprocess=False)
broker = StubBroker()
broker.add_middleware(prom)


@actor(broker=broker, queue_name="billing")
def do_work() -> None:
    print("actor ran")


broker.emit_after("process_boot")
```

## Шаг 2. Воркер и сообщение с задержкой

```python
worker = Worker(broker, worker_threads=2, worker_timeout=50)
worker.start()
try:
    do_work.send_with_options(delay=50)
    broker.join("billing", timeout=5000)
    worker.join()
finally:
    worker.stop()
```

`actor ran` появляется после того, как ETA прошло и сообщение обработано.

## Шаг 3. Смотрим gauge

```python
print("tracked ids:", sorted(prom.delayed_message_ids))
for labels, child in prom.inprogress_delayed_messages._metrics.items():
    print("series", tuple(labels), "=", child._value.get())
```

Ожидаемый вывод:

```text
actor ran
tracked ids: []
series ('billing', 'do_work') = 0.0
delayed gauge settled on the canonical queue name
```

Серия `('billing.DQ', 'do_work')` не создаётся. Ненулевых значений нет.

Готовый файл этого урока:

```bash
uv run python docs/tutorials/examples/delayed_gauge.py
```

Дальше: {doc}`/how-to/clear-multiprocess-directory`, справочник {doc}`/reference/what`, объяснение {doc}`/explanation/why`.
