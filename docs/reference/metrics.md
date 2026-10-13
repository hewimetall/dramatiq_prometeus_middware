# Метрики

Имена совпадают с `dramatiq.middleware.prometheus.Prometheus`. Лейбл `queue_name` — всегда каноническое имя: `billing`, не `billing.DQ` и не `billing.XQ`. Лейбл `actor_name` — имя актора.

| Имя | Тип | `multiprocess_mode` | Что считает |
| --- | --- | --- | --- |
| `dramatiq_messages_total` | counter | — | Вызовы, дошедшие до `after_process_message`, включая исключения |
| `dramatiq_message_errors_total` | counter | — | Те же вызовы, у которых `exception is not None` |
| `dramatiq_message_retries_total` | counter | — | `after_enqueue`, если `delay is not None` и в options есть `retries` |
| `dramatiq_message_rejects_total` | counter | — | `after_nack` |
| `dramatiq_messages_inprogress` | gauge | `livesum` | Сообщения между `before_process_message` и `after_process_message` / `after_skip_message` |
| `dramatiq_delayed_messages_inprogress` | gauge | `livesum` | Сообщения в памяти этого процесса, от `before_delay_message` до выхода из `delay_queue` |
| `dramatiq_message_duration_milliseconds` | histogram | — | `current_millis()` конца минус отметка из `before_process_message` |

Корзины гистограммы те же, что у Dramatiq 2.2.1: `5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, 30000, 60000, 600000, 900000, +Inf`.

Текст помощи delayed gauge оставлен штатный: «The number of delayed messages in memory.»

Пока сообщение лежит в брокере в `<queue>.DQ` и ещё не прочитано воркером, delayed gauge его не включает. Рост начинается в `before_delay_message`, когда consumer уже положил его в `ConsumerThread.delay_queue`.

Повторный `before_delay_message` с тем же `message_id` в том же процессе второй раз gauge не увеличивает.

`before_ack` и `before_nack` снимают id, если он есть в локальном множестве. Второй ack того же id значение не уменьшает.

`before_process_message` увеличивает только `dramatiq_messages_inprogress`.
