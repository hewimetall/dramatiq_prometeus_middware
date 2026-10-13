# Как понять, есть ли у вас отложенные сообщения

Цель: решить, относится ли `dramatiq_delayed_messages_inprogress` к этому процессу.

Сообщение становится отложенным в двух случаях.

1. Вызов `send_with_options(delay=...)` или `send_with_options` с `delay`.
2. Повтор из middleware `Retries`. После исключения оно делает `broker.enqueue(message, delay=delay)`. Даже если актор отправили без `delay`, retry кладёт сообщение в `<queue>.DQ`.

Если у актора `max_retries=0` и задержку вы не задаёте, delayed gauge для этих задач не растёт. Искать ошибку именно `dramatiq_delayed_messages_inprogress` в таком процессе не нужно.

Остаются ошибки каталога multiprocess. Они касаются `dramatiq_messages_inprogress` и остальных gauge с режимом `livesum`:

- файлы прошлого запуска, если каталог не стёрт;
- несколько инстансов в одном каталоге;
- gauge после падения процесса, пока файл не удален.

Как их закрыть: {doc}`/how-to/clear-multiprocess-directory` и {doc}`/how-to/reap-dead-workers`.

Почему retry проходит через ту же очередь, что и явный `delay`: {doc}`/explanation/why`.
