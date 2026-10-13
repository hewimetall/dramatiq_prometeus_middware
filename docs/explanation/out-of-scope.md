# Почему ошибки брокера сюда не входят

Ниже перечислены публичные отчёты Dramatiq про задержку, очереди `.DQ` и доставку. Они не являются дефектами `dramatiq.middleware.prometheus.Prometheus`. Этот пакет их не меняет. Исторический отчёт не означает, что дефект есть в 2.2.1: это надо проверять отдельно по коду брокера.

| Отчёт | О чём он | Почему это не класс Prometheus |
| --- | --- | --- |
| [#431](https://github.com/Bogdanp/dramatiq/issues/431) | Дубли отложенных сообщений в Redis, повторная или параллельная обработка | Доставка и хранение в Redis, не учёт gauge |
| [#827](https://github.com/Bogdanp/dramatiq/issues/827) | Долгая задержка и `consumer_timeout` RabbitMQ: сообщение не подтверждено до ETA | Поведение канала RabbitMQ, пока воркер держит prefetch |
| [#433](https://github.com/Bogdanp/dramatiq/issues/433) | Большой delayed prefetch и переполнение стека Lua в Redis | Скрипт брокера Redis |
| [#761](https://github.com/Bogdanp/dramatiq/issues/761) | Некорректный ETA ломает consumer | Разбор `options["eta"]` в воркере до метрик |
| [#206](https://github.com/Bogdanp/dramatiq/issues/206) | Pipeline не применяет задержку первого сообщения | Middleware pipeline |
| [#148](https://github.com/Bogdanp/dramatiq/issues/148) | При остановке сообщение может остаться и в обычной очереди, и в delayed-очереди RabbitMQ | Подтверждения брокера |
| [#687](https://github.com/Bogdanp/dramatiq/issues/687) | `flush` не очищает `.DQ` | API брокера |
| [#546](https://github.com/Bogdanp/dramatiq/issues/546) | Имена `.DQ` попадают туда, где ждут канонические имена очередей | Имена очередей брокера. Совпадение с лейблом `queue_name` в метриках внешнее: метрики чинятся отдельно, объявление очередей — нет |
| [#481](https://github.com/Bogdanp/dramatiq/issues/481) | Гонка `join()` в Redis между delayed и обычной очередью | `Broker.join` |

Класс Prometheus в этом репозитории — то, что делает объект `Prometheus` и каталог `prometheus_client`: какие лейблы пишутся, в какой функции растёт и падает gauge, какой `multiprocess_mode` у серии, удаляется ли файл умершего процесса. Список этих фактов — {doc}`/reference/what`. Причины — {doc}`/explanation/why`.
