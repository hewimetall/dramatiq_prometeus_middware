# Переменные окружения

Все четыре имени каталога этот пакет записывает одним значением, когда вызываются `prepare_multiproc_dir` или CLI.

| Переменная | Кто ещё её читает | Значение по умолчанию |
| --- | --- | --- |
| `DRAMATIQ_PROM_DB` | этот пакет, в приоритете над остальными | нет; тогда смотрится следующая |
| `dramatiq_prom_db` | штатный Dramatiq, `DB_PATH` при импорте модуля | `$TMPDIR/dramatiq-prometheus` |
| `PROMETHEUS_MULTIPROC_DIR` | `prometheus_client` | нет |
| `prometheus_multiproc_dir` | `prometheus_client`, имя помечено устаревшим | нет |

`prometheus_client` выбирает класс хранения (mmap или память процесса) в момент первого импорта. Каталог должен быть выставлен до этого импорта. `Prometheus.after_process_boot` выставляет его перед своим импортом клиента. Процесс exposition и воркеры сходятся на одном пути, если переменная задана в окружении родителя до старта.

| Переменная | Назначение | По умолчанию |
| --- | --- | --- |
| `DRAMATIQ_PROM_HOST`, `dramatiq_prom_host` | Адрес exposition | `0.0.0.0` |
| `DRAMATIQ_PROM_PORT`, `dramatiq_prom_port` | Порт exposition | `9191` |
| `DRAMATIQ_PROM_CLEAN` | `0`, `false`, `no`, `off` — CLI не удаляет `*.db`. Любое другое значение, включая пустое отсутствие переменной, — удаляет | удалять |

Штатные `dramatiq_prom_host` и `dramatiq_prom_port` сохранены: их читает и оригинальный middleware.
