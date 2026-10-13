# dramatiq-prometheus-fix

Документация собрана по шаблону [Diátaxis](https://diataxis.fr/map/).
Каркас каталога — [`sphinx-quickstart`](https://www.sphinx-doc.org/en/master/tutorial/getting-started.html):
`conf.py`, `index`, `_static`, `_templates`, сборка Sphinx + MyST.

| Раздел | Вопрос | Зачем открывать |
| --- | --- | --- |
| Tutorial | как это сделать в первый раз, по шагам | один отложенный вызов и нулевой gauge |
| How-to | как решить конкретную задачу | каталог, мёртвые процессы, retry без `delay=` |
| Reference | **что** именно устроено и что наблюдалось | факты, числа, API, переменные |
| Explanation | **почему** gauge врёт | путь сообщения через воркер |

Проверено на Dramatiq 2.2.1 и Python 3.12. Issues
[#627](https://github.com/Bogdanp/dramatiq/issues/627) и
[#672](https://github.com/Bogdanp/dramatiq/issues/672) на момент проверки открыты.

```{toctree}
:maxdepth: 1
:caption: Tutorial

tutorials/first-delayed-gauge
```

```{toctree}
:maxdepth: 1
:caption: How-to guides

how-to/clear-multiprocess-directory
how-to/reap-dead-workers
how-to/retries-without-delay
```

```{toctree}
:maxdepth: 1
:caption: Reference — что

reference/what
reference/metrics
reference/api
reference/environment
```

```{toctree}
:maxdepth: 1
:caption: Explanation — почему

explanation/why
explanation/out-of-scope
```
