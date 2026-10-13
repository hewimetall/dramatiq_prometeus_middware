from __future__ import annotations

import os

from dramatiq_prometheus_fix.cli import main
from dramatiq_prometheus_fix.multiproc import prepare_multiproc_dir, reap_dead_processes


def test_prepare_removes_db_files_and_publishes_the_directory(
    tmp_path, monkeypatch
) -> None:
    for key in (
        "PROMETHEUS_MULTIPROC_DIR",
        "prometheus_multiproc_dir",
        "DRAMATIQ_PROM_DB",
        "dramatiq_prom_db",
    ):
        monkeypatch.delenv(key, raising=False)
    (tmp_path / "gauge_livesum_9.db").write_bytes(b"stale")
    (tmp_path / "notes.txt").write_text("keep", encoding="utf-8")

    resolved = prepare_multiproc_dir(tmp_path, clean=True)

    assert resolved == os.fspath(tmp_path)
    assert not (tmp_path / "gauge_livesum_9.db").exists()
    assert (tmp_path / "notes.txt").read_text(encoding="utf-8") == "keep"
    assert os.environ["PROMETHEUS_MULTIPROC_DIR"] == resolved
    assert os.environ["DRAMATIQ_PROM_DB"] == resolved


def test_reap_skips_the_current_process(tmp_path) -> None:
    live = tmp_path / f"gauge_livesum_{os.getpid()}.db"
    dead = tmp_path / "gauge_livesum_2100000003.db"
    live.write_bytes(b"")
    dead.write_bytes(b"")

    reaped = reap_dead_processes(tmp_path)

    assert live.exists()
    assert not dead.exists()
    assert reaped == [2_100_000_003]


def test_cli_clears_the_directory_then_execs_dramatiq(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DRAMATIQ_PROM_DB", os.fspath(tmp_path))
    (tmp_path / "gauge_all_4.db").write_bytes(b"previous-run")
    captured: dict[str, object] = {}

    def fake_exec(command: str, argv: list[str]) -> None:
        captured["command"] = command
        captured["argv"] = argv
        captured["directory"] = os.environ["PROMETHEUS_MULTIPROC_DIR"]

    main(["tasks:broker", "-p", "2", "-t", "4"], execvp=fake_exec)

    assert captured["command"] == "dramatiq"
    assert captured["argv"] == ["dramatiq", "tasks:broker", "-p", "2", "-t", "4"]
    assert captured["directory"] == os.fspath(tmp_path)
    assert not (tmp_path / "gauge_all_4.db").exists()


def test_cli_help_and_disabled_clean_keep_existing_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DRAMATIQ_PROM_DB", os.fspath(tmp_path))
    help_file = tmp_path / "counter_1.db"
    help_file.write_bytes(b"x")
    main(["--help"], execvp=lambda command, argv: None)
    assert help_file.exists()

    monkeypatch.setenv("DRAMATIQ_PROM_CLEAN", "0")
    kept = tmp_path / "gauge_livesum_8.db"
    kept.write_bytes(b"x")
    main(["tasks:broker"], execvp=lambda command, argv: None)
    assert kept.exists()
