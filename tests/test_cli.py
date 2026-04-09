import sqlite3

from typer.testing import CliRunner

from podcast_frequency_list.cli import app
from podcast_frequency_list.config import load_settings

runner = CliRunner()


def test_cli_help() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "CLI for building the podcast-based French frequency deck." in result.stdout


def test_init_db_creates_database(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 0
    db_path = tmp_path / "test.db"
    assert db_path.exists()

    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'app_meta'"
        ).fetchall()

    assert rows == [("app_meta",)]

    load_settings.cache_clear()
