from __future__ import annotations

import sqlite3
from pathlib import Path

from podcast_frequency_list.config import load_settings

SCHEMA_VERSION = "1"


def get_schema_path() -> Path:
    return Path(__file__).with_name("schema.sql")


def load_schema() -> str:
    return get_schema_path().read_text(encoding="utf-8")


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    settings = load_settings()
    target_path = db_path or settings.db_path
    target_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(target_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def bootstrap_database(db_path: Path | None = None) -> Path:
    settings = load_settings()
    settings.ensure_directories()
    target_path = db_path or settings.db_path

    connection = connect(target_path)
    try:
        connection.executescript(load_schema())
        connection.execute(
            """
            INSERT INTO app_meta (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            ("schema_version", SCHEMA_VERSION),
        )
        connection.commit()
    finally:
        connection.close()

    return target_path
