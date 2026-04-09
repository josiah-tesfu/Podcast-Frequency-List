from __future__ import annotations

import sqlite3
from pathlib import Path

from podcast_frequency_list.config import load_settings


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
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()

    return target_path
