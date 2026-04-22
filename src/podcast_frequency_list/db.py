from __future__ import annotations

import sqlite3
from pathlib import Path

from podcast_frequency_list.config import load_settings

SCHEMA_VERSION = "9"


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
        migrate_legacy_schema(connection)
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


def migrate_legacy_schema(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(shows)")
    }
    if "podcast_index_id" not in columns:
        return

    connection.execute("PRAGMA foreign_keys = OFF;")
    connection.execute(
        """
        CREATE TABLE shows_migrated (
            show_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            feed_url TEXT NOT NULL UNIQUE,
            site_url TEXT,
            language TEXT,
            bucket TEXT,
            description TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        INSERT INTO shows_migrated (
            show_id,
            title,
            feed_url,
            site_url,
            language,
            bucket,
            description,
            created_at,
            updated_at
        )
        SELECT
            show_id,
            title,
            feed_url,
            site_url,
            language,
            bucket,
            description,
            created_at,
            updated_at
        FROM shows
        """
    )
    connection.execute("DROP TABLE shows")
    connection.execute("ALTER TABLE shows_migrated RENAME TO shows")
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.commit()


def upsert_show(
    connection: sqlite3.Connection,
    *,
    title: str,
    feed_url: str,
    site_url: str | None = None,
    language: str | None = None,
    bucket: str | None = None,
    description: str | None = None,
) -> int:
    row = connection.execute(
        "SELECT show_id FROM shows WHERE feed_url = ?",
        (feed_url,),
    ).fetchone()
    if row is not None:
        show_id = row["show_id"]
        connection.execute(
            """
            UPDATE shows
            SET title = ?,
                feed_url = ?,
                site_url = ?,
                language = ?,
                bucket = ?,
                description = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE show_id = ?
            """,
            (
                title,
                feed_url,
                site_url,
                language,
                bucket,
                description,
                show_id,
            ),
        )
        return show_id

    cursor = connection.execute(
        """
        INSERT INTO shows (
            title,
            feed_url,
            site_url,
            language,
            bucket,
            description
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            feed_url,
            site_url,
            language,
            bucket,
            description,
        ),
    )
    return int(cursor.lastrowid)


def get_show_by_id(connection: sqlite3.Connection, show_id: int) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            show_id,
            title,
            feed_url,
            site_url,
            language,
            bucket,
            description
        FROM shows
        WHERE show_id = ?
        """,
        (show_id,),
    ).fetchone()


def update_show(
    connection: sqlite3.Connection,
    *,
    show_id: int,
    title: str,
    feed_url: str,
    site_url: str | None = None,
    language: str | None = None,
    bucket: str | None = None,
    description: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE shows
        SET title = ?,
            feed_url = ?,
            site_url = ?,
            language = ?,
            bucket = ?,
            description = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE show_id = ?
        """,
        (
            title,
            feed_url,
            site_url,
            language,
            bucket,
            description,
            show_id,
        ),
    )


def upsert_episode(
    connection: sqlite3.Connection,
    *,
    show_id: int,
    guid: str,
    title: str,
    published_at: str | None = None,
    audio_url: str | None = None,
    episode_url: str | None = None,
    duration_seconds: int | None = None,
    summary: str | None = None,
    has_transcript_tag: bool = False,
    transcript_url: str | None = None,
) -> bool:
    row = connection.execute(
        """
        SELECT episode_id
        FROM episodes
        WHERE show_id = ? AND guid = ?
        """,
        (show_id, guid),
    ).fetchone()

    if row is not None:
        connection.execute(
            """
            UPDATE episodes
            SET title = ?,
                published_at = ?,
                audio_url = ?,
                episode_url = ?,
                duration_seconds = ?,
                summary = ?,
                has_transcript_tag = ?,
                transcript_url = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE episode_id = ?
            """,
            (
                title,
                published_at,
                audio_url,
                episode_url,
                duration_seconds,
                summary,
                int(has_transcript_tag),
                transcript_url,
                row["episode_id"],
            ),
        )
        return False

    connection.execute(
        """
        INSERT INTO episodes (
            show_id,
            guid,
            title,
            published_at,
            audio_url,
            episode_url,
            duration_seconds,
            summary,
            has_transcript_tag,
            transcript_url
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            show_id,
            guid,
            title,
            published_at,
            audio_url,
            episode_url,
            duration_seconds,
            summary,
            int(has_transcript_tag),
            transcript_url,
        ),
    )
    return True


def upsert_transcript_source(
    connection: sqlite3.Connection,
    *,
    episode_id: int,
    source_type: str,
    status: str,
    model: str,
    raw_path: str | None = None,
    estimated_cost_usd: float | None = None,
    preserve_ready: bool = False,
) -> int:
    row = connection.execute(
        """
        SELECT source_id, status
        FROM transcript_sources
        WHERE episode_id = ?
        AND source_type = ?
        AND model = ?
        """,
        (episode_id, source_type, model),
    ).fetchone()

    if row is None:
        cursor = connection.execute(
            """
            INSERT INTO transcript_sources (
                episode_id,
                source_type,
                status,
                model,
                raw_path,
                estimated_cost_usd
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (episode_id, source_type, status, model, raw_path, estimated_cost_usd),
        )
        return int(cursor.lastrowid)

    source_id = int(row["source_id"])
    current_status = str(row["status"])
    next_status = current_status if preserve_ready and current_status == "ready" else status

    connection.execute(
        """
        UPDATE transcript_sources
        SET status = ?,
            raw_path = COALESCE(?, raw_path),
            estimated_cost_usd = COALESCE(?, estimated_cost_usd),
            updated_at = CURRENT_TIMESTAMP
        WHERE source_id = ?
        """,
        (next_status, raw_path, estimated_cost_usd, source_id),
    )
    return source_id


__all__ = [
    "SCHEMA_VERSION",
    "bootstrap_database",
    "connect",
    "get_schema_path",
    "get_show_by_id",
    "load_schema",
    "migrate_legacy_schema",
    "update_show",
    "upsert_episode",
    "upsert_show",
    "upsert_transcript_source",
]
