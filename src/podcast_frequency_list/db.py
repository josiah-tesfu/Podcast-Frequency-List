from __future__ import annotations

import sqlite3
from pathlib import Path

from podcast_frequency_list.config import load_settings

SCHEMA_VERSION = "4"


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


def upsert_show(
    connection: sqlite3.Connection,
    *,
    podcast_index_id: int | None,
    title: str,
    feed_url: str,
    site_url: str | None = None,
    language: str | None = None,
    bucket: str | None = None,
    description: str | None = None,
) -> int:
    matching_show_ids: set[int] = set()

    if podcast_index_id is not None:
        row = connection.execute(
            "SELECT show_id FROM shows WHERE podcast_index_id = ?",
            (podcast_index_id,),
        ).fetchone()
        if row is not None:
            matching_show_ids.add(row["show_id"])

    row = connection.execute(
        "SELECT show_id FROM shows WHERE feed_url = ?",
        (feed_url,),
    ).fetchone()
    if row is not None:
        matching_show_ids.add(row["show_id"])

    if len(matching_show_ids) > 1:
        raise ValueError("conflicting existing shows found for podcast_index_id/feed_url")

    if matching_show_ids:
        show_id = matching_show_ids.pop()
        connection.execute(
            """
            UPDATE shows
            SET podcast_index_id = ?,
                title = ?,
                feed_url = ?,
                site_url = ?,
                language = ?,
                bucket = ?,
                description = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE show_id = ?
            """,
            (
                podcast_index_id,
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
            podcast_index_id,
            title,
            feed_url,
            site_url,
            language,
            bucket,
            description
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            podcast_index_id,
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
            podcast_index_id,
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
    podcast_index_id: int | None,
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
        SET podcast_index_id = ?,
            title = ?,
            feed_url = ?,
            site_url = ?,
            language = ?,
            bucket = ?,
            description = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE show_id = ?
        """,
        (
            podcast_index_id,
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
