import sqlite3

import pytest

from podcast_frequency_list.db import (
    SCHEMA_VERSION,
    bootstrap_database,
    connect,
    upsert_episode,
    upsert_show,
)


def test_bootstrap_sets_schema_version(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]

    assert schema_version == SCHEMA_VERSION


def test_bootstrap_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    bootstrap_database(db_path)
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        counts = connection.execute(
            "SELECT COUNT(*) FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]

    assert counts == 1


def test_episode_unique_constraint_per_show(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO shows (title, feed_url)
            VALUES (?, ?)
            """,
            ("Test Show", "https://example.com/feed.xml"),
        )
        show_id = connection.execute("SELECT show_id FROM shows").fetchone()[0]
        connection.execute(
            """
            INSERT INTO episodes (show_id, guid, title)
            VALUES (?, ?, ?)
            """,
            (show_id, "episode-guid", "Episode 1"),
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO episodes (show_id, guid, title)
                VALUES (?, ?, ?)
                """,
                (show_id, "episode-guid", "Episode 1 Duplicate"),
            )


def test_episode_foreign_key_requires_existing_show(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO episodes (show_id, guid, title)
            VALUES (?, ?, ?)
            """,
            (999, "episode-guid", "Episode 1"),
        )


def test_upsert_show_updates_existing_row(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Original Title",
            feed_url="https://example.com/feed.xml",
            language="en",
        )
        updated_show_id = upsert_show(
            connection,
            title="Updated Title",
            feed_url="https://example.com/feed.xml",
            language="fr",
        )
        connection.commit()
        row = connection.execute(
            "SELECT show_id, title, feed_url, language FROM shows WHERE show_id = ?",
            (show_id,),
        ).fetchone()

    assert updated_show_id == show_id
    assert row["show_id"] == show_id
    assert row["title"] == "Updated Title"
    assert row["feed_url"] == "https://example.com/feed.xml"
    assert row["language"] == "fr"


def test_upsert_episode_updates_existing_row(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Show",
            feed_url="https://example.com/feed.xml",
        )
        inserted = upsert_episode(
            connection,
            show_id=show_id,
            guid="guid-1",
            title="Episode 1",
            audio_url="https://cdn.example.com/ep1.mp3",
            duration_seconds=61,
        )
        updated = upsert_episode(
            connection,
            show_id=show_id,
            guid="guid-1",
            title="Episode 1 Updated",
            audio_url="https://cdn.example.com/ep1.mp3",
            duration_seconds=62,
        )
        row = connection.execute(
            """
            SELECT title, duration_seconds
            FROM episodes
            WHERE show_id = ? AND guid = ?
            """,
            (show_id, "guid-1"),
        ).fetchone()

    assert inserted is True
    assert updated is False
    assert row["title"] == "Episode 1 Updated"
    assert row["duration_seconds"] == 62


def test_bootstrap_migrates_legacy_shows_table_without_podcast_index_id(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE shows (
                show_id INTEGER PRIMARY KEY,
                podcast_index_id INTEGER UNIQUE,
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
            INSERT INTO shows (
                show_id,
                podcast_index_id,
                title,
                feed_url,
                language
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (1, 123, "Legacy Show", "https://example.com/feed.xml", "fr"),
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(shows)").fetchall()
        }
        row = connection.execute(
            "SELECT show_id, title, feed_url, language FROM shows WHERE show_id = 1"
        ).fetchone()

    assert "podcast_index_id" not in columns
    assert row["title"] == "Legacy Show"
    assert row["feed_url"] == "https://example.com/feed.xml"
    assert row["language"] == "fr"
