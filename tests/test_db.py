import sqlite3

import pytest

from podcast_frequency_list.db import SCHEMA_VERSION, bootstrap_database, connect


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
            INSERT INTO shows (podcast_index_id, title, feed_url)
            VALUES (?, ?, ?)
            """,
            (1, "Test Show", "https://example.com/feed.xml"),
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
