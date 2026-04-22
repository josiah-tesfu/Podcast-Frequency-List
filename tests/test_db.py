import sqlite3

import pytest

from podcast_frequency_list.db import (
    SCHEMA_VERSION,
    bootstrap_database,
    connect,
    upsert_episode,
    upsert_show,
)


def _seed_occurrence_context(connection):
    show_id = upsert_show(
        connection,
        title="Candidate Show",
        feed_url="https://example.com/candidate-feed.xml",
    )
    upsert_episode(
        connection,
        show_id=show_id,
        guid="candidate-episode",
        title="Candidate Episode",
        audio_url="https://cdn.example.com/candidate.mp3",
    )
    episode_id = connection.execute(
        """
        SELECT episode_id
        FROM episodes
        WHERE show_id = ? AND guid = ?
        """,
        (show_id, "candidate-episode"),
    ).fetchone()["episode_id"]
    source_cursor = connection.execute(
        """
        INSERT INTO transcript_sources (
            episode_id,
            source_type,
            status,
            model,
            raw_path
        )
        VALUES (?, 'asr', 'ready', 'test-model', ?)
        """,
        (episode_id, "/tmp/candidate-episode.txt"),
    )
    source_id = int(source_cursor.lastrowid)
    segment_cursor = connection.execute(
        """
        INSERT INTO transcript_segments (
            source_id,
            episode_id,
            chunk_index,
            start_ms,
            end_ms,
            raw_text
        )
        VALUES (?, ?, 0, 0, 1000, ?)
        """,
        (source_id, episode_id, "J'ai envie."),
    )
    segment_id = int(segment_cursor.lastrowid)
    sentence_cursor = connection.execute(
        """
        INSERT INTO segment_sentences (
            segment_id,
            episode_id,
            split_version,
            sentence_index,
            char_start,
            char_end,
            sentence_text
        )
        VALUES (?, ?, '1', 0, 0, 11, ?)
        """,
        (segment_id, episode_id, "J'ai envie."),
    )
    sentence_id = int(sentence_cursor.lastrowid)

    return episode_id, segment_id, sentence_id


def _insert_candidate(
    connection,
    *,
    inventory_version: str = "1",
    candidate_key: str = "j ai",
    display_text: str = "J'ai",
    ngram_size: int = 2,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO token_candidates (
            inventory_version,
            candidate_key,
            display_text,
            ngram_size
        )
        VALUES (?, ?, ?, ?)
        """,
        (inventory_version, candidate_key, display_text, ngram_size),
    )
    return int(cursor.lastrowid)


def _insert_occurrence(
    connection,
    *,
    candidate_id: int,
    sentence_id: int,
    episode_id: int,
    segment_id: int,
    inventory_version: str = "1",
    token_start_index: int = 0,
    token_end_index: int = 2,
    char_start: int = 0,
    char_end: int = 4,
    surface_text: str = "J'ai",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO token_occurrences (
            candidate_id,
            sentence_id,
            episode_id,
            segment_id,
            inventory_version,
            token_start_index,
            token_end_index,
            char_start,
            char_end,
            surface_text
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            sentence_id,
            episode_id,
            segment_id,
            inventory_version,
            token_start_index,
            token_end_index,
            char_start,
            char_end,
            surface_text,
        ),
    )
    return int(cursor.lastrowid)


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


def test_bootstrap_creates_candidate_inventory_tables_and_indexes(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                """
            )
        }
        indexes = {
            row["name"]
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'index'
                """
            )
        }
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {"token_candidates", "token_occurrences"} <= tables
    assert {
        "idx_token_candidates_inventory_version",
        "idx_token_candidates_ngram_size",
        "idx_token_candidates_frequency",
        "idx_token_occurrences_candidate",
        "idx_token_occurrences_sentence",
        "idx_token_occurrences_episode",
        "idx_token_occurrences_scope",
    } <= indexes
    assert foreign_key_issues == []


def test_token_candidates_are_unique_per_inventory_version(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(connection)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_candidate(connection, display_text="j'ai")

        _insert_candidate(connection, inventory_version="2")
        row_count = connection.execute(
            "SELECT COUNT(*) FROM token_candidates WHERE candidate_key = 'j ai'"
        ).fetchone()[0]

    assert row_count == 2


def test_token_occurrences_enforce_foreign_keys_and_unique_spans(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        episode_id, segment_id, sentence_id = _seed_occurrence_context(connection)
        candidate_id = _insert_candidate(connection)
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
        )

        with pytest.raises(sqlite3.IntegrityError):
            _insert_occurrence(
                connection,
                candidate_id=candidate_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
            )

        with pytest.raises(sqlite3.IntegrityError):
            _insert_occurrence(
                connection,
                candidate_id=999,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                token_start_index=2,
                token_end_index=3,
                char_start=5,
                char_end=10,
                surface_text="envie",
            )

        with pytest.raises(sqlite3.IntegrityError):
            _insert_occurrence(
                connection,
                candidate_id=candidate_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                inventory_version="2",
                token_start_index=2,
                token_end_index=3,
                char_start=5,
                char_end=10,
                surface_text="envie",
            )


def test_token_occurrences_cascade_when_candidate_is_deleted(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        episode_id, segment_id, sentence_id = _seed_occurrence_context(connection)
        candidate_id = _insert_candidate(connection)
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
        )
        connection.execute(
            "DELETE FROM token_candidates WHERE candidate_id = ?",
            (candidate_id,),
        )
        occurrence_count = connection.execute(
            "SELECT COUNT(*) FROM token_occurrences"
        ).fetchone()[0]

    assert occurrence_count == 0
