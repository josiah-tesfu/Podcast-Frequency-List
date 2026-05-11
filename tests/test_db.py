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
        candidate_columns = {
            row["name"]: row
            for row in connection.execute("PRAGMA table_info(token_candidates)").fetchall()
        }
        containment_columns = {
            row["name"]: row
            for row in connection.execute(
                "PRAGMA table_info(candidate_containment)"
            ).fetchall()
        }
        score_columns = {
            row["name"]: row
            for row in connection.execute(
                "PRAGMA table_info(candidate_scores)"
            ).fetchall()
        }
        containment_table_sql = connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table'
            AND name = 'candidate_containment'
            """
        ).fetchone()["sql"]
        score_table_sql = connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table'
            AND name = 'candidate_scores'
            """
        ).fetchone()["sql"]
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

    assert {
        "token_candidates",
        "token_occurrences",
        "candidate_containment",
        "candidate_scores",
    } <= tables
    assert {
        "episode_dispersion",
        "show_dispersion",
        "t_score",
        "npmi",
        "left_context_type_count",
        "right_context_type_count",
        "left_entropy",
        "right_entropy",
        "punctuation_gap_occurrence_count",
        "punctuation_gap_occurrence_ratio",
        "punctuation_gap_edge_clitic_count",
        "punctuation_gap_edge_clitic_ratio",
        "starts_with_standalone_clitic",
        "ends_with_standalone_clitic",
        "max_component_information",
        "min_component_information",
        "high_information_token_count",
        "max_show_share",
        "top2_show_share",
        "show_entropy",
    } <= candidate_columns.keys()
    assert {
        "inventory_version",
        "smaller_candidate_id",
        "larger_candidate_id",
        "extension_side",
        "shared_occurrence_count",
        "shared_episode_count",
    } <= containment_columns.keys()
    assert {
        "inventory_version",
        "score_version",
        "candidate_id",
        "ranking_lane",
        "passes_support_gate",
        "passes_quality_gate",
        "discard_family",
        "is_eligible",
        "frequency_score",
        "dispersion_score",
        "association_score",
        "boundary_score",
        "redundancy_penalty",
        "final_score",
        "lane_rank",
    } <= score_columns.keys()
    assert candidate_columns["episode_dispersion"]["notnull"] == 1
    assert candidate_columns["episode_dispersion"]["dflt_value"] == "0"
    assert candidate_columns["show_dispersion"]["notnull"] == 1
    assert candidate_columns["show_dispersion"]["dflt_value"] == "0"
    assert candidate_columns["t_score"]["notnull"] == 0
    assert candidate_columns["t_score"]["dflt_value"] is None
    assert candidate_columns["npmi"]["notnull"] == 0
    assert candidate_columns["npmi"]["dflt_value"] is None
    assert candidate_columns["left_context_type_count"]["notnull"] == 0
    assert candidate_columns["left_context_type_count"]["dflt_value"] is None
    assert candidate_columns["right_context_type_count"]["notnull"] == 0
    assert candidate_columns["right_context_type_count"]["dflt_value"] is None
    assert candidate_columns["left_entropy"]["notnull"] == 0
    assert candidate_columns["left_entropy"]["dflt_value"] is None
    assert candidate_columns["right_entropy"]["notnull"] == 0
    assert candidate_columns["right_entropy"]["dflt_value"] is None
    assert candidate_columns["punctuation_gap_occurrence_count"]["notnull"] == 0
    assert candidate_columns["punctuation_gap_occurrence_count"]["dflt_value"] is None
    assert candidate_columns["punctuation_gap_occurrence_ratio"]["notnull"] == 0
    assert candidate_columns["punctuation_gap_occurrence_ratio"]["dflt_value"] is None
    assert candidate_columns["punctuation_gap_edge_clitic_count"]["notnull"] == 0
    assert candidate_columns["punctuation_gap_edge_clitic_count"]["dflt_value"] is None
    assert candidate_columns["punctuation_gap_edge_clitic_ratio"]["notnull"] == 0
    assert candidate_columns["punctuation_gap_edge_clitic_ratio"]["dflt_value"] is None
    assert candidate_columns["starts_with_standalone_clitic"]["notnull"] == 0
    assert candidate_columns["starts_with_standalone_clitic"]["dflt_value"] is None
    assert candidate_columns["ends_with_standalone_clitic"]["notnull"] == 0
    assert candidate_columns["ends_with_standalone_clitic"]["dflt_value"] is None
    assert candidate_columns["max_component_information"]["notnull"] == 0
    assert candidate_columns["max_component_information"]["dflt_value"] is None
    assert candidate_columns["min_component_information"]["notnull"] == 0
    assert candidate_columns["min_component_information"]["dflt_value"] is None
    assert candidate_columns["high_information_token_count"]["notnull"] == 0
    assert candidate_columns["high_information_token_count"]["dflt_value"] is None
    assert candidate_columns["max_show_share"]["notnull"] == 0
    assert candidate_columns["max_show_share"]["dflt_value"] is None
    assert candidate_columns["top2_show_share"]["notnull"] == 0
    assert candidate_columns["top2_show_share"]["dflt_value"] is None
    assert candidate_columns["show_entropy"]["notnull"] == 0
    assert candidate_columns["show_entropy"]["dflt_value"] is None
    assert containment_columns["inventory_version"]["notnull"] == 1
    assert containment_columns["smaller_candidate_id"]["notnull"] == 1
    assert containment_columns["larger_candidate_id"]["notnull"] == 1
    assert containment_columns["extension_side"]["notnull"] == 1
    assert containment_columns["shared_occurrence_count"]["notnull"] == 1
    assert containment_columns["shared_episode_count"]["notnull"] == 1
    assert score_columns["inventory_version"]["notnull"] == 1
    assert score_columns["score_version"]["notnull"] == 1
    assert score_columns["candidate_id"]["notnull"] == 1
    assert score_columns["ranking_lane"]["notnull"] == 1
    assert score_columns["passes_support_gate"]["notnull"] == 1
    assert score_columns["passes_quality_gate"]["notnull"] == 1
    assert score_columns["discard_family"]["notnull"] == 0
    assert score_columns["is_eligible"]["notnull"] == 1
    assert score_columns["created_at"]["notnull"] == 1
    assert score_columns["created_at"]["dflt_value"] == "CURRENT_TIMESTAMP"
    assert score_columns["frequency_score"]["notnull"] == 0
    assert score_columns["dispersion_score"]["notnull"] == 0
    assert score_columns["association_score"]["notnull"] == 0
    assert score_columns["boundary_score"]["notnull"] == 0
    assert score_columns["redundancy_penalty"]["notnull"] == 0
    assert score_columns["final_score"]["notnull"] == 0
    assert score_columns["lane_rank"]["notnull"] == 0
    assert "'both'" in containment_table_sql
    assert "'1gram'" in score_table_sql
    assert "'2gram'" in score_table_sql
    assert "'3gram'" in score_table_sql
    assert "'support_floor'" in score_table_sql
    assert "'edge_clitic_gap'" in score_table_sql
    assert "'weak_multiword'" in score_table_sql
    assert "'show_specificity'" in score_table_sql
    assert "is_eligible = 1 OR lane_rank IS NULL" in score_table_sql
    assert "is_eligible = 1 OR final_score IS NULL" in score_table_sql
    assert "is_eligible = 0 OR discard_family IS NULL" in score_table_sql
    assert "is_eligible = 1 OR discard_family IS NOT NULL" in score_table_sql
    assert {
        "idx_token_candidates_inventory_version",
        "idx_token_candidates_ngram_size",
        "idx_token_candidates_frequency",
        "idx_token_occurrences_candidate",
        "idx_token_occurrences_sentence",
        "idx_token_occurrences_episode",
        "idx_token_occurrences_scope",
        "idx_candidate_containment_larger",
        "idx_candidate_scores_lane_rank",
    } <= indexes
    assert foreign_key_issues == []


def test_bootstrap_migrates_candidate_metric_columns_from_v9_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency
            )
            VALUES ('1', 'en fait', 'en fait', 2, 7)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        candidate_columns = {
            row["name"]: row
            for row in connection.execute("PRAGMA table_info(token_candidates)").fetchall()
        }
        row = connection.execute(
            """
            SELECT
                candidate_key,
                raw_frequency,
                episode_dispersion,
                show_dispersion
            FROM token_candidates
            WHERE candidate_key = 'en fait'
            """
        ).fetchone()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {
        "episode_dispersion",
        "show_dispersion",
    } <= candidate_columns.keys()
    assert row["raw_frequency"] == 7
    assert row["episode_dispersion"] == 0
    assert row["show_dispersion"] == 0
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_migrates_step4_columns_from_v10_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion
            )
            VALUES ('1', 'en fait', 'en fait', 2, 7, 3, 2)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        candidate_columns = {
            row["name"]: row
            for row in connection.execute("PRAGMA table_info(token_candidates)").fetchall()
        }
        row = connection.execute(
            """
            SELECT
                candidate_key,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi,
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy
            FROM token_candidates
            WHERE candidate_key = 'en fait'
            """
        ).fetchone()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {
        "t_score",
        "npmi",
        "left_context_type_count",
        "right_context_type_count",
        "left_entropy",
        "right_entropy",
    } <= candidate_columns.keys()
    assert row["raw_frequency"] == 7
    assert row["episode_dispersion"] == 3
    assert row["show_dispersion"] == 2
    assert row["t_score"] is None
    assert row["npmi"] is None
    assert row["left_context_type_count"] is None
    assert row["right_context_type_count"] is None
    assert row["left_entropy"] is None
    assert row["right_entropy"] is None
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_creates_candidate_containment_table_from_v11_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                t_score REAL,
                npmi REAL,
                left_context_type_count INTEGER
                    CHECK (left_context_type_count IS NULL OR left_context_type_count >= 0),
                right_context_type_count INTEGER
                    CHECK (right_context_type_count IS NULL OR right_context_type_count >= 0),
                left_entropy REAL
                    CHECK (left_entropy IS NULL OR left_entropy >= 0),
                right_entropy REAL
                    CHECK (right_entropy IS NULL OR right_entropy >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi
            )
            VALUES ('1', 'en fait', 'en fait', 2, 7, 3, 2, 1.5, 0.4)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        containment_columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(candidate_containment)"
            ).fetchall()
        }
        containment_table_sql = connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table'
            AND name = 'candidate_containment'
            """
        ).fetchone()["sql"]
        row = connection.execute(
            """
            SELECT
                candidate_key,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi
            FROM token_candidates
            WHERE candidate_key = 'en fait'
            """
        ).fetchone()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {
        "inventory_version",
        "smaller_candidate_id",
        "larger_candidate_id",
        "extension_side",
        "shared_occurrence_count",
        "shared_episode_count",
    } <= containment_columns
    assert "'both'" in containment_table_sql
    assert row["raw_frequency"] == 7
    assert row["episode_dispersion"] == 3
    assert row["show_dispersion"] == 2
    assert row["t_score"] == 1.5
    assert row["npmi"] == 0.4
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_migrates_followup_b_columns_from_v14_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                t_score REAL,
                npmi REAL,
                left_context_type_count INTEGER
                    CHECK (left_context_type_count IS NULL OR left_context_type_count >= 0),
                right_context_type_count INTEGER
                    CHECK (right_context_type_count IS NULL OR right_context_type_count >= 0),
                left_entropy REAL
                    CHECK (left_entropy IS NULL OR left_entropy >= 0),
                right_entropy REAL
                    CHECK (right_entropy IS NULL OR right_entropy >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi,
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy
            )
            VALUES ('1', 'en fait', 'en fait', 2, 7, 3, 2, 1.5, 0.4, 2, 2, 0.2, 0.3)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        candidate_columns = {
            row["name"]: row
            for row in connection.execute("PRAGMA table_info(token_candidates)").fetchall()
        }
        row = connection.execute(
            """
            SELECT
                candidate_key,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi,
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy,
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                starts_with_standalone_clitic,
                ends_with_standalone_clitic,
                max_component_information,
                min_component_information,
                high_information_token_count
            FROM token_candidates
            WHERE candidate_key = 'en fait'
            """
        ).fetchone()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {
        "punctuation_gap_occurrence_count",
        "punctuation_gap_occurrence_ratio",
        "punctuation_gap_edge_clitic_count",
        "punctuation_gap_edge_clitic_ratio",
        "starts_with_standalone_clitic",
        "ends_with_standalone_clitic",
        "max_component_information",
        "min_component_information",
        "high_information_token_count",
    } <= candidate_columns.keys()
    assert row["raw_frequency"] == 7
    assert row["episode_dispersion"] == 3
    assert row["show_dispersion"] == 2
    assert row["t_score"] == 1.5
    assert row["npmi"] == 0.4
    assert row["left_context_type_count"] == 2
    assert row["right_context_type_count"] == 2
    assert row["left_entropy"] == 0.2
    assert row["right_entropy"] == 0.3
    assert row["punctuation_gap_occurrence_count"] is None
    assert row["punctuation_gap_occurrence_ratio"] is None
    assert row["punctuation_gap_edge_clitic_count"] is None
    assert row["punctuation_gap_edge_clitic_ratio"] is None
    assert row["starts_with_standalone_clitic"] is None
    assert row["ends_with_standalone_clitic"] is None
    assert row["max_component_information"] is None
    assert row["min_component_information"] is None
    assert row["high_information_token_count"] is None
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_migrates_specificity_columns_from_v16_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                t_score REAL,
                npmi REAL,
                left_context_type_count INTEGER
                    CHECK (left_context_type_count IS NULL OR left_context_type_count >= 0),
                right_context_type_count INTEGER
                    CHECK (right_context_type_count IS NULL OR right_context_type_count >= 0),
                left_entropy REAL
                    CHECK (left_entropy IS NULL OR left_entropy >= 0),
                right_entropy REAL
                    CHECK (right_entropy IS NULL OR right_entropy >= 0),
                punctuation_gap_occurrence_count INTEGER
                    CHECK (
                        punctuation_gap_occurrence_count IS NULL
                        OR punctuation_gap_occurrence_count >= 0
                    ),
                punctuation_gap_occurrence_ratio REAL
                    CHECK (
                        punctuation_gap_occurrence_ratio IS NULL
                        OR (
                            punctuation_gap_occurrence_ratio >= 0
                            AND punctuation_gap_occurrence_ratio <= 1
                        )
                    ),
                punctuation_gap_edge_clitic_count INTEGER
                    CHECK (
                        punctuation_gap_edge_clitic_count IS NULL
                        OR punctuation_gap_edge_clitic_count >= 0
                    ),
                punctuation_gap_edge_clitic_ratio REAL
                    CHECK (
                        punctuation_gap_edge_clitic_ratio IS NULL
                        OR (
                            punctuation_gap_edge_clitic_ratio >= 0
                            AND punctuation_gap_edge_clitic_ratio <= 1
                        )
                    ),
                max_component_information REAL
                    CHECK (
                        max_component_information IS NULL
                        OR max_component_information >= 0
                    ),
                min_component_information REAL
                    CHECK (
                        min_component_information IS NULL
                        OR min_component_information >= 0
                    ),
                high_information_token_count INTEGER
                    CHECK (
                        high_information_token_count IS NULL
                        OR high_information_token_count >= 0
                    ),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi,
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy,
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                max_component_information,
                min_component_information,
                high_information_token_count
            )
            VALUES (
                '1',
                'en fait',
                'en fait',
                2,
                7,
                3,
                2,
                1.5,
                0.4,
                2,
                2,
                0.2,
                0.3,
                1,
                0.2,
                0,
                0.0,
                1.7,
                1.3,
                1
            )
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        candidate_columns = {
            row["name"]: row
            for row in connection.execute("PRAGMA table_info(token_candidates)").fetchall()
        }
        row = connection.execute(
            """
            SELECT
                candidate_key,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                max_show_share,
                top2_show_share,
                show_entropy
            FROM token_candidates
            WHERE candidate_key = 'en fait'
            """
        ).fetchone()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {
        "max_show_share",
        "top2_show_share",
        "show_entropy",
    } <= candidate_columns.keys()
    assert row["raw_frequency"] == 7
    assert row["episode_dispersion"] == 3
    assert row["show_dispersion"] == 2
    assert row["max_show_share"] is None
    assert row["top2_show_share"] is None
    assert row["show_entropy"] is None
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_migrates_edge_clitic_marker_columns_from_v18_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                t_score REAL,
                npmi REAL,
                left_context_type_count INTEGER
                    CHECK (left_context_type_count IS NULL OR left_context_type_count >= 0),
                right_context_type_count INTEGER
                    CHECK (right_context_type_count IS NULL OR right_context_type_count >= 0),
                left_entropy REAL
                    CHECK (left_entropy IS NULL OR left_entropy >= 0),
                right_entropy REAL
                    CHECK (right_entropy IS NULL OR right_entropy >= 0),
                punctuation_gap_occurrence_count INTEGER
                    CHECK (
                        punctuation_gap_occurrence_count IS NULL
                        OR punctuation_gap_occurrence_count >= 0
                    ),
                punctuation_gap_occurrence_ratio REAL
                    CHECK (
                        punctuation_gap_occurrence_ratio IS NULL
                        OR (
                            punctuation_gap_occurrence_ratio >= 0
                            AND punctuation_gap_occurrence_ratio <= 1
                        )
                    ),
                punctuation_gap_edge_clitic_count INTEGER
                    CHECK (
                        punctuation_gap_edge_clitic_count IS NULL
                        OR punctuation_gap_edge_clitic_count >= 0
                    ),
                punctuation_gap_edge_clitic_ratio REAL
                    CHECK (
                        punctuation_gap_edge_clitic_ratio IS NULL
                        OR (
                            punctuation_gap_edge_clitic_ratio >= 0
                            AND punctuation_gap_edge_clitic_ratio <= 1
                        )
                    ),
                max_component_information REAL
                    CHECK (
                        max_component_information IS NULL
                        OR max_component_information >= 0
                    ),
                min_component_information REAL
                    CHECK (
                        min_component_information IS NULL
                        OR min_component_information >= 0
                    ),
                high_information_token_count INTEGER
                    CHECK (
                        high_information_token_count IS NULL
                        OR high_information_token_count >= 0
                    ),
                max_show_share REAL
                    CHECK (
                        max_show_share IS NULL
                        OR (max_show_share >= 0 AND max_show_share <= 1)
                    ),
                top2_show_share REAL
                    CHECK (
                        top2_show_share IS NULL
                        OR (top2_show_share >= 0 AND top2_show_share <= 1)
                    ),
                show_entropy REAL
                    CHECK (
                        show_entropy IS NULL
                        OR (show_entropy >= 0 AND show_entropy <= 1)
                    ),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                max_show_share,
                top2_show_share,
                show_entropy
            )
            VALUES ('1', 'n avez', 'n''avez', 2, 7, 3, 2, 0.5, 0.7, 0.6)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        candidate_columns = {
            row["name"]: row
            for row in connection.execute("PRAGMA table_info(token_candidates)").fetchall()
        }
        row = connection.execute(
            """
            SELECT
                candidate_key,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                max_show_share,
                top2_show_share,
                show_entropy,
                starts_with_standalone_clitic,
                ends_with_standalone_clitic
            FROM token_candidates
            WHERE candidate_key = 'n avez'
            """
        ).fetchone()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {
        "starts_with_standalone_clitic",
        "ends_with_standalone_clitic",
    } <= candidate_columns.keys()
    assert row["raw_frequency"] == 7
    assert row["episode_dispersion"] == 3
    assert row["show_dispersion"] == 2
    assert row["max_show_share"] == 0.5
    assert row["top2_show_share"] == 0.7
    assert row["show_entropy"] == 0.6
    assert row["starts_with_standalone_clitic"] is None
    assert row["ends_with_standalone_clitic"] is None
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_migrates_candidate_containment_side_from_v12_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                t_score REAL,
                npmi REAL,
                left_context_type_count INTEGER
                    CHECK (left_context_type_count IS NULL OR left_context_type_count >= 0),
                right_context_type_count INTEGER
                    CHECK (right_context_type_count IS NULL OR right_context_type_count >= 0),
                left_entropy REAL
                    CHECK (left_entropy IS NULL OR left_entropy >= 0),
                right_entropy REAL
                    CHECK (right_entropy IS NULL OR right_entropy >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE candidate_containment (
                inventory_version TEXT NOT NULL,
                smaller_candidate_id INTEGER NOT NULL,
                larger_candidate_id INTEGER NOT NULL,
                extension_side TEXT NOT NULL CHECK (extension_side IN ('left', 'right')),
                shared_occurrence_count INTEGER NOT NULL CHECK (shared_occurrence_count > 0),
                shared_episode_count INTEGER NOT NULL
                    CHECK (
                        shared_episode_count > 0
                        AND shared_episode_count <= shared_occurrence_count
                    ),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (inventory_version, smaller_candidate_id, larger_candidate_id),
                CHECK (smaller_candidate_id <> larger_candidate_id),
                FOREIGN KEY (smaller_candidate_id, inventory_version)
                    REFERENCES token_candidates(candidate_id, inventory_version)
                    ON DELETE CASCADE,
                FOREIGN KEY (larger_candidate_id, inventory_version)
                    REFERENCES token_candidates(candidate_id, inventory_version)
                    ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX idx_candidate_containment_larger
                ON candidate_containment (inventory_version, larger_candidate_id)
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                candidate_id,
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency
            )
            VALUES
                (1, '1', 'de', 'de', 1, 2),
                (2, '1', 'de de', 'de de', 2, 1)
            """
        )
        connection.execute(
            """
            INSERT INTO candidate_containment (
                inventory_version,
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            )
            VALUES ('1', 1, 2, 'left', 2, 1)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        containment_table_sql = connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table'
            AND name = 'candidate_containment'
            """
        ).fetchone()["sql"]
        row = connection.execute(
            """
            SELECT extension_side, shared_occurrence_count, shared_episode_count
            FROM candidate_containment
            WHERE inventory_version = '1'
            AND smaller_candidate_id = 1
            AND larger_candidate_id = 2
            """
        ).fetchone()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert "'both'" in containment_table_sql
    assert row["extension_side"] == "left"
    assert row["shared_occurrence_count"] == 2
    assert row["shared_episode_count"] == 1
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_creates_candidate_scores_table_from_v13_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                t_score REAL,
                npmi REAL,
                left_context_type_count INTEGER
                    CHECK (left_context_type_count IS NULL OR left_context_type_count >= 0),
                right_context_type_count INTEGER
                    CHECK (right_context_type_count IS NULL OR right_context_type_count >= 0),
                left_entropy REAL
                    CHECK (left_entropy IS NULL OR left_entropy >= 0),
                right_entropy REAL
                    CHECK (right_entropy IS NULL OR right_entropy >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE candidate_containment (
                inventory_version TEXT NOT NULL,
                smaller_candidate_id INTEGER NOT NULL,
                larger_candidate_id INTEGER NOT NULL,
                extension_side TEXT NOT NULL
                    CHECK (extension_side IN ('left', 'right', 'both')),
                shared_occurrence_count INTEGER NOT NULL
                    CHECK (shared_occurrence_count > 0),
                shared_episode_count INTEGER NOT NULL
                    CHECK (
                        shared_episode_count > 0
                        AND shared_episode_count <= shared_occurrence_count
                    ),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (inventory_version, smaller_candidate_id, larger_candidate_id),
                CHECK (smaller_candidate_id <> larger_candidate_id),
                FOREIGN KEY (smaller_candidate_id, inventory_version)
                    REFERENCES token_candidates(candidate_id, inventory_version)
                    ON DELETE CASCADE,
                FOREIGN KEY (larger_candidate_id, inventory_version)
                    REFERENCES token_candidates(candidate_id, inventory_version)
                    ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX idx_candidate_containment_larger
                ON candidate_containment (inventory_version, larger_candidate_id)
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                candidate_id,
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi
            )
            VALUES
                (1, '1', 'pense que', 'pense que', 2, 9, 4, 1, 3.5, 0.6),
                (2, '1', 'je pense que', 'je pense que', 3, 9, 4, 1, 4.2, 0.7)
            """
        )
        connection.execute(
            """
            INSERT INTO candidate_containment (
                inventory_version,
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            )
            VALUES ('1', 1, 2, 'left', 7, 4)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        score_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(candidate_scores)").fetchall()
        }
        candidate_row = connection.execute(
            """
            SELECT
                candidate_key,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi
            FROM token_candidates
            WHERE candidate_id = 1
            """
        ).fetchone()
        containment_row = connection.execute(
            """
            SELECT
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            FROM candidate_containment
            """
        ).fetchone()
        containment_count = connection.execute(
            "SELECT COUNT(*) FROM candidate_containment"
        ).fetchone()[0]
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {
        "inventory_version",
        "score_version",
        "candidate_id",
        "ranking_lane",
        "passes_support_gate",
        "passes_quality_gate",
        "discard_family",
        "is_eligible",
        "frequency_score",
        "dispersion_score",
        "association_score",
        "boundary_score",
        "redundancy_penalty",
        "final_score",
        "lane_rank",
    } <= score_columns
    assert candidate_row["candidate_key"] == "pense que"
    assert candidate_row["raw_frequency"] == 9
    assert candidate_row["episode_dispersion"] == 4
    assert candidate_row["show_dispersion"] == 1
    assert candidate_row["t_score"] == 3.5
    assert candidate_row["npmi"] == 0.6
    assert containment_row["smaller_candidate_id"] == 1
    assert containment_row["larger_candidate_id"] == 2
    assert containment_row["extension_side"] == "left"
    assert containment_row["shared_occurrence_count"] == 7
    assert containment_row["shared_episode_count"] == 4
    assert containment_count == 1
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_migrates_candidate_scores_table_from_v15_schema(tmp_path) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                candidate_id,
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion
            )
            VALUES
                (1, '1', 'de', 'de', 1, 25, 6, 1),
                (2, '1', 'est que', 'est que', 2, 12, 4, 1)
            """
        )
        connection.execute(
            """
            CREATE TABLE candidate_scores (
                inventory_version TEXT NOT NULL,
                score_version TEXT NOT NULL,
                candidate_id INTEGER NOT NULL,
                ranking_lane TEXT NOT NULL CHECK (ranking_lane IN ('1gram', '2gram', '3gram')),
                is_eligible INTEGER NOT NULL CHECK (is_eligible IN (0, 1)),
                frequency_score REAL,
                dispersion_score REAL,
                association_score REAL,
                boundary_score REAL,
                redundancy_penalty REAL,
                final_score REAL,
                lane_rank INTEGER CHECK (lane_rank IS NULL OR lane_rank > 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (inventory_version, score_version, candidate_id),
                CHECK (is_eligible = 1 OR lane_rank IS NULL),
                CHECK (is_eligible = 1 OR final_score IS NULL),
                FOREIGN KEY (candidate_id, inventory_version)
                    REFERENCES token_candidates(candidate_id, inventory_version)
                    ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX idx_candidate_scores_lane_rank
                ON candidate_scores (
                    inventory_version,
                    score_version,
                    ranking_lane,
                    lane_rank
                )
            """
        )
        connection.execute(
            """
            INSERT INTO candidate_scores (
                inventory_version,
                score_version,
                candidate_id,
                ranking_lane,
                is_eligible,
                frequency_score,
                dispersion_score,
                association_score,
                boundary_score,
                redundancy_penalty,
                final_score,
                lane_rank
            )
            VALUES
                ('1', 'pilot-v1', 1, '1gram', 1, 0.9, 1.0, NULL, NULL, 0.0, 0.95, 1),
                ('1', 'pilot-v1', 2, '2gram', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        score_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(candidate_scores)").fetchall()
        }
        rows = connection.execute(
            """
            SELECT
                candidate_id,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                final_score,
                lane_rank
            FROM candidate_scores
            WHERE inventory_version = '1'
            AND score_version = 'pilot-v1'
            ORDER BY candidate_id
            """
        ).fetchall()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert {"passes_support_gate", "passes_quality_gate", "discard_family"} <= score_columns
    assert [tuple(row) for row in rows] == [
        (1, 1, 1, None, 1, 0.95, 1),
        (2, 0, 0, "support_floor", 0, None, None),
    ]
    assert schema_version == SCHEMA_VERSION
    assert foreign_key_issues == []


def test_bootstrap_migrates_candidate_scores_discard_family_enum_from_v17_schema(
    tmp_path,
) -> None:
    db_path = tmp_path / "test.db"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE token_candidates (
                candidate_id INTEGER PRIMARY KEY,
                inventory_version TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                display_text TEXT NOT NULL,
                ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
                raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
                episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
                show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (inventory_version, candidate_key),
                UNIQUE (candidate_id, inventory_version)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO token_candidates (
                candidate_id,
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion
            )
            VALUES (1, '1', 'de', 'de', 1, 25, 6, 1)
            """
        )
        connection.execute(
            """
            CREATE TABLE candidate_scores (
                inventory_version TEXT NOT NULL,
                score_version TEXT NOT NULL,
                candidate_id INTEGER NOT NULL,
                ranking_lane TEXT NOT NULL CHECK (ranking_lane IN ('1gram', '2gram', '3gram')),
                passes_support_gate INTEGER NOT NULL CHECK (passes_support_gate IN (0, 1)),
                passes_quality_gate INTEGER NOT NULL CHECK (passes_quality_gate IN (0, 1)),
                discard_family TEXT
                    CHECK (
                        discard_family IS NULL
                        OR discard_family IN ('support_floor', 'edge_clitic_gap', 'weak_multiword')
                    ),
                is_eligible INTEGER NOT NULL CHECK (is_eligible IN (0, 1)),
                frequency_score REAL,
                dispersion_score REAL,
                association_score REAL,
                boundary_score REAL,
                redundancy_penalty REAL,
                final_score REAL,
                lane_rank INTEGER CHECK (lane_rank IS NULL OR lane_rank > 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (inventory_version, score_version, candidate_id),
                CHECK (is_eligible = 1 OR lane_rank IS NULL),
                CHECK (is_eligible = 1 OR final_score IS NULL),
                CHECK (is_eligible = 0 OR discard_family IS NULL),
                CHECK (is_eligible = 1 OR discard_family IS NOT NULL),
                FOREIGN KEY (candidate_id, inventory_version)
                    REFERENCES token_candidates(candidate_id, inventory_version)
                    ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            INSERT INTO candidate_scores (
                inventory_version,
                score_version,
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible
            )
            VALUES ('1', 'pilot-v2', 1, '1gram', 0, 0, 'support_floor', 0)
            """
        )
        connection.commit()

    bootstrap_database(db_path)

    with connect(db_path) as connection:
        score_table_sql = connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table'
            AND name = 'candidate_scores'
            """
        ).fetchone()["sql"]
        row = connection.execute(
            """
            SELECT discard_family
            FROM candidate_scores
            WHERE inventory_version = '1'
            AND score_version = 'pilot-v2'
            AND candidate_id = 1
            """
        ).fetchone()
        schema_version = connection.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()[0]

    assert "'show_specificity'" in score_table_sql
    assert row["discard_family"] == "support_floor"
    assert schema_version == SCHEMA_VERSION


def test_step4_columns_default_to_null_for_one_gram_candidates(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        candidate_id = _insert_candidate(
            connection,
            candidate_key="bonjour",
            display_text="bonjour",
            ngram_size=1,
        )
        row = connection.execute(
            """
            SELECT
                t_score,
                npmi,
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()

    assert row["t_score"] is None
    assert row["npmi"] is None
    assert row["left_context_type_count"] is None
    assert row["right_context_type_count"] is None
    assert row["left_entropy"] is None
    assert row["right_entropy"] is None


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


def test_candidate_containment_enforces_uniqueness_and_foreign_keys(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        smaller_candidate_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        larger_candidate_id = _insert_candidate(
            connection,
            candidate_key="ai envie",
            display_text="ai envie",
            ngram_size=2,
        )
        both_side_candidate_id = _insert_candidate(
            connection,
            candidate_key="envie envie",
            display_text="envie envie",
            ngram_size=2,
        )
        connection.execute(
            """
            INSERT INTO candidate_containment (
                inventory_version,
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("1", smaller_candidate_id, larger_candidate_id, "left", 3, 2),
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO candidate_containment (
                    inventory_version,
                    smaller_candidate_id,
                    larger_candidate_id,
                    extension_side,
                    shared_occurrence_count,
                    shared_episode_count
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("1", smaller_candidate_id, larger_candidate_id, "left", 3, 2),
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO candidate_containment (
                    inventory_version,
                    smaller_candidate_id,
                    larger_candidate_id,
                    extension_side,
                    shared_occurrence_count,
                    shared_episode_count
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("1", 999, larger_candidate_id, "left", 1, 1),
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO candidate_containment (
                    inventory_version,
                    smaller_candidate_id,
                    larger_candidate_id,
                    extension_side,
                    shared_occurrence_count,
                    shared_episode_count
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("2", smaller_candidate_id, larger_candidate_id, "left", 1, 1),
            )

        connection.execute(
            """
            INSERT INTO candidate_containment (
                inventory_version,
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("1", smaller_candidate_id, both_side_candidate_id, "both", 1, 1),
        )


def test_candidate_scores_enforce_uniqueness_and_foreign_keys(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        one_gram_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        two_gram_id = _insert_candidate(
            connection,
            candidate_key="j ai",
            display_text="j'ai",
            ngram_size=2,
        )
        connection.execute(
            """
            INSERT INTO candidate_scores (
                inventory_version,
                score_version,
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                frequency_score,
                dispersion_score,
                redundancy_penalty,
                final_score,
                lane_rank
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("1", "pilot-v1", one_gram_id, "1gram", 1, 1, None, 1, 0.8, 0.7, 0.0, 0.765, 1),
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO candidate_scores (
                    inventory_version,
                    score_version,
                    candidate_id,
                    ranking_lane,
                    passes_support_gate,
                    passes_quality_gate,
                    discard_family,
                    is_eligible
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("1", "pilot-v1", one_gram_id, "1gram", 0, 0, "support_floor", 0),
            )

        connection.execute(
            """
            INSERT INTO candidate_scores (
                inventory_version,
                score_version,
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("1", "pilot-v2", one_gram_id, "1gram", 0, 0, "support_floor", 0),
        )
        score_row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM candidate_scores
            WHERE inventory_version = '1'
            AND candidate_id = ?
            """,
            (one_gram_id,),
        ).fetchone()[0]

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO candidate_scores (
                    inventory_version,
                    score_version,
                    candidate_id,
                    ranking_lane,
                    passes_support_gate,
                    passes_quality_gate,
                    discard_family,
                    is_eligible
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("1", "pilot-v1", 999, "2gram", 0, 0, "support_floor", 0),
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO candidate_scores (
                    inventory_version,
                    score_version,
                    candidate_id,
                    ranking_lane,
                    passes_support_gate,
                    passes_quality_gate,
                    discard_family,
                    is_eligible
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("2", "pilot-v1", two_gram_id, "2gram", 0, 0, "support_floor", 0),
            )

    assert score_row_count == 2


def test_candidate_scores_support_lane_semantics(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        one_gram_id = _insert_candidate(
            connection,
            candidate_key="de",
            display_text="de",
            ngram_size=1,
        )
        two_gram_id = _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
        )
        three_gram_id = _insert_candidate(
            connection,
            candidate_key="il y a",
            display_text="il y a",
            ngram_size=3,
        )
        connection.execute(
            """
            INSERT INTO candidate_scores (
                inventory_version,
                score_version,
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                frequency_score,
                dispersion_score,
                association_score,
                boundary_score,
                redundancy_penalty,
                final_score,
                lane_rank
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "1",
                "pilot-v1",
                one_gram_id,
                "1gram",
                1,
                1,
                None,
                1,
                0.9,
                0.8,
                None,
                None,
                0.0,
                0.865,
                1,
            ),
        )
        connection.execute(
            """
            INSERT INTO candidate_scores (
                inventory_version,
                score_version,
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                frequency_score,
                dispersion_score,
                association_score,
                boundary_score,
                redundancy_penalty,
                final_score,
                lane_rank
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "1",
                "pilot-v1",
                two_gram_id,
                "2gram",
                1,
                1,
                None,
                1,
                0.7,
                0.6,
                0.9,
                0.5,
                0.1,
                0.69,
                2,
            ),
        )
        connection.execute(
            """
            INSERT INTO candidate_scores (
                inventory_version,
                score_version,
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                frequency_score,
                dispersion_score,
                association_score,
                boundary_score,
                redundancy_penalty,
                final_score,
                lane_rank
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "1",
                "pilot-v1",
                three_gram_id,
                "3gram",
                0,
                0,
                "support_floor",
                0,
                0.4,
                0.5,
                0.6,
                0.4,
                0.0,
                None,
                None,
            ),
        )
        rows = connection.execute(
            """
            SELECT
                ranking_lane,
                is_eligible,
                association_score,
                boundary_score,
                redundancy_penalty,
                final_score,
                lane_rank
            FROM candidate_scores
            WHERE inventory_version = '1'
            AND score_version = 'pilot-v1'
            ORDER BY candidate_id
            """
        ).fetchall()

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO candidate_scores (
                    inventory_version,
                    score_version,
                    candidate_id,
                    ranking_lane,
                    passes_support_gate,
                    passes_quality_gate,
                    discard_family,
                    is_eligible,
                    lane_rank
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("1", "pilot-v2", one_gram_id, "1gram", 0, 0, "support_floor", 0, 9),
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO candidate_scores (
                    inventory_version,
                    score_version,
                    candidate_id,
                    ranking_lane,
                    passes_support_gate,
                    passes_quality_gate,
                    discard_family,
                    is_eligible,
                    final_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("1", "pilot-v2", three_gram_id, "3gram", 0, 0, "support_floor", 0, 0.2),
            )

    assert [row["ranking_lane"] for row in rows] == ["1gram", "2gram", "3gram"]
    assert rows[0]["association_score"] is None
    assert rows[0]["boundary_score"] is None
    assert rows[0]["redundancy_penalty"] == 0.0
    assert rows[0]["final_score"] == pytest.approx(0.865)
    assert rows[0]["lane_rank"] == 1
    assert rows[1]["association_score"] == pytest.approx(0.9)
    assert rows[1]["boundary_score"] == pytest.approx(0.5)
    assert rows[1]["redundancy_penalty"] == pytest.approx(0.1)
    assert rows[1]["final_score"] == pytest.approx(0.69)
    assert rows[1]["lane_rank"] == 2
    assert rows[2]["is_eligible"] == 0
    assert rows[2]["final_score"] is None
    assert rows[2]["lane_rank"] is None
