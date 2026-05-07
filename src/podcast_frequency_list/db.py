from __future__ import annotations

import sqlite3
from pathlib import Path

from podcast_frequency_list.config import load_settings

SCHEMA_VERSION = "16"


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
        migrate_token_candidate_schema(connection)
        migrate_candidate_containment_schema(connection)
        migrate_candidate_scores_schema(connection)
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


_TOKEN_CANDIDATE_MIGRATIONS = (
    (
        "episode_dispersion",
        """
        ALTER TABLE token_candidates
        ADD COLUMN episode_dispersion INTEGER NOT NULL DEFAULT 0
        CHECK (episode_dispersion >= 0)
        """,
    ),
    (
        "show_dispersion",
        """
        ALTER TABLE token_candidates
        ADD COLUMN show_dispersion INTEGER NOT NULL DEFAULT 0
        CHECK (show_dispersion >= 0)
        """,
    ),
    (
        "t_score",
        """
        ALTER TABLE token_candidates
        ADD COLUMN t_score REAL
        """,
    ),
    (
        "npmi",
        """
        ALTER TABLE token_candidates
        ADD COLUMN npmi REAL
        """,
    ),
    (
        "left_context_type_count",
        """
        ALTER TABLE token_candidates
        ADD COLUMN left_context_type_count INTEGER
        CHECK (left_context_type_count IS NULL OR left_context_type_count >= 0)
        """,
    ),
    (
        "right_context_type_count",
        """
        ALTER TABLE token_candidates
        ADD COLUMN right_context_type_count INTEGER
        CHECK (right_context_type_count IS NULL OR right_context_type_count >= 0)
        """,
    ),
    (
        "left_entropy",
        """
        ALTER TABLE token_candidates
        ADD COLUMN left_entropy REAL
        CHECK (left_entropy IS NULL OR left_entropy >= 0)
        """,
    ),
    (
        "right_entropy",
        """
        ALTER TABLE token_candidates
        ADD COLUMN right_entropy REAL
        CHECK (right_entropy IS NULL OR right_entropy >= 0)
        """,
    ),
    (
        "punctuation_gap_occurrence_count",
        """
        ALTER TABLE token_candidates
        ADD COLUMN punctuation_gap_occurrence_count INTEGER
        CHECK (
            punctuation_gap_occurrence_count IS NULL
            OR punctuation_gap_occurrence_count >= 0
        )
        """,
    ),
    (
        "punctuation_gap_occurrence_ratio",
        """
        ALTER TABLE token_candidates
        ADD COLUMN punctuation_gap_occurrence_ratio REAL
        CHECK (
            punctuation_gap_occurrence_ratio IS NULL
            OR (
                punctuation_gap_occurrence_ratio >= 0
                AND punctuation_gap_occurrence_ratio <= 1
            )
        )
        """,
    ),
    (
        "punctuation_gap_edge_clitic_count",
        """
        ALTER TABLE token_candidates
        ADD COLUMN punctuation_gap_edge_clitic_count INTEGER
        CHECK (
            punctuation_gap_edge_clitic_count IS NULL
            OR punctuation_gap_edge_clitic_count >= 0
        )
        """,
    ),
    (
        "punctuation_gap_edge_clitic_ratio",
        """
        ALTER TABLE token_candidates
        ADD COLUMN punctuation_gap_edge_clitic_ratio REAL
        CHECK (
            punctuation_gap_edge_clitic_ratio IS NULL
            OR (
                punctuation_gap_edge_clitic_ratio >= 0
                AND punctuation_gap_edge_clitic_ratio <= 1
            )
        )
        """,
    ),
    (
        "max_component_information",
        """
        ALTER TABLE token_candidates
        ADD COLUMN max_component_information REAL
        CHECK (
            max_component_information IS NULL
            OR max_component_information >= 0
        )
        """,
    ),
    (
        "min_component_information",
        """
        ALTER TABLE token_candidates
        ADD COLUMN min_component_information REAL
        CHECK (
            min_component_information IS NULL
            OR min_component_information >= 0
        )
        """,
    ),
    (
        "high_information_token_count",
        """
        ALTER TABLE token_candidates
        ADD COLUMN high_information_token_count INTEGER
        CHECK (
            high_information_token_count IS NULL
            OR high_information_token_count >= 0
        )
        """,
    ),
)


def migrate_token_candidate_schema(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(token_candidates)")
    }
    if not columns:
        return

    for column_name, migration_sql in _TOKEN_CANDIDATE_MIGRATIONS:
        if column_name not in columns:
            connection.execute(migration_sql)


def migrate_candidate_containment_schema(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table'
        AND name = 'candidate_containment'
        """
    ).fetchone()
    if row is None:
        return

    table_sql = row["sql"] or ""
    if "'both'" in table_sql:
        return

    connection.execute("ALTER TABLE candidate_containment RENAME TO candidate_containment_old")
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
        INSERT INTO candidate_containment (
            inventory_version,
            smaller_candidate_id,
            larger_candidate_id,
            extension_side,
            shared_occurrence_count,
            shared_episode_count,
            created_at
        )
        SELECT
            inventory_version,
            smaller_candidate_id,
            larger_candidate_id,
            extension_side,
            shared_occurrence_count,
            shared_episode_count,
            created_at
        FROM candidate_containment_old
        """
    )
    connection.execute("DROP TABLE candidate_containment_old")
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidate_containment_larger
            ON candidate_containment (inventory_version, larger_candidate_id)
        """
    )


def migrate_candidate_scores_schema(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table'
        AND name = 'candidate_scores'
        """
    ).fetchone()
    if row is None:
        return

    table_sql = row["sql"] or ""
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(candidate_scores)").fetchall()
    }
    if {
        "passes_support_gate",
        "passes_quality_gate",
        "discard_family",
    } <= columns and "discard_family IS NOT NULL" in table_sql:
        return

    connection.execute("ALTER TABLE candidate_scores RENAME TO candidate_scores_old")
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
            is_eligible,
            frequency_score,
            dispersion_score,
            association_score,
            boundary_score,
            redundancy_penalty,
            final_score,
            lane_rank,
            created_at
        )
        SELECT
            inventory_version,
            score_version,
            candidate_id,
            ranking_lane,
            is_eligible,
            is_eligible,
            CASE
                WHEN is_eligible = 1 THEN NULL
                ELSE 'support_floor'
            END,
            is_eligible,
            frequency_score,
            dispersion_score,
            association_score,
            boundary_score,
            redundancy_penalty,
            final_score,
            lane_rank,
            created_at
        FROM candidate_scores_old
        """
    )
    connection.execute("DROP TABLE candidate_scores_old")
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidate_scores_lane_rank
            ON candidate_scores (
                inventory_version,
                score_version,
                ranking_lane,
                lane_rank
            )
        """
    )


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
    "migrate_token_candidate_schema",
    "migrate_legacy_schema",
    "update_show",
    "upsert_episode",
    "upsert_show",
    "upsert_transcript_source",
]
