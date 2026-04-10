from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.normalize import (
    NORMALIZATION_VERSION,
    TranscriptNormalizationError,
    TranscriptNormalizationService,
    normalize_transcript_text,
)
from podcast_frequency_list.pilot import PilotSelectionService


def _seed_normalization_data(db_path):
    bootstrap_database(db_path)
    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            podcast_index_id=None,
            title="Zack en Roue Libre by Zack Nani",
            feed_url="https://example.com/zack.xml",
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-1",
            title="Episode 1",
            audio_url="https://cdn.example.com/ep1.mp3",
            duration_seconds=120,
        )
        episode_id = connection.execute(
            "SELECT episode_id FROM episodes WHERE guid = ?",
            ("ep-1",),
        ).fetchone()["episode_id"]
        cursor = connection.execute(
            """
            INSERT INTO transcript_sources (
                episode_id,
                source_type,
                status,
                model,
                raw_path
            )
            VALUES (?, 'asr', 'ready', 'gpt-4o-mini-transcribe', ?)
            """,
            (episode_id, "/tmp/episode-1.txt"),
        )
        source_id = int(cursor.lastrowid)
        connection.execute(
            """
            INSERT INTO transcript_segments (
                source_id,
                episode_id,
                chunk_index,
                start_ms,
                end_ms,
                raw_text
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                source_id,
                episode_id,
                0,
                0,
                60_000,
                " Bonjour\u00a0tout  le monde...\n\nJ’ espère  que   ça va ?  ",
            ),
        )
        connection.commit()

    PilotSelectionService(db_path=db_path).create_pilot(
        show_id=show_id,
        name="zack-normalize-pilot",
        target_seconds=120,
    )
    return episode_id


def test_normalize_transcript_text_preserves_words_but_standardizes_spacing() -> None:
    assert (
        normalize_transcript_text(" Bonjour\u00a0tout  le monde...\n\nJ’ espère  que   ça va ?  ")
        == "Bonjour tout le monde... J'espère que ça va?"
    )


def test_transcript_normalization_service_normalizes_pilot_segments(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_id = _seed_normalization_data(db_path)
    service = TranscriptNormalizationService(db_path=db_path)

    result = service.normalize(pilot_name="zack-normalize-pilot")

    with connect(db_path) as connection:
        normalized_row = connection.execute(
            """
            SELECT normalization_version, normalized_text
            FROM normalized_segments
            WHERE episode_id = ?
            """,
            (episode_id,),
        ).fetchone()
        raw_row = connection.execute(
            """
            SELECT raw_text
            FROM transcript_segments
            WHERE episode_id = ?
            """,
            (episode_id,),
        ).fetchone()

    assert result.scope == "pilot"
    assert result.scope_value == "zack-normalize-pilot"
    assert result.selected_segments == 1
    assert result.normalized_segments == 1
    assert result.skipped_segments == 0
    assert normalized_row["normalization_version"] == NORMALIZATION_VERSION
    assert normalized_row["normalized_text"] == "Bonjour tout le monde... J'espère que ça va?"
    assert raw_row["raw_text"] == " Bonjour\u00a0tout  le monde...\n\nJ’ espère  que   ça va ?  "


def test_transcript_normalization_service_skips_current_version_without_force(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_id = _seed_normalization_data(db_path)
    service = TranscriptNormalizationService(db_path=db_path)

    first_result = service.normalize(episode_id=episode_id)
    second_result = service.normalize(episode_id=episode_id)

    with connect(db_path) as connection:
        rows = connection.execute(
            "SELECT COUNT(*) FROM normalized_segments WHERE episode_id = ?",
            (episode_id,),
        ).fetchone()[0]

    assert first_result.normalized_segments == 1
    assert second_result.normalized_segments == 0
    assert second_result.skipped_segments == 1
    assert rows == 1


def test_transcript_normalization_service_requires_exactly_one_scope(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    service = TranscriptNormalizationService(db_path=db_path)

    try:
        service.normalize()
    except TranscriptNormalizationError as exc:
        assert "exactly one" in str(exc)
    else:
        raise AssertionError("expected TranscriptNormalizationError")
