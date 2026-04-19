from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.pilot import PilotSelectionService
from podcast_frequency_list.sentences import (
    SPLIT_VERSION,
    SentenceSplitError,
    SentenceSplitService,
    split_segment_text,
)


def _seed_sentence_split_data(db_path):
    bootstrap_database(db_path)
    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
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

        segment_texts = (
            ("Bonjour tout le monde... J'espère que ça va? Oui.", "keep"),
            ("Ça, c'est à revoir.", "review"),
            ("Ça ne doit pas être gardé.", "remove"),
        )

        for chunk_index, (text, status) in enumerate(segment_texts):
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
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    episode_id,
                    chunk_index,
                    chunk_index * 10_000,
                    (chunk_index + 1) * 10_000,
                    text,
                ),
            )
            segment_id = int(segment_cursor.lastrowid)
            connection.execute(
                """
                INSERT INTO normalized_segments (
                    segment_id,
                    episode_id,
                    normalization_version,
                    normalized_text
                )
                VALUES (?, ?, '1', ?)
                """,
                (segment_id, episode_id, text),
            )
            connection.execute(
                """
                INSERT INTO segment_qc (
                    segment_id,
                    episode_id,
                    qc_version,
                    status,
                    reason_summary
                )
                VALUES (?, ?, '1', ?, ?)
                """,
                (segment_id, episode_id, status, f"{status}_summary"),
            )

        connection.commit()

    PilotSelectionService(db_path=db_path).create_pilot(
        show_id=show_id,
        name="zack-sentence-pilot",
        target_seconds=120,
    )
    return episode_id


def test_split_segment_text_preserves_spoken_short_sentences() -> None:
    result = split_segment_text(" Bonjour tout le monde... J'espère que ça va? Oui. ")

    assert tuple(sentence.sentence_text for sentence in result) == (
        "Bonjour tout le monde...",
        "J'espère que ça va?",
        "Oui.",
    )
    assert result[0].char_start == 1
    assert result[0].char_end == 25


def test_sentence_split_service_splits_keep_segments_only(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_id = _seed_sentence_split_data(db_path)
    service = SentenceSplitService(db_path=db_path)

    result = service.split(pilot_name="zack-sentence-pilot")

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT split_version, sentence_index, char_start, char_end, sentence_text
            FROM segment_sentences
            WHERE episode_id = ?
            ORDER BY sentence_index
            """,
            (episode_id,),
        ).fetchall()

    assert result.scope == "pilot"
    assert result.scope_value == "zack-sentence-pilot"
    assert result.selected_segments == 1
    assert result.created_sentences == 3
    assert result.skipped_segments == 0
    assert result.episode_count == 1
    assert rows[0]["split_version"] == SPLIT_VERSION
    assert [row["sentence_text"] for row in rows] == [
        "Bonjour tout le monde...",
        "J'espère que ça va?",
        "Oui.",
    ]


def test_sentence_split_service_skips_current_version_without_force(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_id = _seed_sentence_split_data(db_path)
    service = SentenceSplitService(db_path=db_path)

    first_result = service.split(episode_id=episode_id)
    second_result = service.split(episode_id=episode_id)

    with connect(db_path) as connection:
        row_count = connection.execute(
            "SELECT COUNT(*) FROM segment_sentences WHERE episode_id = ? AND split_version = ?",
            (episode_id, SPLIT_VERSION),
        ).fetchone()[0]

    assert first_result.created_sentences == 3
    assert second_result.created_sentences == 0
    assert second_result.skipped_segments == 1
    assert row_count == 3


def test_sentence_split_service_requires_exactly_one_scope(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    service = SentenceSplitService(db_path=db_path)

    try:
        service.split()
    except SentenceSplitError as exc:
        assert "exactly one" in str(exc)
    else:
        raise AssertionError("expected SentenceSplitError")
