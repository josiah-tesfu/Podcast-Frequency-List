from collections import Counter

from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.sentences.service import SPLIT_VERSION
from podcast_frequency_list.tokens import (
    INVENTORY_VERSION,
    TOKENIZATION_VERSION,
    CandidateInventoryError,
    CandidateInventoryService,
    generate_sentence_spans,
    tokenize_sentence_text,
)


def _insert_sentence(
    connection,
    *,
    source_id: int,
    episode_id: int,
    chunk_index: int,
    sentence_text: str,
    split_version: str = SPLIT_VERSION,
    tokenization_version: str = TOKENIZATION_VERSION,
):
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
        VALUES (?, ?, ?, 0, 1000, ?)
        """,
        (source_id, episode_id, chunk_index, sentence_text),
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
        VALUES (?, ?, ?, 0, 0, ?, ?)
        """,
        (segment_id, episode_id, split_version, len(sentence_text), sentence_text),
    )
    sentence_id = int(sentence_cursor.lastrowid)

    for token in tokenize_sentence_text(sentence_text):
        connection.execute(
            """
            INSERT INTO sentence_tokens (
                sentence_id,
                episode_id,
                segment_id,
                tokenization_version,
                token_index,
                token_key,
                surface_text,
                char_start,
                char_end,
                token_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sentence_id,
                episode_id,
                segment_id,
                tokenization_version,
                token.token_index,
                token.token_key,
                token.surface_text,
                token.char_start,
                token.char_end,
                token.token_type,
            ),
        )

    return segment_id, sentence_id


def _seed_candidate_inventory_data(db_path, *, include_stale_versions: bool = False):
    bootstrap_database(db_path)
    current_texts = {
        "ep1_a": "J'ai envie de dire.",
        "ep1_b": "Tu vois.",
        "ep2_a": "J'ai envie aussi.",
        "ep2_b": "En fait, tu vois.",
    }

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Candidate Show",
            feed_url="https://example.com/candidate.xml",
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-1",
            title="Episode 1",
            audio_url="https://cdn.example.com/ep1.mp3",
            duration_seconds=120,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-2",
            title="Episode 2",
            audio_url="https://cdn.example.com/ep2.mp3",
            duration_seconds=120,
        )
        episode_ids = {
            row["guid"]: int(row["episode_id"])
            for row in connection.execute(
                "SELECT episode_id, guid FROM episodes ORDER BY episode_id"
            ).fetchall()
        }

        source_ids: dict[str, int] = {}
        for guid, episode_id in episode_ids.items():
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
                (episode_id, f"/tmp/{guid}.txt"),
            )
            source_ids[guid] = int(source_cursor.lastrowid)

        sentence_ids: dict[str, int] = {}
        segment_ids: dict[str, int] = {}
        for chunk_index, label in enumerate(("ep1_a", "ep1_b"), start=1):
            segment_id, sentence_id = _insert_sentence(
                connection,
                source_id=source_ids["ep-1"],
                episode_id=episode_ids["ep-1"],
                chunk_index=chunk_index,
                sentence_text=current_texts[label],
            )
            sentence_ids[label] = sentence_id
            segment_ids[label] = segment_id

        for chunk_index, label in enumerate(("ep2_a", "ep2_b"), start=1):
            segment_id, sentence_id = _insert_sentence(
                connection,
                source_id=source_ids["ep-2"],
                episode_id=episode_ids["ep-2"],
                chunk_index=chunk_index,
                sentence_text=current_texts[label],
            )
            sentence_ids[label] = sentence_id
            segment_ids[label] = segment_id

        if include_stale_versions:
            _insert_sentence(
                connection,
                source_id=source_ids["ep-1"],
                episode_id=episode_ids["ep-1"],
                chunk_index=99,
                sentence_text="Ignoreme unique.",
                split_version="0",
                tokenization_version=TOKENIZATION_VERSION,
            )
            _insert_sentence(
                connection,
                source_id=source_ids["ep-2"],
                episode_id=episode_ids["ep-2"],
                chunk_index=100,
                sentence_text="Ancien token.",
                split_version=SPLIT_VERSION,
                tokenization_version="0",
            )

        pilot_cursor = connection.execute(
            """
            INSERT INTO pilot_runs (
                show_id,
                name,
                target_seconds,
                selection_order
            )
            VALUES (?, ?, ?, ?)
            """,
            (show_id, "candidate-pilot", 240, "newest"),
        )
        pilot_run_id = int(pilot_cursor.lastrowid)
        connection.execute(
            """
            INSERT INTO pilot_run_episodes (
                pilot_run_id,
                episode_id,
                position,
                cumulative_seconds
            )
            VALUES (?, ?, 1, 120), (?, ?, 2, 240)
            """,
            (pilot_run_id, episode_ids["ep-1"], pilot_run_id, episode_ids["ep-2"]),
        )
        connection.commit()

    return {
        "episode_ids": {
            "ep1": episode_ids["ep-1"],
            "ep2": episode_ids["ep-2"],
        },
        "sentence_ids": sentence_ids,
        "segment_ids": segment_ids,
        "texts": current_texts,
    }


def _expected_inventory(sentence_texts: list[str]) -> tuple[int, Counter[str]]:
    counts: Counter[str] = Counter()
    total_occurrences = 0

    for sentence_id, sentence_text in enumerate(sentence_texts, start=1):
        spans = generate_sentence_spans(
            sentence_id=sentence_id,
            episode_id=1,
            segment_id=sentence_id,
            sentence_text=sentence_text,
            tokens=tokenize_sentence_text(sentence_text),
        )
        total_occurrences += len(spans)
        counts.update(span.candidate_key for span in spans)

    return total_occurrences, counts


def _replace_sentence_text(
    connection,
    *,
    sentence_id: int,
    episode_id: int,
    segment_id: int,
    sentence_text: str,
) -> None:
    connection.execute(
        """
        UPDATE transcript_segments
        SET raw_text = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE segment_id = ?
        """,
        (sentence_text, segment_id),
    )
    connection.execute(
        """
        UPDATE segment_sentences
        SET char_start = 0,
            char_end = ?,
            sentence_text = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE sentence_id = ?
        """,
        (len(sentence_text), sentence_text, sentence_id),
    )
    connection.execute(
        """
        DELETE FROM sentence_tokens
        WHERE sentence_id = ?
        AND tokenization_version = ?
        """,
        (sentence_id, TOKENIZATION_VERSION),
    )
    for token in tokenize_sentence_text(sentence_text):
        connection.execute(
            """
            INSERT INTO sentence_tokens (
                sentence_id,
                episode_id,
                segment_id,
                tokenization_version,
                token_index,
                token_key,
                surface_text,
                char_start,
                char_end,
                token_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sentence_id,
                episode_id,
                segment_id,
                TOKENIZATION_VERSION,
                token.token_index,
                token.token_key,
                token.surface_text,
                token.char_start,
                token.char_end,
                token.token_type,
            ),
        )


def test_candidate_inventory_service_generates_pilot_inventory(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    seeded = _seed_candidate_inventory_data(db_path)
    service = CandidateInventoryService(db_path=db_path)
    expected_occurrences, expected_counts = _expected_inventory(list(seeded["texts"].values()))

    result = service.generate(pilot_name="candidate-pilot")

    with connect(db_path) as connection:
        candidate_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE inventory_version = ?
            """,
            (INVENTORY_VERSION,),
        ).fetchone()[0]
        occurrence_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_occurrences
            WHERE inventory_version = ?
            """,
            (INVENTORY_VERSION,),
        ).fetchone()[0]
        j_ai_row = connection.execute(
            """
            SELECT raw_frequency, display_text
            FROM token_candidates
            WHERE inventory_version = ?
            AND candidate_key = 'j ai'
            """,
            (INVENTORY_VERSION,),
        ).fetchone()
        mismatch_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_occurrences occ
            JOIN segment_sentences sent
                ON sent.sentence_id = occ.sentence_id
            WHERE occ.inventory_version = ?
            AND occ.surface_text != substr(
                sent.sentence_text,
                occ.char_start + 1,
                occ.char_end - occ.char_start
            )
            """,
            (INVENTORY_VERSION,),
        ).fetchone()[0]

    assert result.scope == "pilot"
    assert result.scope_value == "candidate-pilot"
    assert result.inventory_version == INVENTORY_VERSION
    assert result.selected_sentences == 4
    assert result.processed_sentences == 4
    assert result.skipped_sentences == 0
    assert result.created_candidates == len(expected_counts)
    assert result.created_occurrences == expected_occurrences
    assert result.episode_count == 2
    assert candidate_count == len(expected_counts)
    assert occurrence_count == expected_occurrences
    assert j_ai_row["raw_frequency"] == expected_counts["j ai"]
    assert j_ai_row["display_text"] == "J'ai"
    assert mismatch_count == 0


def test_candidate_inventory_service_generates_episode_inventory(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    seeded = _seed_candidate_inventory_data(db_path)
    service = CandidateInventoryService(db_path=db_path)
    expected_occurrences, expected_counts = _expected_inventory(
        [seeded["texts"]["ep1_a"], seeded["texts"]["ep1_b"]]
    )

    result = service.generate(episode_id=seeded["episode_ids"]["ep1"])

    with connect(db_path) as connection:
        episode_ids = [
            row["episode_id"]
            for row in connection.execute(
                """
                SELECT DISTINCT episode_id
                FROM token_occurrences
                WHERE inventory_version = ?
                ORDER BY episode_id
                """,
                (INVENTORY_VERSION,),
            ).fetchall()
        ]
        j_ai_frequency = connection.execute(
            """
            SELECT raw_frequency
            FROM token_candidates
            WHERE inventory_version = ?
            AND candidate_key = 'j ai'
            """,
            (INVENTORY_VERSION,),
        ).fetchone()["raw_frequency"]

    assert result.scope == "episode"
    assert result.scope_value == str(seeded["episode_ids"]["ep1"])
    assert result.selected_sentences == 2
    assert result.processed_sentences == 2
    assert result.skipped_sentences == 0
    assert result.created_candidates == len(expected_counts)
    assert result.created_occurrences == expected_occurrences
    assert result.episode_count == 1
    assert episode_ids == [seeded["episode_ids"]["ep1"]]
    assert j_ai_frequency == expected_counts["j ai"]


def test_candidate_inventory_service_skips_current_version_without_force(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    _seed_candidate_inventory_data(db_path)
    service = CandidateInventoryService(db_path=db_path)

    first_result = service.generate(pilot_name="candidate-pilot")
    second_result = service.generate(pilot_name="candidate-pilot")

    with connect(db_path) as connection:
        candidate_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE inventory_version = ?
            """,
            (INVENTORY_VERSION,),
        ).fetchone()[0]
        occurrence_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_occurrences
            WHERE inventory_version = ?
            """,
            (INVENTORY_VERSION,),
        ).fetchone()[0]

    assert first_result.processed_sentences == 4
    assert second_result.processed_sentences == 0
    assert second_result.skipped_sentences == 4
    assert second_result.created_candidates == 0
    assert second_result.created_occurrences == 0
    assert candidate_count == 23
    assert occurrence_count == 31


def test_candidate_inventory_service_force_rebuilds_scope_and_cleans_orphans(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    seeded = _seed_candidate_inventory_data(db_path)
    service = CandidateInventoryService(db_path=db_path)

    service.generate(pilot_name="candidate-pilot")

    with connect(db_path) as connection:
        _replace_sentence_text(
            connection,
            sentence_id=seeded["sentence_ids"]["ep1_a"],
            episode_id=seeded["episode_ids"]["ep1"],
            segment_id=seeded["segment_ids"]["ep1_a"],
            sentence_text="J'ai envie.",
        )
        connection.commit()

    result = service.generate(episode_id=seeded["episode_ids"]["ep1"], force=True)

    with connect(db_path) as connection:
        dire_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE inventory_version = ?
            AND candidate_key = 'dire'
            """,
            (INVENTORY_VERSION,),
        ).fetchone()[0]
        j_ai_frequency = connection.execute(
            """
            SELECT raw_frequency
            FROM token_candidates
            WHERE inventory_version = ?
            AND candidate_key = 'j ai'
            """,
            (INVENTORY_VERSION,),
        ).fetchone()["raw_frequency"]
        episode_one_occurrences = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_occurrences
            WHERE inventory_version = ?
            AND episode_id = ?
            """,
            (INVENTORY_VERSION, seeded["episode_ids"]["ep1"]),
        ).fetchone()[0]

    assert result.processed_sentences == 2
    assert result.skipped_sentences == 0
    assert result.created_occurrences == 8
    assert dire_count == 0
    assert j_ai_frequency == 2
    assert episode_one_occurrences == 8


def test_candidate_inventory_service_uses_current_split_and_token_versions(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    _seed_candidate_inventory_data(db_path, include_stale_versions=True)
    service = CandidateInventoryService(db_path=db_path)

    result = service.generate(pilot_name="candidate-pilot")

    with connect(db_path) as connection:
        ignored_keys = {
            row["candidate_key"]
            for row in connection.execute(
                """
                SELECT candidate_key
                FROM token_candidates
                WHERE inventory_version = ?
                AND candidate_key IN ('ignoreme', 'unique', 'ancien', 'token')
                """,
                (INVENTORY_VERSION,),
            ).fetchall()
        }

    assert result.selected_sentences == 4
    assert result.created_candidates == 23
    assert result.created_occurrences == 31
    assert ignored_keys == set()


def test_candidate_inventory_service_requires_exactly_one_scope(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    service = CandidateInventoryService(db_path=db_path)

    try:
        service.generate()
    except CandidateInventoryError as exc:
        assert "exactly one" in str(exc)
    else:
        raise AssertionError("expected CandidateInventoryError")
