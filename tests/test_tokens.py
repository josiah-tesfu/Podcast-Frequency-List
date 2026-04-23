from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.pilot import PilotSelectionService
from podcast_frequency_list.tokens import (
    TOKENIZATION_VERSION,
    SentenceTokenizationError,
    SentenceTokenizationService,
    tokenize_sentence_text,
)


def _seed_tokenization_data(db_path):
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
        source_cursor = connection.execute(
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
            VALUES (?, ?, 0, 0, 60000, ?)
            """,
            (source_id, episode_id, "J'ai envie. C'est-à-dire aujourd'hui."),
        )
        segment_id = int(segment_cursor.lastrowid)

        sentence_texts = (
            "J'ai envie de dire que l'homme est là.",
            "Vas-y, c'est-à-dire aujourd'hui: 22 fois.",
        )
        sentence_ids: list[int] = []
        for sentence_index, sentence_text in enumerate(sentence_texts):
            cursor = connection.execute(
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
                VALUES (?, ?, '1', ?, 0, ?, ?)
                """,
                (segment_id, episode_id, sentence_index, len(sentence_text), sentence_text),
            )
            sentence_ids.append(int(cursor.lastrowid))

        connection.commit()

    PilotSelectionService(db_path=db_path).create_pilot(
        show_id=show_id,
        name="zack-token-pilot",
        target_seconds=120,
    )
    return episode_id, tuple(sentence_ids)


def test_tokenize_sentence_text_splits_apostrophes_but_preserves_offsets() -> None:
    text = "J'ai vu l’homme aujourd’hui. Vas-y, c'est-à-dire OK 22 et 1-0!"

    tokens = tokenize_sentence_text(text)

    assert [token.token_key for token in tokens] == [
        "j",
        "ai",
        "vu",
        "l",
        "homme",
        "aujourd'hui",
        "vas-y",
        "c'est-à-dire",
        "ok",
        "22",
        "et",
        "1-0",
    ]
    assert [token.token_type for token in tokens][-4:] == ["word", "number", "word", "number"]
    assert all(text[token.char_start : token.char_end] == token.surface_text for token in tokens)


def test_tokenize_sentence_text_keeps_protected_and_hyphenated_forms() -> None:
    text = "Quelqu'un dit peut-être qu'est-ce que c'est."

    tokens = tokenize_sentence_text(text)

    assert [token.token_key for token in tokens] == [
        "quelqu'un",
        "dit",
        "peut-être",
        "qu",
        "est-ce",
        "que",
        "c",
        "est",
    ]


def test_sentence_tokenization_service_tokenizes_pilot_sentences(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_id, sentence_ids = _seed_tokenization_data(db_path)
    service = SentenceTokenizationService(db_path=db_path)

    result = service.tokenize(pilot_name="zack-token-pilot")

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT sentence_id, token_index, token_key, surface_text, token_type
            FROM sentence_tokens
            WHERE episode_id = ?
            ORDER BY sentence_id, token_index
            """,
            (episode_id,),
        ).fetchall()

    assert result.scope == "pilot"
    assert result.scope_value == "zack-token-pilot"
    assert result.tokenization_version == TOKENIZATION_VERSION
    assert result.selected_sentences == 2
    assert result.tokenized_sentences == 2
    assert result.skipped_sentences == 0
    assert result.created_tokens == 15
    assert result.episode_count == 1
    assert [row["token_key"] for row in rows if row["sentence_id"] == sentence_ids[0]] == [
        "j",
        "ai",
        "envie",
        "de",
        "dire",
        "que",
        "l",
        "homme",
        "est",
        "là",
    ]
    assert [row["token_key"] for row in rows if row["sentence_id"] == sentence_ids[1]] == [
        "vas-y",
        "c'est-à-dire",
        "aujourd'hui",
        "22",
        "fois",
    ]
    assert [row["token_type"] for row in rows if row["token_key"] == "22"] == ["number"]


def test_sentence_tokenization_service_skips_current_version_without_force(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_id, _sentence_ids = _seed_tokenization_data(db_path)
    service = SentenceTokenizationService(db_path=db_path)

    first_result = service.tokenize(episode_id=episode_id)
    second_result = service.tokenize(episode_id=episode_id)
    force_result = service.tokenize(episode_id=episode_id, force=True)

    with connect(db_path) as connection:
        row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM sentence_tokens
            WHERE episode_id = ?
            AND tokenization_version = ?
            """,
            (episode_id, TOKENIZATION_VERSION),
        ).fetchone()[0]

    assert first_result.created_tokens == 15
    assert second_result.created_tokens == 0
    assert second_result.skipped_sentences == 2
    assert force_result.created_tokens == 15
    assert row_count == 15


def test_sentence_tokenization_service_requires_exactly_one_scope(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    service = SentenceTokenizationService(db_path=db_path)

    try:
        service.tokenize()
    except SentenceTokenizationError as exc:
        assert "exactly one" in str(exc)
    else:
        raise AssertionError("expected SentenceTokenizationError")


def test_sentence_tokenization_service_ignores_stale_split_versions(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_id, sentence_ids = _seed_tokenization_data(db_path)
    service = SentenceTokenizationService(db_path=db_path)

    with connect(db_path) as connection:
        segment_id = connection.execute(
            """
            SELECT segment_id
            FROM segment_sentences
            WHERE sentence_id = ?
            """,
            (sentence_ids[0],),
        ).fetchone()["segment_id"]
        stale_sentence_text = "Ancien split ignoré."
        stale_sentence_id = int(
            connection.execute(
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
                VALUES (?, ?, '0', 99, 0, ?, ?)
                """,
                (segment_id, episode_id, len(stale_sentence_text), stale_sentence_text),
            ).lastrowid
        )
        connection.commit()

    result = service.tokenize(episode_id=episode_id)

    with connect(db_path) as connection:
        stale_token_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM sentence_tokens
            WHERE sentence_id = ?
            """,
            (stale_sentence_id,),
        ).fetchone()[0]

    assert result.selected_sentences == 2
    assert result.tokenized_sentences == 2
    assert stale_token_count == 0
