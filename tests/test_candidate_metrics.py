import math

import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.tokens import (
    INVENTORY_VERSION,
    TOKENIZATION_VERSION,
    CandidateMetricsError,
    CandidateMetricsService,
    generate_sentence_spans,
    tokenize_sentence_text,
)
from podcast_frequency_list.tokens.models import CandidateMetricsValidationResult


def _insert_episode_context(
    connection,
    *,
    show_id: int,
    guid: str,
    sentence_text: str = "en fait",
    source_model: str = "test-model",
) -> tuple[int, int, int]:
    upsert_episode(
        connection,
        show_id=show_id,
        guid=guid,
        title=f"Episode {guid}",
        audio_url=f"https://cdn.example.com/{guid}.mp3",
    )
    episode_id = int(
        connection.execute(
            """
            SELECT episode_id
            FROM episodes
            WHERE show_id = ?
            AND guid = ?
            """,
            (show_id, guid),
        ).fetchone()["episode_id"]
    )
    source_id = int(
        connection.execute(
            """
            INSERT INTO transcript_sources (
                episode_id,
                source_type,
                status,
                model
            )
            VALUES (?, 'asr', 'ready', ?)
            """,
            (episode_id, source_model),
        ).lastrowid
    )
    segment_id = int(
        connection.execute(
            """
            INSERT INTO transcript_segments (
                source_id,
                episode_id,
                chunk_index,
                raw_text
            )
            VALUES (?, ?, 0, ?)
            """,
            (source_id, episode_id, sentence_text),
        ).lastrowid
    )
    sentence_id = int(
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
            VALUES (?, ?, '1', 0, 0, ?, ?)
            """,
            (segment_id, episode_id, len(sentence_text), sentence_text),
        ).lastrowid
    )
    return episode_id, segment_id, sentence_id


def _insert_candidate(
    connection,
    *,
    candidate_key: str,
    display_text: str,
    ngram_size: int = 2,
    inventory_version: str = INVENTORY_VERSION,
    raw_frequency: int = 99,
    episode_dispersion: int = 99,
    show_dispersion: int = 99,
    t_score: float | None = None,
    npmi: float | None = None,
    left_context_type_count: int | None = None,
    right_context_type_count: int | None = None,
    left_entropy: float | None = None,
    right_entropy: float | None = None,
) -> int:
    return int(
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
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
            ),
        ).lastrowid
    )


def _insert_occurrence(
    connection,
    *,
    candidate_id: int,
    sentence_id: int,
    episode_id: int,
    segment_id: int,
    surface_text: str,
    inventory_version: str = INVENTORY_VERSION,
    token_start_index: int = 0,
    token_end_index: int = 2,
    char_start: int = 0,
    char_end: int | None = None,
) -> None:
    connection.execute(
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
            len(surface_text) if char_end is None else char_end,
            surface_text,
        ),
    )


def _insert_sentence_tokens(
    connection,
    *,
    sentence_id: int,
    episode_id: int,
    segment_id: int,
    sentence_text: str,
):
    tokens = tokenize_sentence_text(sentence_text)
    for token in tokens:
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
    return tokens


def _insert_occurrence_from_sentence(
    connection,
    *,
    candidate_id: int,
    sentence_id: int,
    episode_id: int,
    segment_id: int,
    sentence_text: str,
    candidate_key: str,
    inventory_version: str = INVENTORY_VERSION,
) -> None:
    tokens = _insert_sentence_tokens(
        connection,
        sentence_id=sentence_id,
        episode_id=episode_id,
        segment_id=segment_id,
        sentence_text=sentence_text,
    )
    spans = generate_sentence_spans(
        sentence_id=sentence_id,
        episode_id=episode_id,
        segment_id=segment_id,
        sentence_text=sentence_text,
        tokens=tokens,
    )
    span = next(span for span in spans if span.candidate_key == candidate_key)
    _insert_occurrence(
        connection,
        candidate_id=candidate_id,
        sentence_id=sentence_id,
        episode_id=episode_id,
        segment_id=segment_id,
        inventory_version=inventory_version,
        token_start_index=span.token_start_index,
        token_end_index=span.token_end_index,
        char_start=span.char_start,
        char_end=span.char_end,
        surface_text=span.surface_text,
    )


def _entropy(*counts: int) -> float:
    total = sum(counts)
    return -sum((count / total) * math.log(count / total) for count in counts)


def _t_score(
    *,
    observed: int,
    left_frequency: int,
    right_frequency: int,
    total_unigrams: int,
) -> float:
    expected = (left_frequency * right_frequency) / total_unigrams
    return (observed - expected) / math.sqrt(observed)


def _npmi(
    *,
    observed: int,
    left_frequency: int,
    right_frequency: int,
    total_unigrams: int,
) -> float:
    return math.log((observed * total_unigrams) / (left_frequency * right_frequency)) / -math.log(
        observed / total_unigrams
    )


def _insert_occurrence_bundle(
    connection,
    *,
    show_id: int,
    guid: str,
    sentence_text: str,
    occurrences: tuple[tuple[int, int, int, int, int, str], ...],
    source_model: str = "test-model",
) -> None:
    episode_id, segment_id, sentence_id = _insert_episode_context(
        connection,
        show_id=show_id,
        guid=guid,
        sentence_text=sentence_text,
        source_model=source_model,
    )
    _insert_sentence_tokens(
        connection,
        sentence_id=sentence_id,
        episode_id=episode_id,
        segment_id=segment_id,
        sentence_text=sentence_text,
    )
    for (
        candidate_id,
        token_start_index,
        token_end_index,
        char_start,
        char_end,
        surface_text,
    ) in occurrences:
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            token_start_index=token_start_index,
            token_end_index=token_end_index,
            char_start=char_start,
            char_end=char_end,
            surface_text=surface_text,
        )


def _load_containment_rows(connection, *, inventory_version: str = INVENTORY_VERSION):
    rows = connection.execute(
        """
        SELECT
            smaller.candidate_key AS smaller_key,
            larger.candidate_key AS larger_key,
            cc.extension_side,
            cc.shared_occurrence_count,
            cc.shared_episode_count
        FROM candidate_containment cc
        JOIN token_candidates smaller
            ON smaller.candidate_id = cc.smaller_candidate_id
            AND smaller.inventory_version = cc.inventory_version
        JOIN token_candidates larger
            ON larger.candidate_id = cc.larger_candidate_id
            AND larger.inventory_version = cc.inventory_version
        WHERE cc.inventory_version = ?
        ORDER BY smaller_key, larger_key
        """,
        (inventory_version,),
    ).fetchall()
    return tuple(dict(row) for row in rows)


def test_candidate_metrics_service_refreshes_metrics_and_display_text(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        other_show_id = upsert_show(
            connection,
            title="Other Show",
            feed_url="https://example.com/other.xml",
        )
        contexts = (
            _insert_episode_context(connection, show_id=show_id, guid="ep-1"),
            _insert_episode_context(connection, show_id=show_id, guid="ep-2"),
            _insert_episode_context(connection, show_id=other_show_id, guid="ep-3"),
            _insert_episode_context(connection, show_id=show_id, guid="ep-4"),
            _insert_episode_context(connection, show_id=show_id, guid="ep-5"),
            _insert_episode_context(connection, show_id=show_id, guid="ep-6"),
        )
        en_fait_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="En fait",
        )
        du_coup_id = _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="Du coup",
        )
        orphan_id = _insert_candidate(
            connection,
            candidate_key="orphan",
            display_text="orphan",
        )
        other_version_orphan_id = _insert_candidate(
            connection,
            candidate_key="other version orphan",
            display_text="other version orphan",
            inventory_version="2",
        )

        for context, surface_text in zip(
            contexts[:4],
            ("En fait", "en fait", "En fait", "en fait"),
            strict=True,
        ):
            episode_id, segment_id, sentence_id = context
            _insert_occurrence(
                connection,
                candidate_id=en_fait_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                surface_text=surface_text,
            )

        for context in contexts[4:]:
            episode_id, segment_id, sentence_id = context
            _insert_occurrence(
                connection,
                candidate_id=du_coup_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                surface_text="du coup",
            )

        connection.commit()

    result = CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        en_fait = connection.execute(
            """
            SELECT raw_frequency, episode_dispersion, show_dispersion, display_text
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (en_fait_id,),
        ).fetchone()
        du_coup = connection.execute(
            """
            SELECT raw_frequency, episode_dispersion, show_dispersion, display_text
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (du_coup_id,),
        ).fetchone()
        orphan_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (orphan_id,),
        ).fetchone()[0]
        other_version_orphan_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (other_version_orphan_id,),
        ).fetchone()[0]

    assert result.inventory_version == INVENTORY_VERSION
    assert result.selected_candidates == 3
    assert result.refreshed_candidates == 2
    assert result.deleted_orphan_candidates == 1
    assert result.occurrence_count == 6
    assert result.raw_frequency_total == 6
    assert result.episode_dispersion_total == 6
    assert result.show_dispersion_total == 3
    assert result.display_text_updates == 2

    assert en_fait["raw_frequency"] == 4
    assert en_fait["episode_dispersion"] == 4
    assert en_fait["show_dispersion"] == 2
    assert en_fait["display_text"] == "en fait"
    assert du_coup["raw_frequency"] == 2
    assert du_coup["episode_dispersion"] == 2
    assert du_coup["show_dispersion"] == 1
    assert du_coup["display_text"] == "du coup"
    assert orphan_count == 0
    assert other_version_orphan_count == 1


def test_candidate_metrics_service_refresh_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        episode_id, segment_id, sentence_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-1",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="tu vois",
            display_text="Tu vois",
        )
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            surface_text="tu vois",
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    first_result = service.refresh()
    second_result = service.refresh()

    assert first_result.selected_candidates == 1
    assert first_result.display_text_updates == 1
    assert second_result.selected_candidates == 1
    assert second_result.refreshed_candidates == 1
    assert second_result.deleted_orphan_candidates == 0
    assert second_result.occurrence_count == 1
    assert second_result.raw_frequency_total == 1
    assert second_result.display_text_updates == 0


def test_candidate_metrics_service_refreshes_boundary_metrics(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="En fait",
        )
        contexts = (
            _insert_episode_context(
                connection,
                show_id=show_id,
                guid="ep-1",
                sentence_text="En fait oui",
            ),
            _insert_episode_context(
                connection,
                show_id=show_id,
                guid="ep-2",
                sentence_text="bon en fait",
            ),
            _insert_episode_context(
                connection,
                show_id=show_id,
                guid="ep-3",
                sentence_text="bon en fait oui",
            ),
        )

        for context, sentence_text in zip(
            contexts,
            ("En fait oui", "bon en fait", "bon en fait oui"),
            strict=True,
        ):
            episode_id, segment_id, sentence_id = context
            _insert_occurrence_from_sentence(
                connection,
                candidate_id=candidate_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                sentence_text=sentence_text,
                candidate_key="en fait",
            )

        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()

    assert row["left_context_type_count"] == 2
    assert row["right_context_type_count"] == 2
    assert row["left_entropy"] == pytest.approx(_entropy(2, 1))
    assert row["right_entropy"] == pytest.approx(_entropy(2, 1))


def test_candidate_metrics_service_refreshes_fixed_boundary_contexts_to_zero_entropy(
    tmp_path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="En fait",
        )
        contexts = (
            _insert_episode_context(
                connection,
                show_id=show_id,
                guid="ep-1",
                sentence_text="bon en fait oui",
            ),
            _insert_episode_context(
                connection,
                show_id=show_id,
                guid="ep-2",
                sentence_text="bon en fait oui",
            ),
        )

        for episode_id, segment_id, sentence_id in contexts:
            _insert_occurrence_from_sentence(
                connection,
                candidate_id=candidate_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                sentence_text="bon en fait oui",
                candidate_key="en fait",
            )

        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()

    assert row["left_context_type_count"] == 1
    assert row["right_context_type_count"] == 1
    assert row["left_entropy"] == pytest.approx(0.0)
    assert row["right_entropy"] == pytest.approx(0.0)


def test_candidate_metrics_service_keeps_one_gram_boundary_metrics_null(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        episode_id, segment_id, sentence_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-1",
            sentence_text="bonjour oui",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="bonjour",
            display_text="bonjour",
            ngram_size=1,
        )
        _insert_occurrence_from_sentence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            sentence_text="bonjour oui",
            candidate_key="bonjour",
        )
        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()

    assert row["left_context_type_count"] is None
    assert row["right_context_type_count"] is None
    assert row["left_entropy"] is None
    assert row["right_entropy"] is None


def test_candidate_metrics_service_boundary_refresh_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        episode_id, segment_id, sentence_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-1",
            sentence_text="bon en fait oui",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="En fait",
        )
        _insert_occurrence_from_sentence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            sentence_text="bon en fait oui",
            candidate_key="en fait",
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()

    with connect(db_path) as connection:
        first_row = tuple(
            connection.execute(
                """
                SELECT
                    left_context_type_count,
                    right_context_type_count,
                    left_entropy,
                    right_entropy
                FROM token_candidates
                WHERE candidate_id = ?
                """,
                (candidate_id,),
            ).fetchone()
        )

    service.refresh()

    with connect(db_path) as connection:
        second_row = tuple(
            connection.execute(
                """
                SELECT
                    left_context_type_count,
                    right_context_type_count,
                    left_entropy,
                    right_entropy
                FROM token_candidates
                WHERE candidate_id = ?
                """,
                (candidate_id,),
            ).fetchone()
        )

    assert second_row == first_row


def test_candidate_metrics_service_refreshes_bigram_association_metrics(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        en_id = _insert_candidate(
            connection,
            candidate_key="en",
            display_text="en",
            ngram_size=1,
        )
        fait_id = _insert_candidate(
            connection,
            candidate_key="fait",
            display_text="fait",
            ngram_size=1,
        )
        en_fait_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="en fait",
            ngram_size=2,
        )
        bonjour_id = _insert_candidate(
            connection,
            candidate_key="bonjour",
            display_text="bonjour",
            ngram_size=1,
        )

        for index in range(6):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-bigram-shared-{index}",
                sentence_text="en fait",
                occurrences=(
                    (en_id, 0, 1, 0, 2, "en"),
                    (fait_id, 1, 2, 3, 7, "fait"),
                    (en_fait_id, 0, 2, 0, 7, "en fait"),
                ),
            )

        for index in range(4):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-bigram-en-{index}",
                sentence_text="en",
                occurrences=((en_id, 0, 1, 0, 2, "en"),),
            )

        for index in range(2):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-bigram-fait-{index}",
                sentence_text="fait",
                occurrences=((fait_id, 0, 1, 0, 4, "fait"),),
            )

        for index in range(4):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-bigram-bonjour-{index}",
                sentence_text="bonjour",
                occurrences=((bonjour_id, 0, 1, 0, 7, "bonjour"),),
            )

        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT raw_frequency, t_score, npmi
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (en_fait_id,),
        ).fetchone()

    assert row["raw_frequency"] == 6
    assert row["t_score"] == pytest.approx(
        _t_score(
            observed=6,
            left_frequency=10,
            right_frequency=8,
            total_unigrams=22,
        )
    )
    assert row["npmi"] == pytest.approx(
        _npmi(
            observed=6,
            left_frequency=10,
            right_frequency=8,
            total_unigrams=22,
        )
    )


def test_candidate_metrics_service_refreshes_trigram_association_by_weakest_split(
    tmp_path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        il_id = _insert_candidate(
            connection,
            candidate_key="il",
            display_text="il",
            ngram_size=1,
        )
        y_id = _insert_candidate(
            connection,
            candidate_key="y",
            display_text="y",
            ngram_size=1,
        )
        a_id = _insert_candidate(
            connection,
            candidate_key="a",
            display_text="a",
            ngram_size=1,
        )
        il_y_id = _insert_candidate(
            connection,
            candidate_key="il y",
            display_text="il y",
            ngram_size=2,
        )
        y_a_id = _insert_candidate(
            connection,
            candidate_key="y a",
            display_text="y a",
            ngram_size=2,
        )
        il_y_a_id = _insert_candidate(
            connection,
            candidate_key="il y a",
            display_text="il y a",
            ngram_size=3,
        )
        bonjour_id = _insert_candidate(
            connection,
            candidate_key="bonjour",
            display_text="bonjour",
            ngram_size=1,
        )

        for index in range(5):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-trigram-shared-{index}",
                sentence_text="il y a",
                occurrences=(
                    (il_id, 0, 1, 0, 2, "il"),
                    (y_id, 1, 2, 3, 4, "y"),
                    (a_id, 2, 3, 5, 6, "a"),
                    (il_y_id, 0, 2, 0, 4, "il y"),
                    (y_a_id, 1, 3, 3, 6, "y a"),
                    (il_y_a_id, 0, 3, 0, 6, "il y a"),
                ),
            )

        for index in range(3):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-trigram-il-y-{index}",
                sentence_text="il y",
                occurrences=(
                    (il_id, 0, 1, 0, 2, "il"),
                    (y_id, 1, 2, 3, 4, "y"),
                    (il_y_id, 0, 2, 0, 4, "il y"),
                ),
            )

        for index in range(2):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-trigram-il-{index}",
                sentence_text="il",
                occurrences=((il_id, 0, 1, 0, 2, "il"),),
            )

        for index in range(4):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-trigram-a-{index}",
                sentence_text="a",
                occurrences=((a_id, 0, 1, 0, 1, "a"),),
            )

        for index in range(3):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-trigram-bonjour-{index}",
                sentence_text="bonjour",
                occurrences=((bonjour_id, 0, 1, 0, 7, "bonjour"),),
            )

        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT raw_frequency, t_score, npmi
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (il_y_a_id,),
        ).fetchone()

    expected_t_score = min(
        _t_score(
            observed=5,
            left_frequency=10,
            right_frequency=5,
            total_unigrams=30,
        ),
        _t_score(
            observed=5,
            left_frequency=8,
            right_frequency=9,
            total_unigrams=30,
        ),
    )
    expected_npmi = min(
        _npmi(
            observed=5,
            left_frequency=10,
            right_frequency=5,
            total_unigrams=30,
        ),
        _npmi(
            observed=5,
            left_frequency=8,
            right_frequency=9,
            total_unigrams=30,
        ),
    )

    assert row["raw_frequency"] == 5
    assert row["t_score"] == pytest.approx(expected_t_score)
    assert row["npmi"] == pytest.approx(expected_npmi)


def test_candidate_metrics_service_keeps_one_gram_association_metrics_null(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="bonjour",
            display_text="bonjour",
            ngram_size=1,
        )
        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-association-one-gram",
            sentence_text="bonjour",
            occurrences=((candidate_id, 0, 1, 0, 7, "bonjour"),),
        )
        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT t_score, npmi
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()

    assert row["t_score"] is None
    assert row["npmi"] is None


def test_candidate_metrics_service_association_refresh_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        en_id = _insert_candidate(
            connection,
            candidate_key="en",
            display_text="en",
            ngram_size=1,
        )
        fait_id = _insert_candidate(
            connection,
            candidate_key="fait",
            display_text="fait",
            ngram_size=1,
        )
        en_fait_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="en fait",
            ngram_size=2,
        )
        for index in range(3):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-association-repeat-{index}",
                sentence_text="en fait",
                occurrences=(
                    (en_id, 0, 1, 0, 2, "en"),
                    (fait_id, 1, 2, 3, 7, "fait"),
                    (en_fait_id, 0, 2, 0, 7, "en fait"),
                ),
            )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()

    with connect(db_path) as connection:
        first_row = tuple(
            connection.execute(
                """
                SELECT t_score, npmi
                FROM token_candidates
                WHERE candidate_id = ?
                """,
                (en_fait_id,),
            ).fetchone()
        )

    service.refresh()

    with connect(db_path) as connection:
        second_row = tuple(
            connection.execute(
                """
                SELECT t_score, npmi
                FROM token_candidates
                WHERE candidate_id = ?
                """,
                (en_fait_id,),
            ).fetchone()
        )

    assert second_row == first_row


def test_candidate_metrics_service_refreshes_association_for_filtered_split_unigrams(
    tmp_path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        ai_id = _insert_candidate(
            connection,
            candidate_key="ai",
            display_text="ai",
            ngram_size=1,
        )
        j_ai_id = _insert_candidate(
            connection,
            candidate_key="j ai",
            display_text="j'ai",
            ngram_size=2,
        )
        bonjour_id = _insert_candidate(
            connection,
            candidate_key="bonjour",
            display_text="bonjour",
            ngram_size=1,
        )

        for index in range(4):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-association-clitic-shared-{index}",
                sentence_text="j ai",
                occurrences=(
                    (ai_id, 1, 2, 2, 4, "ai"),
                    (j_ai_id, 0, 2, 0, 4, "j ai"),
                ),
            )

        for index in range(2):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-association-clitic-ai-{index}",
                sentence_text="ai",
                occurrences=((ai_id, 0, 1, 0, 2, "ai"),),
            )

        for index in range(4):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-association-clitic-bonjour-{index}",
                sentence_text="bonjour",
                occurrences=((bonjour_id, 0, 1, 0, 7, "bonjour"),),
            )

        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT raw_frequency, t_score, npmi
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (j_ai_id,),
        ).fetchone()

    assert row["raw_frequency"] == 4
    assert row["t_score"] == pytest.approx(
        _t_score(
            observed=4,
            left_frequency=4,
            right_frequency=6,
            total_unigrams=10,
        )
    )
    assert row["npmi"] == pytest.approx(
        _npmi(
            observed=4,
            left_frequency=4,
            right_frequency=6,
            total_unigrams=10,
        )
    )


def test_candidate_metrics_service_refreshes_association_for_filtered_numeric_split_bigrams(
    tmp_path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        bonjour_id = _insert_candidate(
            connection,
            candidate_key="bonjour",
            display_text="bonjour",
            ngram_size=1,
        )
        trente_trois_bonjour_id = _insert_candidate(
            connection,
            candidate_key="33 bonjour",
            display_text="33 bonjour",
            ngram_size=2,
        )
        grand_chunk_id = _insert_candidate(
            connection,
            candidate_key="22 33 bonjour",
            display_text="22 33 bonjour",
            ngram_size=3,
        )
        salut_id = _insert_candidate(
            connection,
            candidate_key="salut",
            display_text="salut",
            ngram_size=1,
        )

        for index in range(3):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-association-numeric-shared-{index}",
                sentence_text="22 33 bonjour",
                occurrences=(
                    (bonjour_id, 2, 3, 6, 13, "bonjour"),
                    (trente_trois_bonjour_id, 1, 3, 3, 13, "33 bonjour"),
                    (grand_chunk_id, 0, 3, 0, 13, "22 33 bonjour"),
                ),
            )

        for index in range(2):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-association-numeric-bigram-{index}",
                sentence_text="33 bonjour",
                occurrences=(
                    (bonjour_id, 1, 2, 3, 10, "bonjour"),
                    (trente_trois_bonjour_id, 0, 2, 0, 10, "33 bonjour"),
                ),
            )

        for index in range(2):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-association-numeric-bonjour-{index}",
                sentence_text="bonjour",
                occurrences=((bonjour_id, 0, 1, 0, 7, "bonjour"),),
            )

        for index in range(5):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid=f"ep-association-numeric-salut-{index}",
                sentence_text="salut",
                occurrences=((salut_id, 0, 1, 0, 5, "salut"),),
            )

        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT raw_frequency, t_score, npmi
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (grand_chunk_id,),
        ).fetchone()

    expected_t_score = min(
        _t_score(
            observed=3,
            left_frequency=3,
            right_frequency=5,
            total_unigrams=12,
        ),
        _t_score(
            observed=3,
            left_frequency=3,
            right_frequency=7,
            total_unigrams=12,
        ),
    )
    expected_npmi = min(
        _npmi(
            observed=3,
            left_frequency=3,
            right_frequency=5,
            total_unigrams=12,
        ),
        _npmi(
            observed=3,
            left_frequency=3,
            right_frequency=7,
            total_unigrams=12,
        ),
    )

    assert row["raw_frequency"] == 3
    assert row["t_score"] == pytest.approx(expected_t_score)
    assert row["npmi"] == pytest.approx(expected_npmi)


def test_candidate_metrics_service_refreshes_direct_parent_containment_rows(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        envie_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        ai_envie_id = _insert_candidate(
            connection,
            candidate_key="ai envie",
            display_text="ai envie",
            ngram_size=2,
        )
        envie_de_id = _insert_candidate(
            connection,
            candidate_key="envie de",
            display_text="envie de",
            ngram_size=2,
        )
        j_ai_envie_id = _insert_candidate(
            connection,
            candidate_key="j ai envie",
            display_text="j ai envie",
            ngram_size=3,
        )
        ai_envie_de_id = _insert_candidate(
            connection,
            candidate_key="ai envie de",
            display_text="ai envie de",
            ngram_size=3,
        )

        for index in range(2):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid="ep-shared",
                sentence_text="j ai envie",
                source_model=f"test-model-{index}",
                occurrences=(
                    (envie_id, 2, 3, 5, 10, "envie"),
                    (ai_envie_id, 1, 3, 2, 10, "ai envie"),
                    (j_ai_envie_id, 0, 3, 0, 10, "j ai envie"),
                ),
            )

        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-other",
            sentence_text="ai envie de",
            occurrences=(
                (envie_id, 1, 2, 3, 8, "envie"),
                (ai_envie_id, 0, 2, 0, 8, "ai envie"),
                (envie_de_id, 1, 3, 3, 11, "envie de"),
                (ai_envie_de_id, 0, 3, 0, 11, "ai envie de"),
            ),
        )
        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        rows = _load_containment_rows(connection)
        skipped_pair_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM candidate_containment cc
            JOIN token_candidates smaller
                ON smaller.candidate_id = cc.smaller_candidate_id
                AND smaller.inventory_version = cc.inventory_version
            JOIN token_candidates larger
                ON larger.candidate_id = cc.larger_candidate_id
                AND larger.inventory_version = cc.inventory_version
            WHERE cc.inventory_version = ?
            AND smaller.candidate_key = 'envie'
            AND larger.candidate_key = 'j ai envie'
            """,
            (INVENTORY_VERSION,),
        ).fetchone()[0]

    assert rows == (
        {
            "smaller_key": "ai envie",
            "larger_key": "ai envie de",
            "extension_side": "right",
            "shared_occurrence_count": 1,
            "shared_episode_count": 1,
        },
        {
            "smaller_key": "ai envie",
            "larger_key": "j ai envie",
            "extension_side": "left",
            "shared_occurrence_count": 2,
            "shared_episode_count": 1,
        },
        {
            "smaller_key": "envie",
            "larger_key": "ai envie",
            "extension_side": "left",
            "shared_occurrence_count": 3,
            "shared_episode_count": 2,
        },
        {
            "smaller_key": "envie",
            "larger_key": "envie de",
            "extension_side": "right",
            "shared_occurrence_count": 1,
            "shared_episode_count": 1,
        },
        {
            "smaller_key": "envie de",
            "larger_key": "ai envie de",
            "extension_side": "left",
            "shared_occurrence_count": 1,
            "shared_episode_count": 1,
        },
    )
    assert skipped_pair_count == 0


def test_candidate_metrics_service_containment_refresh_replaces_stale_rows(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        envie_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        ai_envie_id = _insert_candidate(
            connection,
            candidate_key="ai envie",
            display_text="ai envie",
            ngram_size=2,
        )
        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-1",
            sentence_text="ai envie",
            occurrences=(
                (envie_id, 1, 2, 3, 8, "envie"),
                (ai_envie_id, 0, 2, 0, 8, "ai envie"),
            ),
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
            (INVENTORY_VERSION, envie_id, ai_envie_id, "left", 99, 99),
        )
        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        rows = _load_containment_rows(connection)

    assert rows == (
        {
            "smaller_key": "envie",
            "larger_key": "ai envie",
            "extension_side": "left",
            "shared_occurrence_count": 1,
            "shared_episode_count": 1,
        },
    )


def test_candidate_metrics_service_containment_refresh_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        envie_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        ai_envie_id = _insert_candidate(
            connection,
            candidate_key="ai envie",
            display_text="ai envie",
            ngram_size=2,
        )
        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-1",
            sentence_text="ai envie",
            occurrences=(
                (envie_id, 1, 2, 3, 8, "envie"),
                (ai_envie_id, 0, 2, 0, 8, "ai envie"),
            ),
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()

    with connect(db_path) as connection:
        first_rows = _load_containment_rows(connection)

    service.refresh()

    with connect(db_path) as connection:
        second_rows = _load_containment_rows(connection)

    assert second_rows == first_rows


def test_candidate_metrics_service_containment_refresh_aggregates_both_sides(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        de_id = _insert_candidate(
            connection,
            candidate_key="de",
            display_text="de",
            ngram_size=1,
        )
        de_de_id = _insert_candidate(
            connection,
            candidate_key="de de",
            display_text="de de",
            ngram_size=2,
        )
        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-1",
            sentence_text="de de",
            occurrences=(
                (de_id, 0, 1, 0, 2, "de"),
                (de_id, 1, 2, 3, 5, "de"),
                (de_de_id, 0, 2, 0, 5, "de de"),
            ),
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()

    with connect(db_path) as connection:
        rows = _load_containment_rows(connection)

    summary = service.list_candidates_by_key(candidate_keys=("de",))[0]

    assert rows == (
        {
            "smaller_key": "de",
            "larger_key": "de de",
            "extension_side": "both",
            "shared_occurrence_count": 2,
            "shared_episode_count": 1,
        },
    )
    assert summary.covered_by_any_count == 2
    assert summary.covered_by_any_ratio == pytest.approx(1.0)
    assert summary.independent_occurrence_count == 0
    assert summary.direct_parent_count == 1
    assert summary.dominant_parent_key == "de de"
    assert summary.dominant_parent_shared_count == 2
    assert summary.dominant_parent_share == pytest.approx(1.0)
    assert summary.dominant_parent_side == "both"


def test_candidate_metrics_service_errors_without_candidates(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    service = CandidateMetricsService(db_path=db_path)

    with pytest.raises(CandidateMetricsError, match="no token candidates"):
        service.refresh()


def test_candidate_metrics_service_lists_top_candidates_by_ngram(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="alpha",
            display_text="alpha",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=1,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_key="beta",
            display_text="beta",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
            show_dispersion=1,
            t_score=4.5,
            npmi=0.7,
            left_context_type_count=2,
            right_context_type_count=3,
            left_entropy=0.4,
            right_entropy=0.8,
        )
        _insert_candidate(
            connection,
            candidate_key="gamma",
            display_text="gamma",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_key="delta",
            display_text="delta",
            ngram_size=1,
            raw_frequency=99,
            episode_dispersion=1,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_key="epsilon",
            display_text="epsilon",
            ngram_size=2,
            inventory_version="2",
            raw_frequency=100,
            episode_dispersion=100,
            show_dispersion=1,
        )
        connection.commit()

    rows = CandidateMetricsService(db_path=db_path).list_top_candidates(
        ngram_size=2,
        limit=2,
    )

    assert [row.candidate_key for row in rows] == ["beta", "gamma"]
    assert rows[0].display_text == "beta"
    assert rows[0].ngram_size == 2
    assert rows[0].raw_frequency == 10
    assert rows[0].episode_dispersion == 3
    assert rows[0].show_dispersion == 1
    assert rows[0].t_score == 4.5
    assert rows[0].npmi == 0.7
    assert rows[0].left_context_type_count == 2
    assert rows[0].right_context_type_count == 3
    assert rows[0].left_entropy == 0.4
    assert rows[0].right_entropy == 0.8
    assert rows[0].covered_by_any_count == 0
    assert rows[0].covered_by_any_ratio == pytest.approx(0.0)
    assert rows[0].independent_occurrence_count == 10
    assert rows[0].direct_parent_count == 0
    assert rows[0].dominant_parent_key is None
    assert rows[0].dominant_parent_shared_count is None
    assert rows[0].dominant_parent_share is None
    assert rows[0].dominant_parent_side is None


def test_candidate_metrics_service_summary_empty_bucket_returns_empty_tuple(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="alpha",
            display_text="alpha",
            ngram_size=1,
        )
        connection.commit()

    rows = CandidateMetricsService(db_path=db_path).list_top_candidates(
        ngram_size=3,
        limit=20,
    )

    assert rows == ()


def test_candidate_metrics_service_summary_validates_arguments(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    service = CandidateMetricsService(db_path=db_path)

    with pytest.raises(CandidateMetricsError, match="ngram_size"):
        service.list_top_candidates(ngram_size=0)

    with pytest.raises(CandidateMetricsError, match="limit"):
        service.list_top_candidates(ngram_size=1, limit=0)


def test_candidate_metrics_service_validate_detects_stale_metrics(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        other_show_id = upsert_show(
            connection,
            title="Other Show",
            feed_url="https://example.com/other.xml",
        )
        episode_id, segment_id, sentence_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-1",
        )
        other_episode_id, other_segment_id, other_sentence_id = _insert_episode_context(
            connection,
            show_id=other_show_id,
            guid="ep-2",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="En fait",
            raw_frequency=99,
            episode_dispersion=99,
            show_dispersion=99,
        )
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            surface_text="En fait",
        )
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=other_sentence_id,
            episode_id=other_episode_id,
            segment_id=other_segment_id,
            surface_text="en fait",
        )
        connection.commit()

    result = CandidateMetricsService(db_path=db_path).validate()

    assert result == CandidateMetricsValidationResult(
        inventory_version=INVENTORY_VERSION,
        candidate_count=1,
        occurrence_count=2,
        raw_frequency_mismatch_count=1,
        episode_dispersion_mismatch_count=1,
        show_dispersion_mismatch_count=1,
        display_text_mismatch_count=1,
        foreign_key_issue_count=0,
    )


def test_candidate_metrics_service_validate_reports_clean_state_after_refresh(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        episode_id, segment_id, sentence_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-1",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="tu vois",
            display_text="Tu vois",
        )
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            surface_text="tu vois",
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()
    result = service.validate()

    assert result == CandidateMetricsValidationResult(
        inventory_version=INVENTORY_VERSION,
        candidate_count=1,
        occurrence_count=1,
        raw_frequency_mismatch_count=0,
        episode_dispersion_mismatch_count=0,
        show_dispersion_mismatch_count=0,
        display_text_mismatch_count=0,
        foreign_key_issue_count=0,
    )


def test_candidate_metrics_service_lists_candidates_with_step5_summary_fields(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        envie_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        ai_envie_id = _insert_candidate(
            connection,
            candidate_key="ai envie",
            display_text="ai envie",
            ngram_size=2,
        )
        envie_de_id = _insert_candidate(
            connection,
            candidate_key="envie de",
            display_text="envie de",
            ngram_size=2,
        )
        j_ai_envie_id = _insert_candidate(
            connection,
            candidate_key="j ai envie",
            display_text="j ai envie",
            ngram_size=3,
        )
        ai_envie_de_id = _insert_candidate(
            connection,
            candidate_key="ai envie de",
            display_text="ai envie de",
            ngram_size=3,
        )

        for index in range(2):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid="ep-shared",
                sentence_text="j ai envie",
                source_model=f"test-model-{index}",
                occurrences=(
                    (envie_id, 2, 3, 5, 10, "envie"),
                    (ai_envie_id, 1, 3, 2, 10, "ai envie"),
                    (j_ai_envie_id, 0, 3, 0, 10, "j ai envie"),
                ),
            )

        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-other",
            sentence_text="ai envie de",
            occurrences=(
                (envie_id, 1, 2, 3, 8, "envie"),
                (ai_envie_id, 0, 2, 0, 8, "ai envie"),
                (envie_de_id, 1, 3, 3, 11, "envie de"),
                (ai_envie_de_id, 0, 3, 0, 11, "ai envie de"),
            ),
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()
    rows = service.list_candidates_by_key(
        candidate_keys=("j ai envie", "envie", "ai envie"),
    )

    assert [row.candidate_key for row in rows] == ["j ai envie", "envie", "ai envie"]

    assert rows[0].covered_by_any_count is None
    assert rows[0].covered_by_any_ratio is None
    assert rows[0].independent_occurrence_count is None
    assert rows[0].direct_parent_count is None
    assert rows[0].dominant_parent_key is None
    assert rows[0].dominant_parent_shared_count is None
    assert rows[0].dominant_parent_share is None
    assert rows[0].dominant_parent_side is None

    assert rows[1].covered_by_any_count == 3
    assert rows[1].covered_by_any_ratio == pytest.approx(1.0)
    assert rows[1].independent_occurrence_count == 0
    assert rows[1].direct_parent_count == 2
    assert rows[1].dominant_parent_key == "ai envie"
    assert rows[1].dominant_parent_shared_count == 3
    assert rows[1].dominant_parent_share == pytest.approx(1.0)
    assert rows[1].dominant_parent_side == "left"

    assert rows[2].covered_by_any_count == 3
    assert rows[2].covered_by_any_ratio == pytest.approx(1.0)
    assert rows[2].independent_occurrence_count == 0
    assert rows[2].direct_parent_count == 2
    assert rows[2].dominant_parent_key == "j ai envie"
    assert rows[2].dominant_parent_shared_count == 2
    assert rows[2].dominant_parent_share == pytest.approx(2 / 3)
    assert rows[2].dominant_parent_side == "left"


def test_candidate_metrics_service_lists_candidates_by_key_in_requested_order(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="en fait",
            ngram_size=2,
            raw_frequency=12,
            episode_dispersion=4,
            show_dispersion=2,
        )
        _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=9,
            episode_dispersion=5,
            show_dispersion=2,
            t_score=5.25,
            npmi=0.83,
            left_context_type_count=4,
            right_context_type_count=2,
            left_entropy=1.2,
            right_entropy=0.35,
        )
        _insert_candidate(
            connection,
            candidate_key="il y a",
            display_text="il y a",
            ngram_size=3,
            raw_frequency=8,
            episode_dispersion=6,
            show_dispersion=3,
            t_score=3.1,
            npmi=0.61,
            left_context_type_count=2,
            right_context_type_count=5,
            left_entropy=0.25,
            right_entropy=1.4,
        )
        connection.commit()

    rows = CandidateMetricsService(db_path=db_path).list_candidates_by_key(
        candidate_keys=("il y a", "missing", "du coup", "il y a", "  "),
    )

    assert [row.candidate_key for row in rows] == ["il y a", "du coup"]
    assert rows[0].display_text == "il y a"
    assert rows[1].raw_frequency == 9
    assert rows[0].t_score == 3.1
    assert rows[0].npmi == 0.61
    assert rows[0].left_context_type_count == 2
    assert rows[0].right_context_type_count == 5
    assert rows[0].left_entropy == 0.25
    assert rows[0].right_entropy == 1.4
    assert rows[0].covered_by_any_count is None
    assert rows[0].covered_by_any_ratio is None
    assert rows[0].independent_occurrence_count is None
    assert rows[0].direct_parent_count is None
    assert rows[1].t_score == 5.25
    assert rows[1].npmi == 0.83
    assert rows[1].covered_by_any_count == 0
    assert rows[1].covered_by_any_ratio == pytest.approx(0.0)
    assert rows[1].independent_occurrence_count == 9
    assert rows[1].direct_parent_count == 0
    assert rows[1].dominant_parent_key is None
    assert rows[1].dominant_parent_shared_count is None
    assert rows[1].dominant_parent_share is None
    assert rows[1].dominant_parent_side is None
