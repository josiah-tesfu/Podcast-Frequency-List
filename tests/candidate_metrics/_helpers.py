from __future__ import annotations

import math

from podcast_frequency_list.db import upsert_episode
from podcast_frequency_list.tokens import (
    INVENTORY_VERSION,
    TOKENIZATION_VERSION,
    generate_sentence_spans,
    tokenize_sentence_text,
)


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
    punctuation_gap_occurrence_count: int | None = None,
    punctuation_gap_occurrence_ratio: float | None = None,
    punctuation_gap_edge_clitic_count: int | None = None,
    punctuation_gap_edge_clitic_ratio: float | None = None,
    max_component_information: float | None = None,
    min_component_information: float | None = None,
    high_information_token_count: int | None = None,
    max_show_share: float | None = None,
    top2_show_share: float | None = None,
    show_entropy: float | None = None,
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
                right_entropy,
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                max_component_information,
                min_component_information,
                high_information_token_count,
                max_show_share,
                top2_show_share,
                show_entropy
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                max_component_information,
                min_component_information,
                high_information_token_count,
                max_show_share,
                top2_show_share,
                show_entropy,
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


def _information_content(*, observed_frequency: int, total_tokens: int) -> float:
    return -math.log(observed_frequency / total_tokens)


def _high_information_threshold(*information_values: float) -> float:
    ordered_values = sorted(information_values)
    threshold_index = max(0, math.ceil(0.75 * len(ordered_values)) - 1)
    return ordered_values[threshold_index]


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
