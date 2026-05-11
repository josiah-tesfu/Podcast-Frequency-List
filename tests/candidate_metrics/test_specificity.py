import math

import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_show
from podcast_frequency_list.tokens import CandidateMetricsService

from ._helpers import (
    _insert_candidate,
    _insert_episode_context,
    _insert_occurrence_from_sentence,
)


def test_candidate_metrics_service_refreshes_show_specificity_metrics(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_ids = [
            upsert_show(
                connection,
                title=f"Show {index}",
                feed_url=f"https://example.com/show-{index}.xml",
            )
            for index in range(1, 4)
        ]
        bonjour_id = _insert_candidate(
            connection,
            candidate_key="bonjour",
            display_text="bonjour",
            ngram_size=1,
        )
        souvenir_id = _insert_candidate(
            connection,
            candidate_key="un souvenir",
            display_text="un souvenir",
            ngram_size=2,
        )
        lille_id = _insert_candidate(
            connection,
            candidate_key="a lille",
            display_text="a Lille",
            ngram_size=2,
        )

        _insert_candidate_occurrences(
            connection,
            candidate_id=bonjour_id,
            candidate_key="bonjour",
            sentence_text="bonjour",
            show_occurrence_counts=(2, 1, 1),
            show_ids=show_ids,
            guid_prefix="bonjour",
        )
        _insert_candidate_occurrences(
            connection,
            candidate_id=souvenir_id,
            candidate_key="un souvenir",
            sentence_text="un souvenir",
            show_occurrence_counts=(4, 1, 1),
            show_ids=show_ids,
            guid_prefix="souvenir",
        )
        _insert_candidate_occurrences(
            connection,
            candidate_id=lille_id,
            candidate_key="a lille",
            sentence_text="a lille",
            show_occurrence_counts=(3, 0, 0),
            show_ids=show_ids,
            guid_prefix="lille",
        )
        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        bonjour_row = connection.execute(
            """
            SELECT max_show_share, top2_show_share, show_entropy
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (bonjour_id,),
        ).fetchone()
        souvenir_row = connection.execute(
            """
            SELECT max_show_share, top2_show_share, show_entropy
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (souvenir_id,),
        ).fetchone()
        lille_row = connection.execute(
            """
            SELECT max_show_share, top2_show_share, show_entropy
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (lille_id,),
        ).fetchone()

    assert bonjour_row["max_show_share"] == pytest.approx(0.5)
    assert bonjour_row["top2_show_share"] == pytest.approx(0.75)
    assert bonjour_row["show_entropy"] == pytest.approx(
        _normalized_entropy((2, 1, 1), total_show_count=3)
    )

    assert souvenir_row["max_show_share"] == pytest.approx(4 / 6)
    assert souvenir_row["top2_show_share"] == pytest.approx(5 / 6)
    assert souvenir_row["show_entropy"] == pytest.approx(
        _normalized_entropy((4, 1, 1), total_show_count=3)
    )

    assert lille_row["max_show_share"] == pytest.approx(1.0)
    assert lille_row["top2_show_share"] == pytest.approx(1.0)
    assert lille_row["show_entropy"] == pytest.approx(0.0)


def _insert_candidate_occurrences(
    connection,
    *,
    candidate_id: int,
    candidate_key: str,
    sentence_text: str,
    show_occurrence_counts: tuple[int, ...],
    show_ids: list[int],
    guid_prefix: str,
) -> None:
    for show_id, occurrence_count in zip(show_ids, show_occurrence_counts, strict=True):
        for occurrence_index in range(occurrence_count):
            episode_id, segment_id, sentence_id = _insert_episode_context(
                connection,
                show_id=show_id,
                guid=f"{guid_prefix}-{show_id}-{occurrence_index}",
                sentence_text=sentence_text,
            )
            _insert_occurrence_from_sentence(
                connection,
                candidate_id=candidate_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                sentence_text=sentence_text,
                candidate_key=candidate_key,
            )


def _normalized_entropy(
    show_occurrence_counts: tuple[int, ...],
    *,
    total_show_count: int,
) -> float:
    total_occurrence_count = sum(show_occurrence_counts)
    return -sum(
        probability * math.log(probability)
        for probability in (
            show_occurrence_count / total_occurrence_count
            for show_occurrence_count in show_occurrence_counts
            if show_occurrence_count > 0
        )
    ) / math.log(total_show_count)
