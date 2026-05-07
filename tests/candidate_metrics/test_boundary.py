import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_show
from podcast_frequency_list.tokens import CandidateMetricsService

from ._helpers import (
    _entropy,
    _insert_candidate,
    _insert_episode_context,
    _insert_occurrence_from_sentence,
)


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
