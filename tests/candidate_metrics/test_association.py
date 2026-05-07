import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_show
from podcast_frequency_list.tokens import CandidateMetricsService

from ._helpers import _insert_candidate, _insert_occurrence_bundle, _npmi, _t_score


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
