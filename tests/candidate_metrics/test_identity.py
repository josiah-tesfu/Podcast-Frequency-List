import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_show
from podcast_frequency_list.tokens import CandidateMetricsService

from ._helpers import (
    _high_information_threshold,
    _information_content,
    _insert_candidate,
    _insert_episode_context,
    _insert_occurrence,
    _insert_occurrence_from_sentence,
    _insert_sentence_tokens,
)


def test_candidate_metrics_service_refreshes_unit_identity_metrics(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )

        en_fait_c_id = _insert_candidate(
            connection,
            candidate_key="en fait c",
            display_text="En fait, c",
            ngram_size=3,
        )
        non_mais_id = _insert_candidate(
            connection,
            candidate_key="non mais",
            display_text="Non, mais",
            ngram_size=2,
        )
        du_coup_id = _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
        )
        n_avez_id = _insert_candidate(
            connection,
            candidate_key="n avez",
            display_text="n'avez",
            ngram_size=2,
        )

        ep1_id, seg1_id, sent1_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-identity-1",
            sentence_text="En fait, c'est bon.",
        )
        sent1_text = "En fait, c'est bon."
        sent1_tokens = _insert_sentence_tokens(
            connection,
            sentence_id=sent1_id,
            episode_id=ep1_id,
            segment_id=seg1_id,
            sentence_text=sent1_text,
        )
        _insert_occurrence(
            connection,
            candidate_id=en_fait_c_id,
            sentence_id=sent1_id,
            episode_id=ep1_id,
            segment_id=seg1_id,
            token_start_index=0,
            token_end_index=3,
            char_start=sent1_tokens[0].char_start,
            char_end=sent1_tokens[2].char_end,
            surface_text=sent1_text[sent1_tokens[0].char_start : sent1_tokens[2].char_end],
        )

        ep2_id, seg2_id, sent2_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-identity-2",
            sentence_text="Non, mais oui.",
        )
        sent2_text = "Non, mais oui."
        sent2_tokens = _insert_sentence_tokens(
            connection,
            sentence_id=sent2_id,
            episode_id=ep2_id,
            segment_id=seg2_id,
            sentence_text=sent2_text,
        )
        _insert_occurrence(
            connection,
            candidate_id=non_mais_id,
            sentence_id=sent2_id,
            episode_id=ep2_id,
            segment_id=seg2_id,
            token_start_index=0,
            token_end_index=2,
            char_start=sent2_tokens[0].char_start,
            char_end=sent2_tokens[1].char_end,
            surface_text=sent2_text[sent2_tokens[0].char_start : sent2_tokens[1].char_end],
        )

        for guid in ("ep-identity-3", "ep-identity-4"):
            episode_id, segment_id, sentence_id = _insert_episode_context(
                connection,
                show_id=show_id,
                guid=guid,
                sentence_text="du coup",
            )
            _insert_occurrence_from_sentence(
                connection,
                candidate_id=du_coup_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                sentence_text="du coup",
                candidate_key="du coup",
            )

        ep5_id, seg5_id, sent5_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-identity-5",
            sentence_text="n'avez pas.",
        )
        sent5_text = "n'avez pas."
        sent5_tokens = _insert_sentence_tokens(
            connection,
            sentence_id=sent5_id,
            episode_id=ep5_id,
            segment_id=seg5_id,
            sentence_text=sent5_text,
        )
        _insert_occurrence(
            connection,
            candidate_id=n_avez_id,
            sentence_id=sent5_id,
            episode_id=ep5_id,
            segment_id=seg5_id,
            token_start_index=0,
            token_end_index=2,
            char_start=sent5_tokens[0].char_start,
            char_end=sent5_tokens[1].char_end,
            surface_text=sent5_text[sent5_tokens[0].char_start : sent5_tokens[1].char_end],
        )

        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    rare_information = _information_content(observed_frequency=1, total_tokens=15)
    common_information = _information_content(observed_frequency=2, total_tokens=15)
    high_information_threshold = _high_information_threshold(
        rare_information,
        rare_information,
        rare_information,
        rare_information,
        rare_information,
        rare_information,
        rare_information,
        rare_information,
        rare_information,
        rare_information,
        rare_information,
        common_information,
        common_information,
    )

    with connect(db_path) as connection:
        en_fait_c_row = connection.execute(
            """
            SELECT
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                starts_with_standalone_clitic,
                ends_with_standalone_clitic,
                max_component_information,
                min_component_information,
                high_information_token_count
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (en_fait_c_id,),
        ).fetchone()
        non_mais_row = connection.execute(
            """
            SELECT
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                starts_with_standalone_clitic,
                ends_with_standalone_clitic,
                max_component_information,
                min_component_information,
                high_information_token_count
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (non_mais_id,),
        ).fetchone()
        du_coup_row = connection.execute(
            """
            SELECT
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                starts_with_standalone_clitic,
                ends_with_standalone_clitic,
                max_component_information,
                min_component_information,
                high_information_token_count
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (du_coup_id,),
        ).fetchone()
        n_avez_row = connection.execute(
            """
            SELECT
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                starts_with_standalone_clitic,
                ends_with_standalone_clitic,
                max_component_information,
                min_component_information,
                high_information_token_count
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (n_avez_id,),
        ).fetchone()

    assert en_fait_c_row["punctuation_gap_occurrence_count"] == 1
    assert en_fait_c_row["punctuation_gap_occurrence_ratio"] == pytest.approx(1.0)
    assert en_fait_c_row["punctuation_gap_edge_clitic_count"] == 1
    assert en_fait_c_row["punctuation_gap_edge_clitic_ratio"] == pytest.approx(1.0)
    assert en_fait_c_row["starts_with_standalone_clitic"] == 0
    assert en_fait_c_row["ends_with_standalone_clitic"] == 1
    assert en_fait_c_row["max_component_information"] == pytest.approx(rare_information)
    assert en_fait_c_row["min_component_information"] == pytest.approx(rare_information)
    assert en_fait_c_row["high_information_token_count"] == 3

    assert non_mais_row["punctuation_gap_occurrence_count"] == 1
    assert non_mais_row["punctuation_gap_occurrence_ratio"] == pytest.approx(1.0)
    assert non_mais_row["punctuation_gap_edge_clitic_count"] == 0
    assert non_mais_row["punctuation_gap_edge_clitic_ratio"] == pytest.approx(0.0)
    assert non_mais_row["starts_with_standalone_clitic"] == 0
    assert non_mais_row["ends_with_standalone_clitic"] == 0
    assert non_mais_row["max_component_information"] == pytest.approx(rare_information)
    assert non_mais_row["min_component_information"] == pytest.approx(rare_information)
    assert non_mais_row["high_information_token_count"] == 2

    assert du_coup_row["punctuation_gap_occurrence_count"] == 0
    assert du_coup_row["punctuation_gap_occurrence_ratio"] == pytest.approx(0.0)
    assert du_coup_row["punctuation_gap_edge_clitic_count"] == 0
    assert du_coup_row["punctuation_gap_edge_clitic_ratio"] == pytest.approx(0.0)
    assert du_coup_row["starts_with_standalone_clitic"] == 0
    assert du_coup_row["ends_with_standalone_clitic"] == 0
    assert du_coup_row["max_component_information"] == pytest.approx(common_information)
    assert du_coup_row["min_component_information"] == pytest.approx(common_information)
    assert high_information_threshold == pytest.approx(rare_information)
    assert du_coup_row["high_information_token_count"] == 0

    assert n_avez_row["punctuation_gap_occurrence_count"] == 0
    assert n_avez_row["punctuation_gap_occurrence_ratio"] == pytest.approx(0.0)
    assert n_avez_row["punctuation_gap_edge_clitic_count"] == 0
    assert n_avez_row["punctuation_gap_edge_clitic_ratio"] == pytest.approx(0.0)
    assert n_avez_row["starts_with_standalone_clitic"] == 1
    assert n_avez_row["ends_with_standalone_clitic"] == 0
    assert n_avez_row["max_component_information"] == pytest.approx(rare_information)
    assert n_avez_row["min_component_information"] == pytest.approx(rare_information)
    assert n_avez_row["high_information_token_count"] == 2


def test_candidate_metrics_service_keeps_one_gram_unit_identity_metrics_null(tmp_path) -> None:
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
            guid="ep-identity-1gram",
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

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()

    with connect(db_path) as connection:
        first_row = tuple(
            connection.execute(
                """
                SELECT
                    punctuation_gap_occurrence_count,
                    punctuation_gap_occurrence_ratio,
                    punctuation_gap_edge_clitic_count,
                    punctuation_gap_edge_clitic_ratio,
                    starts_with_standalone_clitic,
                    ends_with_standalone_clitic,
                    max_component_information,
                    min_component_information,
                    high_information_token_count
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
                    punctuation_gap_occurrence_count,
                    punctuation_gap_occurrence_ratio,
                    punctuation_gap_edge_clitic_count,
                    punctuation_gap_edge_clitic_ratio,
                    starts_with_standalone_clitic,
                    ends_with_standalone_clitic,
                    max_component_information,
                    min_component_information,
                    high_information_token_count
                FROM token_candidates
                WHERE candidate_id = ?
                """,
                (candidate_id,),
            ).fetchone()
        )

    assert first_row == (None, None, None, None, None, None, None, None, None)
    assert second_row == first_row
