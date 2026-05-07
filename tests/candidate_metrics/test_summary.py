import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_show
from podcast_frequency_list.tokens import (
    INVENTORY_VERSION,
    CandidateMetricsError,
    CandidateMetricsService,
)
from podcast_frequency_list.tokens.models import CandidateMetricsValidationResult

from ._helpers import (
    _insert_candidate,
    _insert_episode_context,
    _insert_occurrence,
    _insert_occurrence_bundle,
)


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
            punctuation_gap_occurrence_count=1,
            punctuation_gap_occurrence_ratio=0.1,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=2.4,
            min_component_information=0.7,
            high_information_token_count=1,
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
    assert rows[0].punctuation_gap_occurrence_count == 1
    assert rows[0].punctuation_gap_occurrence_ratio == pytest.approx(0.1)
    assert rows[0].punctuation_gap_edge_clitic_count == 0
    assert rows[0].punctuation_gap_edge_clitic_ratio == pytest.approx(0.0)
    assert rows[0].max_component_information == pytest.approx(2.4)
    assert rows[0].min_component_information == pytest.approx(0.7)
    assert rows[0].high_information_token_count == 1
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

    with pytest.raises(CandidateMetricsError, match="offset"):
        service.list_top_candidates(ngram_size=1, offset=-1)


def test_candidate_metrics_service_lists_top_candidates_with_offset(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        for candidate_key, raw_frequency in (("alpha", 30), ("beta", 20), ("gamma", 10)):
            _insert_candidate(
                connection,
                candidate_key=candidate_key,
                display_text=candidate_key,
                ngram_size=2,
                raw_frequency=raw_frequency,
                episode_dispersion=1,
                show_dispersion=1,
            )
        connection.commit()

    rows = CandidateMetricsService(db_path=db_path).list_top_candidates(
        ngram_size=2,
        limit=1,
        offset=1,
    )

    assert [row.candidate_key for row in rows] == ["beta"]


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
            punctuation_gap_occurrence_count=0,
            punctuation_gap_occurrence_ratio=0.0,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=2.1,
            min_component_information=0.9,
            high_information_token_count=1,
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
    assert rows[0].punctuation_gap_occurrence_count == 0
    assert rows[0].punctuation_gap_occurrence_ratio == pytest.approx(0.0)
    assert rows[0].punctuation_gap_edge_clitic_count == 0
    assert rows[0].punctuation_gap_edge_clitic_ratio == pytest.approx(0.0)
    assert rows[0].max_component_information == pytest.approx(2.1)
    assert rows[0].min_component_information == pytest.approx(0.9)
    assert rows[0].high_information_token_count == 1
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
