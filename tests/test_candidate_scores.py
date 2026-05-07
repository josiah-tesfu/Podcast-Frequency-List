import math

import pytest

from podcast_frequency_list.db import bootstrap_database, connect
from podcast_frequency_list.tokens import (
    INVENTORY_VERSION,
    CandidateScoresError,
    CandidateScoresService,
)
from podcast_frequency_list.tokens.scores import SCORE_VERSION


def _insert_candidate(
    connection,
    *,
    candidate_key: str,
    display_text: str,
    ngram_size: int,
    raw_frequency: int,
    episode_dispersion: int,
    show_dispersion: int = 1,
    t_score: float | None = None,
    npmi: float | None = None,
    left_entropy: float | None = None,
    right_entropy: float | None = None,
    punctuation_gap_occurrence_count: int | None = None,
    punctuation_gap_occurrence_ratio: float | None = None,
    punctuation_gap_edge_clitic_count: int | None = None,
    punctuation_gap_edge_clitic_ratio: float | None = None,
    max_component_information: float | None = None,
    min_component_information: float | None = None,
    high_information_token_count: int | None = None,
) -> int:
    if ngram_size >= 2:
        punctuation_gap_occurrence_count = (
            0 if punctuation_gap_occurrence_count is None else punctuation_gap_occurrence_count
        )
        punctuation_gap_occurrence_ratio = (
            0.0 if punctuation_gap_occurrence_ratio is None else punctuation_gap_occurrence_ratio
        )
        punctuation_gap_edge_clitic_count = (
            0 if punctuation_gap_edge_clitic_count is None else punctuation_gap_edge_clitic_count
        )
        punctuation_gap_edge_clitic_ratio = (
            0.0
            if punctuation_gap_edge_clitic_ratio is None
            else punctuation_gap_edge_clitic_ratio
        )
        max_component_information = (
            4.0 if max_component_information is None else max_component_information
        )
        min_component_information = (
            4.0 if min_component_information is None else min_component_information
        )
        high_information_token_count = (
            0 if high_information_token_count is None else high_information_token_count
        )

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
                left_entropy,
                right_entropy,
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                max_component_information,
                min_component_information,
                high_information_token_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                INVENTORY_VERSION,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
                t_score,
                npmi,
                left_entropy,
                right_entropy,
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                max_component_information,
                min_component_information,
                high_information_token_count,
            ),
        ).lastrowid
    )


def _insert_containment(
    connection,
    *,
    smaller_candidate_id: int,
    larger_candidate_id: int,
    shared_occurrence_count: int,
    shared_episode_count: int,
    extension_side: str = "left",
) -> None:
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
        (
            INVENTORY_VERSION,
            smaller_candidate_id,
            larger_candidate_id,
            extension_side,
            shared_occurrence_count,
            shared_episode_count,
        ),
    )


def _build_scored_test_db(tmp_path):
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="de",
            display_text="de",
            ngram_size=1,
            raw_frequency=20,
            episode_dispersion=5,
        )
        _insert_candidate(
            connection,
            candidate_key="et",
            display_text="et",
            ngram_size=1,
            raw_frequency=80,
            episode_dispersion=6,
        )
        _insert_candidate(
            connection,
            candidate_key="ce",
            display_text="ce",
            ngram_size=1,
            raw_frequency=19,
            episode_dispersion=5,
        )
        pense_que_id = _insert_candidate(
            connection,
            candidate_key="pense que",
            display_text="pense que",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
            t_score=1.0,
            npmi=0.2,
            left_entropy=0.2,
            right_entropy=0.4,
        )
        du_coup_id = _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=20,
            episode_dispersion=4,
            t_score=2.0,
            npmi=0.6,
            left_entropy=0.8,
            right_entropy=0.6,
            punctuation_gap_occurrence_count=0,
            punctuation_gap_occurrence_ratio=0.0,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=1.8,
            min_component_information=0.9,
            high_information_token_count=1,
        )
        en_fait_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="en fait",
            ngram_size=2,
            raw_frequency=40,
            episode_dispersion=6,
            t_score=4.0,
            npmi=0.8,
            left_entropy=1.0,
            right_entropy=1.2,
            punctuation_gap_occurrence_count=1,
            punctuation_gap_occurrence_ratio=0.025,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=2.6,
            min_component_information=0.8,
            high_information_token_count=1,
        )
        _insert_candidate(
            connection,
            candidate_key="dans le",
            display_text="dans le",
            ngram_size=2,
            raw_frequency=9,
            episode_dispersion=4,
            t_score=3.0,
            npmi=0.3,
            left_entropy=0.9,
            right_entropy=0.9,
        )
        je_pense_que_id = _insert_candidate(
            connection,
            candidate_key="je pense que",
            display_text="je pense que",
            ngram_size=3,
            raw_frequency=43,
            episode_dispersion=6,
            t_score=1.4,
            npmi=0.9,
            left_entropy=0.95,
            right_entropy=0.9,
        )
        il_y_a_id = _insert_candidate(
            connection,
            candidate_key="il y a",
            display_text="il y a",
            ngram_size=3,
            raw_frequency=20,
            episode_dispersion=6,
            t_score=1.0,
            npmi=0.7,
            left_entropy=0.8,
            right_entropy=0.7,
            punctuation_gap_occurrence_count=0,
            punctuation_gap_occurrence_ratio=0.0,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=2.0,
            min_component_information=0.7,
            high_information_token_count=1,
        )
        _insert_candidate(
            connection,
            candidate_key="j ai envie",
            display_text="j'ai envie",
            ngram_size=3,
            raw_frequency=10,
            episode_dispersion=3,
            t_score=0.4,
            npmi=0.4,
            left_entropy=0.4,
            right_entropy=0.6,
        )
        _insert_containment(
            connection,
            smaller_candidate_id=pense_que_id,
            larger_candidate_id=je_pense_que_id,
            shared_occurrence_count=10,
            shared_episode_count=3,
        )
        _insert_containment(
            connection,
            smaller_candidate_id=du_coup_id,
            larger_candidate_id=il_y_a_id,
            shared_occurrence_count=14,
            shared_episode_count=4,
        )
        _insert_containment(
            connection,
            smaller_candidate_id=en_fait_id,
            larger_candidate_id=je_pense_que_id,
            shared_occurrence_count=8,
            shared_episode_count=3,
        )
        connection.commit()

    service = CandidateScoresService(db_path=db_path)
    refresh_result = service.refresh()
    return db_path, service, refresh_result


def test_refresh_candidate_scores_populates_component_scores_and_ranks(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="de",
            display_text="de",
            ngram_size=1,
            raw_frequency=20,
            episode_dispersion=5,
        )
        _insert_candidate(
            connection,
            candidate_key="et",
            display_text="et",
            ngram_size=1,
            raw_frequency=80,
            episode_dispersion=6,
        )
        _insert_candidate(
            connection,
            candidate_key="ce",
            display_text="ce",
            ngram_size=1,
            raw_frequency=19,
            episode_dispersion=5,
        )
        pense_que_id = _insert_candidate(
            connection,
            candidate_key="pense que",
            display_text="pense que",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
            t_score=1.0,
            npmi=0.2,
            left_entropy=0.2,
            right_entropy=0.4,
        )
        du_coup_id = _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=20,
            episode_dispersion=4,
            t_score=2.0,
            npmi=0.6,
            left_entropy=0.8,
            right_entropy=0.6,
        )
        en_fait_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="en fait",
            ngram_size=2,
            raw_frequency=40,
            episode_dispersion=6,
            t_score=4.0,
            npmi=0.8,
            left_entropy=1.0,
            right_entropy=1.2,
        )
        _insert_candidate(
            connection,
            candidate_key="dans le",
            display_text="dans le",
            ngram_size=2,
            raw_frequency=9,
            episode_dispersion=4,
            t_score=3.0,
            npmi=0.3,
            left_entropy=0.9,
            right_entropy=0.9,
        )
        je_pense_que_id = _insert_candidate(
            connection,
            candidate_key="je pense que",
            display_text="je pense que",
            ngram_size=3,
            raw_frequency=43,
            episode_dispersion=6,
            t_score=1.4,
            npmi=0.9,
            left_entropy=0.95,
            right_entropy=0.9,
        )
        il_y_a_id = _insert_candidate(
            connection,
            candidate_key="il y a",
            display_text="il y a",
            ngram_size=3,
            raw_frequency=20,
            episode_dispersion=6,
            t_score=1.0,
            npmi=0.7,
            left_entropy=0.8,
            right_entropy=0.7,
        )
        _insert_candidate(
            connection,
            candidate_key="j ai envie",
            display_text="j'ai envie",
            ngram_size=3,
            raw_frequency=10,
            episode_dispersion=3,
            t_score=0.4,
            npmi=0.4,
            left_entropy=0.4,
            right_entropy=0.6,
        )
        _insert_containment(
            connection,
            smaller_candidate_id=pense_que_id,
            larger_candidate_id=je_pense_que_id,
            shared_occurrence_count=10,
            shared_episode_count=3,
        )
        _insert_containment(
            connection,
            smaller_candidate_id=du_coup_id,
            larger_candidate_id=il_y_a_id,
            shared_occurrence_count=14,
            shared_episode_count=4,
        )
        _insert_containment(
            connection,
            smaller_candidate_id=en_fait_id,
            larger_candidate_id=je_pense_que_id,
            shared_occurrence_count=8,
            shared_episode_count=3,
        )
        connection.commit()

    result = CandidateScoresService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
                cand.candidate_key,
                score.ranking_lane,
                score.passes_support_gate,
                score.passes_quality_gate,
                score.discard_family,
                score.is_eligible,
                score.frequency_score,
                score.dispersion_score,
                score.association_score,
                score.boundary_score,
                score.redundancy_penalty,
                score.final_score,
                score.lane_rank
            FROM candidate_scores score
            JOIN token_candidates cand
              ON cand.candidate_id = score.candidate_id
             AND cand.inventory_version = score.inventory_version
            WHERE score.inventory_version = ?
            AND score.score_version = ?
            ORDER BY cand.candidate_id
            """,
            (INVENTORY_VERSION, SCORE_VERSION),
        ).fetchall()

    row_by_key = {row["candidate_key"]: row for row in rows}

    expected_de_frequency = 0.0
    expected_et_frequency = 1.0
    expected_de_dispersion = 0.0
    expected_et_dispersion = 1.0
    expected_de_final = 0.65 * expected_de_frequency + 0.35 * expected_de_dispersion
    expected_et_final = 1.0

    du_coup_frequency = 0.0
    du_coup_dispersion = 0.0
    du_coup_association = 0.0
    du_coup_boundary = 0.0
    du_coup_final = 0.0
    il_y_a_frequency = (math.log1p(20) - math.log1p(10)) / (math.log1p(43) - math.log1p(10))
    il_y_a_dispersion = 1.0
    il_y_a_association = 0.65 * ((0.7 - 0.4) / (0.9 - 0.4)) + 0.35 * ((1.0 - 0.4) / (1.4 - 0.4))
    il_y_a_boundary = (0.7 - 0.4) / (0.9 - 0.4)
    il_y_a_support = 0.55 * il_y_a_frequency + 0.45 * il_y_a_dispersion
    il_y_a_final = (
        0.20 * il_y_a_support
        + 0.55 * il_y_a_association
        + 0.25 * il_y_a_boundary
    )

    assert result.selected_candidates == 10
    assert result.stored_candidates == 10
    assert result.support_pass_candidates == 8
    assert result.quality_pass_candidates == 7
    assert result.eligible_candidates == 7
    assert result.eligible_1gram_candidates == 2
    assert result.eligible_2gram_candidates == 2
    assert result.eligible_3gram_candidates == 3

    assert row_by_key["de"]["ranking_lane"] == "1gram"
    assert row_by_key["de"]["passes_support_gate"] == 1
    assert row_by_key["de"]["passes_quality_gate"] == 1
    assert row_by_key["de"]["discard_family"] is None
    assert row_by_key["de"]["is_eligible"] == 1
    assert row_by_key["de"]["frequency_score"] == pytest.approx(expected_de_frequency)
    assert row_by_key["de"]["dispersion_score"] == pytest.approx(expected_de_dispersion)
    assert row_by_key["de"]["association_score"] is None
    assert row_by_key["de"]["boundary_score"] is None
    assert row_by_key["de"]["redundancy_penalty"] == pytest.approx(0.0)
    assert row_by_key["de"]["final_score"] == pytest.approx(expected_de_final)
    assert row_by_key["de"]["lane_rank"] == 2

    assert row_by_key["et"]["frequency_score"] == pytest.approx(expected_et_frequency)
    assert row_by_key["et"]["dispersion_score"] == pytest.approx(expected_et_dispersion)
    assert row_by_key["et"]["final_score"] == pytest.approx(expected_et_final)
    assert row_by_key["et"]["lane_rank"] == 1

    assert row_by_key["ce"]["is_eligible"] == 0
    assert row_by_key["ce"]["passes_support_gate"] == 0
    assert row_by_key["ce"]["passes_quality_gate"] == 0
    assert row_by_key["ce"]["discard_family"] == "support_floor"
    assert row_by_key["ce"]["frequency_score"] is None
    assert row_by_key["ce"]["lane_rank"] is None

    assert row_by_key["pense que"]["passes_support_gate"] == 1
    assert row_by_key["pense que"]["passes_quality_gate"] == 0
    assert row_by_key["pense que"]["discard_family"] == "weak_multiword"
    assert row_by_key["pense que"]["is_eligible"] == 0
    assert row_by_key["pense que"]["association_score"] is None
    assert row_by_key["pense que"]["boundary_score"] is None
    assert row_by_key["pense que"]["redundancy_penalty"] is None
    assert row_by_key["pense que"]["final_score"] is None
    assert row_by_key["pense que"]["lane_rank"] is None

    assert row_by_key["du coup"]["frequency_score"] == pytest.approx(du_coup_frequency)
    assert row_by_key["du coup"]["dispersion_score"] == pytest.approx(du_coup_dispersion)
    assert row_by_key["du coup"]["association_score"] == pytest.approx(du_coup_association)
    assert row_by_key["du coup"]["boundary_score"] == pytest.approx(du_coup_boundary)
    assert row_by_key["du coup"]["redundancy_penalty"] == pytest.approx(0.0)
    assert row_by_key["du coup"]["final_score"] == pytest.approx(du_coup_final)
    assert row_by_key["du coup"]["lane_rank"] == 2

    assert row_by_key["en fait"]["association_score"] == pytest.approx(1.0)
    assert row_by_key["en fait"]["boundary_score"] == pytest.approx(1.0)
    assert row_by_key["en fait"]["redundancy_penalty"] == pytest.approx(0.0)
    assert row_by_key["en fait"]["lane_rank"] == 1

    assert row_by_key["dans le"]["is_eligible"] == 0
    assert row_by_key["dans le"]["passes_support_gate"] == 0
    assert row_by_key["dans le"]["passes_quality_gate"] == 0
    assert row_by_key["dans le"]["discard_family"] == "support_floor"
    assert row_by_key["dans le"]["final_score"] is None

    assert row_by_key["il y a"]["frequency_score"] == pytest.approx(il_y_a_frequency)
    assert row_by_key["il y a"]["dispersion_score"] == pytest.approx(il_y_a_dispersion)
    assert row_by_key["il y a"]["association_score"] == pytest.approx(il_y_a_association)
    assert row_by_key["il y a"]["boundary_score"] == pytest.approx(il_y_a_boundary)
    assert row_by_key["il y a"]["redundancy_penalty"] == pytest.approx(0.0)
    assert row_by_key["il y a"]["final_score"] == pytest.approx(il_y_a_final)
    assert row_by_key["il y a"]["lane_rank"] == 2

    assert row_by_key["j ai envie"]["association_score"] == pytest.approx(0.0)
    assert row_by_key["j ai envie"]["boundary_score"] == pytest.approx(0.0)
    assert row_by_key["j ai envie"]["redundancy_penalty"] == pytest.approx(0.0)
    assert row_by_key["j ai envie"]["final_score"] == pytest.approx(0.0)
    assert row_by_key["j ai envie"]["lane_rank"] == 3


def test_refresh_candidate_scores_requires_step4_metrics_for_eligible_multiword_candidates(
    tmp_path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
        )
        connection.commit()

    service = CandidateScoresService(db_path=db_path)

    with pytest.raises(CandidateScoresError) as exc_info:
        service.refresh()

    assert "missing association metrics" in str(exc_info.value)


def test_refresh_candidate_scores_requires_unit_identity_metrics_for_multiword_candidates(
    tmp_path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        candidate_id = _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
            t_score=1.0,
            npmi=0.5,
            left_entropy=0.5,
            right_entropy=0.6,
        )
        connection.execute(
            """
            UPDATE token_candidates
            SET punctuation_gap_edge_clitic_ratio = NULL,
                max_component_information = NULL
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        )
        connection.commit()

    service = CandidateScoresService(db_path=db_path)

    with pytest.raises(CandidateScoresError) as exc_info:
        service.refresh()

    assert "missing unit identity metrics" in str(exc_info.value)


def test_refresh_candidate_scores_assigns_edge_clitic_discard_family(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="moi j",
            display_text="moi, j",
            ngram_size=2,
            raw_frequency=12,
            episode_dispersion=4,
            t_score=1.4,
            npmi=0.5,
            left_entropy=1.2,
            right_entropy=1.4,
            punctuation_gap_occurrence_count=10,
            punctuation_gap_occurrence_ratio=0.83,
            punctuation_gap_edge_clitic_count=10,
            punctuation_gap_edge_clitic_ratio=0.83,
            max_component_information=5.2,
            min_component_information=4.3,
            high_information_token_count=0,
        )
        connection.commit()

    result = CandidateScoresService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                final_score,
                lane_rank
            FROM candidate_scores
            WHERE inventory_version = ?
            AND score_version = ?
            """,
            (INVENTORY_VERSION, SCORE_VERSION),
        ).fetchone()

    assert result.support_pass_candidates == 1
    assert result.quality_pass_candidates == 0
    assert result.eligible_candidates == 0
    assert row["passes_support_gate"] == 1
    assert row["passes_quality_gate"] == 0
    assert row["discard_family"] == "edge_clitic_gap"
    assert row["is_eligible"] == 0
    assert row["final_score"] is None
    assert row["lane_rank"] is None


def test_refresh_candidate_scores_rejects_high_punctuation_gap_rows(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="voila je",
            display_text="voilà, je",
            ngram_size=2,
            raw_frequency=12,
            episode_dispersion=4,
            t_score=1.5,
            npmi=0.4,
            left_entropy=1.8,
            right_entropy=1.9,
            punctuation_gap_occurrence_count=11,
            punctuation_gap_occurrence_ratio=0.92,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=6.2,
            min_component_information=4.4,
            high_information_token_count=1,
        )
        _insert_candidate(
            connection,
            candidate_key="non mais",
            display_text="non, mais",
            ngram_size=2,
            raw_frequency=12,
            episode_dispersion=4,
            t_score=1.4,
            npmi=0.34,
            left_entropy=1.7,
            right_entropy=1.8,
            punctuation_gap_occurrence_count=8,
            punctuation_gap_occurrence_ratio=0.67,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=5.9,
            min_component_information=4.2,
            high_information_token_count=0,
        )
        connection.commit()

    result = CandidateScoresService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
                cand.candidate_key,
                score.passes_support_gate,
                score.passes_quality_gate,
                score.discard_family,
                score.is_eligible,
                score.final_score,
                score.lane_rank
            FROM candidate_scores score
            JOIN token_candidates cand
              ON cand.candidate_id = score.candidate_id
             AND cand.inventory_version = score.inventory_version
            WHERE score.inventory_version = ?
            AND score.score_version = ?
            ORDER BY cand.candidate_key
            """,
            (INVENTORY_VERSION, SCORE_VERSION),
        ).fetchall()

    row_by_key = {row["candidate_key"]: row for row in rows}

    assert result.support_pass_candidates == 2
    assert result.quality_pass_candidates == 1
    assert result.eligible_candidates == 1

    assert row_by_key["voila je"]["passes_support_gate"] == 1
    assert row_by_key["voila je"]["passes_quality_gate"] == 0
    assert row_by_key["voila je"]["discard_family"] == "edge_clitic_gap"
    assert row_by_key["voila je"]["is_eligible"] == 0
    assert row_by_key["voila je"]["final_score"] is None
    assert row_by_key["voila je"]["lane_rank"] is None

    assert row_by_key["non mais"]["passes_support_gate"] == 1
    assert row_by_key["non mais"]["passes_quality_gate"] == 1
    assert row_by_key["non mais"]["discard_family"] is None
    assert row_by_key["non mais"]["is_eligible"] == 1
    assert row_by_key["non mais"]["final_score"] is not None
    assert row_by_key["non mais"]["lane_rank"] == 1


def test_refresh_candidate_scores_tightens_lexical_only_rescue(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="de gens",
            display_text="de gens",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=4,
            t_score=1.4,
            npmi=0.19,
            left_entropy=1.3,
            right_entropy=1.2,
            punctuation_gap_occurrence_count=0,
            punctuation_gap_occurrence_ratio=0.0,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=6.4,
            min_component_information=4.1,
            high_information_token_count=1,
        )
        _insert_candidate(
            connection,
            candidate_key="faut que",
            display_text="faut que",
            ngram_size=2,
            raw_frequency=11,
            episode_dispersion=5,
            t_score=1.5,
            npmi=0.296,
            left_entropy=0.31,
            right_entropy=1.34,
            punctuation_gap_occurrence_count=0,
            punctuation_gap_occurrence_ratio=0.0,
            punctuation_gap_edge_clitic_count=0,
            punctuation_gap_edge_clitic_ratio=0.0,
            max_component_information=6.74,
            min_component_information=4.0,
            high_information_token_count=0,
        )
        connection.commit()

    result = CandidateScoresService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
                cand.candidate_key,
                score.passes_support_gate,
                score.passes_quality_gate,
                score.discard_family,
                score.is_eligible,
                score.final_score,
                score.lane_rank
            FROM candidate_scores score
            JOIN token_candidates cand
              ON cand.candidate_id = score.candidate_id
             AND cand.inventory_version = score.inventory_version
            WHERE score.inventory_version = ?
            AND score.score_version = ?
            ORDER BY cand.candidate_key
            """,
            (INVENTORY_VERSION, SCORE_VERSION),
        ).fetchall()

    row_by_key = {row["candidate_key"]: row for row in rows}

    assert result.support_pass_candidates == 2
    assert result.quality_pass_candidates == 1
    assert result.eligible_candidates == 1

    assert row_by_key["de gens"]["passes_support_gate"] == 1
    assert row_by_key["de gens"]["passes_quality_gate"] == 0
    assert row_by_key["de gens"]["discard_family"] == "weak_multiword"
    assert row_by_key["de gens"]["is_eligible"] == 0
    assert row_by_key["de gens"]["final_score"] is None
    assert row_by_key["de gens"]["lane_rank"] is None

    assert row_by_key["faut que"]["passes_support_gate"] == 1
    assert row_by_key["faut que"]["passes_quality_gate"] == 1
    assert row_by_key["faut que"]["discard_family"] is None
    assert row_by_key["faut que"]["is_eligible"] == 1
    assert row_by_key["faut que"]["final_score"] is not None
    assert row_by_key["faut que"]["lane_rank"] == 1


def test_refresh_candidate_scores_is_deterministic(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="de",
            display_text="de",
            ngram_size=1,
            raw_frequency=20,
            episode_dispersion=5,
        )
        du_coup_id = _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
            t_score=1.0,
            npmi=0.5,
            left_entropy=0.5,
            right_entropy=0.6,
        )
        il_y_a_id = _insert_candidate(
            connection,
            candidate_key="il y a",
            display_text="il y a",
            ngram_size=3,
            raw_frequency=10,
            episode_dispersion=3,
            t_score=0.8,
            npmi=0.7,
            left_entropy=0.4,
            right_entropy=0.6,
        )
        _insert_containment(
            connection,
            smaller_candidate_id=du_coup_id,
            larger_candidate_id=il_y_a_id,
            shared_occurrence_count=7,
            shared_episode_count=3,
        )
        connection.commit()

    service = CandidateScoresService(db_path=db_path)
    first_result = service.refresh()
    with connect(db_path) as connection:
        first_rows = connection.execute(
            """
            SELECT
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                frequency_score,
                dispersion_score,
                association_score,
                boundary_score,
                redundancy_penalty,
                final_score,
                lane_rank
            FROM candidate_scores
            WHERE inventory_version = ?
            AND score_version = ?
            ORDER BY candidate_id
            """,
            (INVENTORY_VERSION, SCORE_VERSION),
        ).fetchall()

    second_result = service.refresh()
    with connect(db_path) as connection:
        second_rows = connection.execute(
            """
            SELECT
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                frequency_score,
                dispersion_score,
                association_score,
                boundary_score,
                redundancy_penalty,
                final_score,
                lane_rank
            FROM candidate_scores
            WHERE inventory_version = ?
            AND score_version = ?
            ORDER BY candidate_id
            """,
            (INVENTORY_VERSION, SCORE_VERSION),
        ).fetchall()

    assert first_result == second_result
    assert first_rows == second_rows


def test_refresh_candidate_scores_rejects_unsupported_ngram_sizes(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="je ne sais pas",
            display_text="je ne sais pas",
            ngram_size=4,
            raw_frequency=20,
            episode_dispersion=5,
        )
        connection.commit()

    service = CandidateScoresService(db_path=db_path)

    with pytest.raises(CandidateScoresError) as exc_info:
        service.refresh()

    assert "unsupported ngram sizes" in str(exc_info.value)
    assert "4" in str(exc_info.value)


def test_refresh_candidate_scores_without_candidates_fails(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    service = CandidateScoresService(db_path=db_path)

    with pytest.raises(CandidateScoresError) as exc_info:
        service.refresh()

    assert "no token candidates found" in str(exc_info.value)


def test_candidate_scores_summarize_matches_refreshed_counts(tmp_path) -> None:
    _db_path, service, refresh_result = _build_scored_test_db(tmp_path)

    summary = service.summarize()

    assert summary == refresh_result


def test_candidate_scores_list_top_candidates_by_lane(tmp_path) -> None:
    _db_path, service, _refresh_result = _build_scored_test_db(tmp_path)

    top_1gram_rows = service.list_top_candidates(ngram_size=1, limit=2)
    top_2gram_rows = service.list_top_candidates(ngram_size=2, limit=2)
    top_3gram_rows = service.list_top_candidates(ngram_size=3, limit=2)

    assert tuple(row.candidate_key for row in top_1gram_rows) == ("et", "de")
    assert top_1gram_rows[0].ranking_lane == "1gram"
    assert top_1gram_rows[0].score_version == SCORE_VERSION
    assert top_1gram_rows[0].passes_support_gate == 1
    assert top_1gram_rows[0].passes_quality_gate == 1
    assert top_1gram_rows[0].discard_family is None
    assert top_1gram_rows[0].is_eligible == 1
    assert top_1gram_rows[0].association_score is None
    assert top_1gram_rows[0].lane_rank == 1
    assert top_1gram_rows[1].lane_rank == 2

    assert tuple(row.candidate_key for row in top_2gram_rows) == ("en fait", "du coup")
    assert top_2gram_rows[0].ranking_lane == "2gram"
    assert top_2gram_rows[0].passes_support_gate == 1
    assert top_2gram_rows[0].passes_quality_gate == 1
    assert top_2gram_rows[0].discard_family is None
    assert top_2gram_rows[0].punctuation_gap_occurrence_count == 1
    assert top_2gram_rows[0].max_component_information == pytest.approx(2.6)
    assert top_2gram_rows[0].direct_parent_count == 1
    assert top_2gram_rows[0].dominant_parent_key == "je pense que"
    assert top_2gram_rows[0].final_score == pytest.approx(0.9)
    assert top_2gram_rows[1].lane_rank == 2

    assert tuple(row.candidate_key for row in top_3gram_rows) == ("je pense que", "il y a")
    assert top_3gram_rows[0].ranking_lane == "3gram"
    assert top_3gram_rows[0].redundancy_penalty == pytest.approx(0.0)
    assert top_3gram_rows[0].lane_rank == 1


def test_candidate_scores_list_candidates_by_key_keeps_order_and_ineligible_rows(tmp_path) -> None:
    _db_path, service, _refresh_result = _build_scored_test_db(tmp_path)

    rows = service.list_candidates_by_key(
        candidate_keys=("ce", "pense que", "en fait", "missing", "ce", "de")
    )

    assert tuple(row.candidate_key for row in rows) == ("ce", "pense que", "en fait", "de")

    ce_row, pense_que_row, en_fait_row, de_row = rows
    assert ce_row.ranking_lane == "1gram"
    assert ce_row.passes_support_gate == 0
    assert ce_row.passes_quality_gate == 0
    assert ce_row.discard_family == "support_floor"
    assert ce_row.is_eligible == 0
    assert ce_row.final_score is None
    assert ce_row.lane_rank is None

    assert pense_que_row.ranking_lane == "2gram"
    assert pense_que_row.passes_support_gate == 1
    assert pense_que_row.passes_quality_gate == 0
    assert pense_que_row.discard_family == "weak_multiword"
    assert pense_que_row.is_eligible == 0
    assert pense_que_row.final_score is None
    assert pense_que_row.lane_rank is None

    assert en_fait_row.ranking_lane == "2gram"
    assert en_fait_row.passes_support_gate == 1
    assert en_fait_row.passes_quality_gate == 1
    assert en_fait_row.discard_family is None
    assert en_fait_row.is_eligible == 1
    assert en_fait_row.punctuation_gap_occurrence_count == 1
    assert en_fait_row.high_information_token_count == 1
    assert en_fait_row.final_score == pytest.approx(0.9)
    assert en_fait_row.lane_rank == 1

    assert de_row.ranking_lane == "1gram"
    assert de_row.passes_support_gate == 1
    assert de_row.passes_quality_gate == 1
    assert de_row.discard_family is None
    assert de_row.is_eligible == 1
    assert de_row.final_score == pytest.approx(0.0)
    assert de_row.lane_rank == 2


def test_candidate_scores_list_global_candidates_uses_cross_lane_score_order(tmp_path) -> None:
    _db_path, service, _refresh_result = _build_scored_test_db(tmp_path)

    rows = service.list_global_candidates(limit=5)

    assert tuple(row.candidate_key for row in rows) == (
        "et",
        "je pense que",
        "en fait",
        "il y a",
        "de",
    )
    assert [row.ranking_lane for row in rows] == [
        "1gram",
        "3gram",
        "2gram",
        "3gram",
        "1gram",
    ]
    assert [row.is_eligible for row in rows] == [1, 1, 1, 1, 1]
    assert rows[0].final_score == pytest.approx(1.0)
    assert rows[1].final_score == pytest.approx(1.0)
    assert rows[2].final_score == pytest.approx(0.9)
    assert rows[2].lane_rank == 1
    assert rows[3].lane_rank == 2
    assert rows[4].lane_rank == 2
    assert all(row.candidate_key not in {"ce", "pense que"} for row in rows)


def test_candidate_scores_list_top_candidates_by_lane_supports_offset(tmp_path) -> None:
    _db_path, service, _refresh_result = _build_scored_test_db(tmp_path)

    rows = service.list_top_candidates(ngram_size=2, limit=1, offset=1)

    assert tuple(row.candidate_key for row in rows) == ("du coup",)


def test_candidate_scores_list_global_candidates_supports_offset(tmp_path) -> None:
    _db_path, service, _refresh_result = _build_scored_test_db(tmp_path)

    rows = service.list_global_candidates(limit=2, offset=1)

    assert tuple(row.candidate_key for row in rows) == ("je pense que", "en fait")
