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
                left_entropy,
                right_entropy
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    du_coup_frequency = (
        math.log1p(20) - math.log1p(10)
    ) / (math.log1p(40) - math.log1p(10))
    du_coup_dispersion = (4 - 3) / (6 - 3)
    du_coup_association = 0.65 * ((0.6 - 0.2) / (0.8 - 0.2)) + 0.35 * ((2.0 - 1.0) / (4.0 - 1.0))
    du_coup_boundary = (0.6 - 0.2) / (1.0 - 0.2)
    du_coup_support = 0.60 * du_coup_frequency + 0.40 * du_coup_dispersion
    du_coup_final = (
        0.20 * du_coup_support
        + 0.50 * du_coup_association
        + 0.20 * du_coup_boundary
    )
    pense_que_penalty = 1.0
    pense_que_final = -0.10
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
    assert result.eligible_candidates == 8
    assert result.eligible_1gram_candidates == 2
    assert result.eligible_2gram_candidates == 3
    assert result.eligible_3gram_candidates == 3

    assert row_by_key["de"]["ranking_lane"] == "1gram"
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
    assert row_by_key["ce"]["frequency_score"] is None
    assert row_by_key["ce"]["lane_rank"] is None

    assert row_by_key["pense que"]["association_score"] == pytest.approx(0.0)
    assert row_by_key["pense que"]["boundary_score"] == pytest.approx(0.0)
    assert row_by_key["pense que"]["redundancy_penalty"] == pytest.approx(pense_que_penalty)
    assert row_by_key["pense que"]["final_score"] == pytest.approx(pense_que_final)
    assert row_by_key["pense que"]["lane_rank"] == 3

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
