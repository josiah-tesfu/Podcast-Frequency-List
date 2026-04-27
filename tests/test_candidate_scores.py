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
                show_dispersion
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                INVENTORY_VERSION,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
            ),
        ).lastrowid
    )


def test_refresh_candidate_scores_assigns_lanes_and_eligibility(tmp_path) -> None:
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
            candidate_key="ce",
            display_text="ce",
            ngram_size=1,
            raw_frequency=19,
            episode_dispersion=5,
        )
        _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
        )
        _insert_candidate(
            connection,
            candidate_key="dans le",
            display_text="dans le",
            ngram_size=2,
            raw_frequency=9,
            episode_dispersion=3,
        )
        _insert_candidate(
            connection,
            candidate_key="c est que",
            display_text="c'est que",
            ngram_size=3,
            raw_frequency=10,
            episode_dispersion=3,
        )
        _insert_candidate(
            connection,
            candidate_key="j ai envie",
            display_text="j'ai envie",
            ngram_size=3,
            raw_frequency=10,
            episode_dispersion=2,
        )
        connection.commit()

    service = CandidateScoresService(db_path=db_path)
    result = service.refresh()

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
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

    assert result.inventory_version == INVENTORY_VERSION
    assert result.score_version == SCORE_VERSION
    assert result.selected_candidates == 6
    assert result.stored_candidates == 6
    assert result.eligible_candidates == 3
    assert result.eligible_1gram_candidates == 1
    assert result.eligible_2gram_candidates == 1
    assert result.eligible_3gram_candidates == 1
    assert [row["ranking_lane"] for row in rows] == [
        "1gram",
        "1gram",
        "2gram",
        "2gram",
        "3gram",
        "3gram",
    ]
    assert [row["is_eligible"] for row in rows] == [1, 0, 1, 0, 1, 0]
    for row in rows:
        assert row["frequency_score"] is None
        assert row["dispersion_score"] is None
        assert row["association_score"] is None
        assert row["boundary_score"] is None
        assert row["redundancy_penalty"] is None
        assert row["final_score"] is None
        assert row["lane_rank"] is None


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
        _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
        )
        _insert_candidate(
            connection,
            candidate_key="c est que",
            display_text="c'est que",
            ngram_size=3,
            raw_frequency=9,
            episode_dispersion=3,
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
