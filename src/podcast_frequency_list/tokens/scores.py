from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.tokens.inventory import INVENTORY_VERSION
from podcast_frequency_list.tokens.models import CandidateScoresResult

SCORE_VERSION = "pilot-v1"


class CandidateScoresError(RuntimeError):
    pass


@dataclass(frozen=True)
class _LaneSpec:
    ranking_lane: str
    min_raw_frequency: int
    min_episode_dispersion: int


_LANE_SPECS: dict[int, _LaneSpec] = {
    1: _LaneSpec("1gram", 20, 5),
    2: _LaneSpec("2gram", 10, 3),
    3: _LaneSpec("3gram", 10, 3),
}


class CandidateScoresService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def refresh(
        self,
        *,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> CandidateScoresResult:
        with connect(self.db_path) as connection:
            selected_candidates = _count_candidates(
                connection,
                inventory_version=inventory_version,
            )
            if selected_candidates == 0:
                raise CandidateScoresError(
                    f"no token candidates found for inventory_version={inventory_version!r}"
                )

            unsupported_sizes = _load_unsupported_ngram_sizes(
                connection,
                inventory_version=inventory_version,
            )
            if unsupported_sizes:
                raise CandidateScoresError(
                    "unsupported ngram sizes found for scoring lanes: "
                    + ", ".join(str(size) for size in unsupported_sizes)
                )

            connection.execute(
                """
                DELETE FROM candidate_scores
                WHERE inventory_version = ?
                AND score_version = ?
                """,
                (inventory_version, score_version),
            )
            connection.execute(
                f"""
                INSERT INTO candidate_scores (
                    inventory_version,
                    score_version,
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
                )
                SELECT
                    cand.inventory_version,
                    ?,
                    cand.candidate_id,
                    {_ranking_lane_case_sql()},
                    {_eligibility_case_sql()},
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL
                FROM token_candidates cand
                WHERE cand.inventory_version = ?
                """,
                (score_version, inventory_version),
            )
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS stored_candidates,
                    COALESCE(SUM(is_eligible), 0) AS eligible_candidates,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN ranking_lane = '1gram' AND is_eligible = 1 THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS eligible_1gram_candidates,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN ranking_lane = '2gram' AND is_eligible = 1 THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS eligible_2gram_candidates,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN ranking_lane = '3gram' AND is_eligible = 1 THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS eligible_3gram_candidates
                FROM candidate_scores
                WHERE inventory_version = ?
                AND score_version = ?
                """,
                (inventory_version, score_version),
            ).fetchone()
            connection.commit()

        return CandidateScoresResult(
            inventory_version=inventory_version,
            score_version=score_version,
            selected_candidates=selected_candidates,
            stored_candidates=int(row["stored_candidates"]),
            eligible_candidates=int(row["eligible_candidates"]),
            eligible_1gram_candidates=int(row["eligible_1gram_candidates"]),
            eligible_2gram_candidates=int(row["eligible_2gram_candidates"]),
            eligible_3gram_candidates=int(row["eligible_3gram_candidates"]),
        )


def _count_candidates(connection, *, inventory_version: str) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE inventory_version = ?
            """,
            (inventory_version,),
        ).fetchone()[0]
    )


def _load_unsupported_ngram_sizes(connection, *, inventory_version: str) -> tuple[int, ...]:
    rows = connection.execute(
        f"""
        SELECT DISTINCT ngram_size
        FROM token_candidates
        WHERE inventory_version = ?
        AND ngram_size NOT IN ({", ".join(str(size) for size in _LANE_SPECS)})
        ORDER BY ngram_size
        """,
        (inventory_version,),
    ).fetchall()
    return tuple(int(row["ngram_size"]) for row in rows)


def _ranking_lane_case_sql() -> str:
    clauses = [
        f"WHEN cand.ngram_size = {ngram_size} THEN '{lane_spec.ranking_lane}'"
        for ngram_size, lane_spec in _LANE_SPECS.items()
    ]
    return "CASE " + " ".join(clauses) + " END"


def _eligibility_case_sql() -> str:
    clauses = [
        (
            "WHEN cand.ngram_size = "
            f"{ngram_size} AND cand.raw_frequency >= {lane_spec.min_raw_frequency} "
            f"AND cand.episode_dispersion >= {lane_spec.min_episode_dispersion} THEN 1"
        )
        for ngram_size, lane_spec in _LANE_SPECS.items()
    ]
    return "CASE " + " ".join(clauses) + " ELSE 0 END"
