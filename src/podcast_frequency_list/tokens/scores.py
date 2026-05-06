from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from sqlite3 import Connection, Row

from podcast_frequency_list.db import connect
from podcast_frequency_list.tokens.inventory import INVENTORY_VERSION
from podcast_frequency_list.tokens.models import CandidateScoresResult, CandidateSummaryRow
from podcast_frequency_list.tokens.spans import DEFAULT_MAX_NGRAM_SIZE

SCORE_VERSION = "pilot-v1"
_REDUNDANCY_THRESHOLD = 0.80
DEFAULT_SUMMARY_LIMIT = 20


class CandidateScoresError(RuntimeError):
    pass


@dataclass(frozen=True)
class _LaneSpec:
    ranking_lane: str
    min_raw_frequency: int
    min_episode_dispersion: int


@dataclass(frozen=True)
class _CandidateScoreInput:
    candidate_id: int
    candidate_key: str
    ngram_size: int
    raw_frequency: int
    episode_dispersion: int
    t_score: float | None
    npmi: float | None
    left_entropy: float | None
    right_entropy: float | None
    dominant_parent_share: float | None
    ranking_lane: str
    is_eligible: bool


@dataclass(frozen=True)
class _CandidateScoreRow:
    inventory_version: str
    score_version: str
    candidate_id: int
    ranking_lane: str
    is_eligible: int
    frequency_score: float | None
    dispersion_score: float | None
    association_score: float | None
    boundary_score: float | None
    redundancy_penalty: float | None
    final_score: float | None
    lane_rank: int | None


_LANE_SPECS: dict[int, _LaneSpec] = {
    1: _LaneSpec("1gram", 20, 5),
    2: _LaneSpec("2gram", 10, 3),
    3: _LaneSpec("3gram", 10, 3),
}


class CandidateScoresService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def summarize(
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
            return _build_candidate_scores_result(
                connection,
                inventory_version=inventory_version,
                score_version=score_version,
                selected_candidates=selected_candidates,
            )

    def list_candidates_by_key(
        self,
        *,
        candidate_keys: Iterable[str],
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> tuple[CandidateSummaryRow, ...]:
        with connect(self.db_path) as connection:
            return _CandidateScoreSummaryStore(
                connection=connection,
                inventory_version=inventory_version,
                score_version=score_version,
            ).list_candidates_by_key(candidate_keys)

    def list_top_candidates(
        self,
        *,
        ngram_size: int,
        limit: int = DEFAULT_SUMMARY_LIMIT,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> tuple[CandidateSummaryRow, ...]:
        _validate_ngram_size(ngram_size)
        _validate_limit(limit)

        with connect(self.db_path) as connection:
            return _CandidateScoreSummaryStore(
                connection=connection,
                inventory_version=inventory_version,
                score_version=score_version,
            ).list_top_candidates(ngram_size=ngram_size, limit=limit)

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

            candidate_inputs = _load_candidate_inputs(
                connection,
                inventory_version=inventory_version,
            )
            scored_rows = _build_scored_rows(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_inputs=candidate_inputs,
            )

            connection.execute(
                """
                DELETE FROM candidate_scores
                WHERE inventory_version = ?
                AND score_version = ?
                """,
                (inventory_version, score_version),
            )
            connection.executemany(
                """
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.inventory_version,
                        row.score_version,
                        row.candidate_id,
                        row.ranking_lane,
                        row.is_eligible,
                        row.frequency_score,
                        row.dispersion_score,
                        row.association_score,
                        row.boundary_score,
                        row.redundancy_penalty,
                        row.final_score,
                        row.lane_rank,
                    )
                    for row in scored_rows
                ],
            )
            connection.commit()

        with connect(self.db_path) as connection:
            return _build_candidate_scores_result(
                connection,
                inventory_version=inventory_version,
                score_version=score_version,
                selected_candidates=selected_candidates,
            )


def _count_candidates(connection: Connection, *, inventory_version: str) -> int:
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


def _build_candidate_scores_result(
    connection: Connection,
    *,
    inventory_version: str,
    score_version: str,
    selected_candidates: int,
) -> CandidateScoresResult:
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


def _load_candidate_inputs(
    connection: Connection,
    *,
    inventory_version: str,
) -> tuple[_CandidateScoreInput, ...]:
    rows = connection.execute(
        """
        WITH ranked_containment AS (
            SELECT
                cc.smaller_candidate_id AS candidate_id,
                cc.shared_occurrence_count AS dominant_parent_shared_count,
                ROW_NUMBER() OVER (
                    PARTITION BY cc.smaller_candidate_id
                    ORDER BY
                        cc.shared_occurrence_count DESC,
                        larger.raw_frequency DESC,
                        larger.candidate_key
                ) AS parent_rank
            FROM candidate_containment cc
            JOIN token_candidates larger
                ON larger.candidate_id = cc.larger_candidate_id
                AND larger.inventory_version = cc.inventory_version
            WHERE cc.inventory_version = ?
        ),
        dominant_parent AS (
            SELECT candidate_id, dominant_parent_shared_count
            FROM ranked_containment
            WHERE parent_rank = 1
        )
        SELECT
            cand.candidate_id,
            cand.candidate_key,
            cand.ngram_size,
            cand.raw_frequency,
            cand.episode_dispersion,
            cand.t_score,
            cand.npmi,
            cand.left_entropy,
            cand.right_entropy,
            CASE
                WHEN dominant_parent.dominant_parent_shared_count IS NOT NULL
                AND cand.raw_frequency > 0
                THEN 1.0 * dominant_parent.dominant_parent_shared_count / cand.raw_frequency
            END AS dominant_parent_share
        FROM token_candidates cand
        LEFT JOIN dominant_parent
            ON dominant_parent.candidate_id = cand.candidate_id
        WHERE cand.inventory_version = ?
        ORDER BY cand.candidate_id
        """,
        (inventory_version, inventory_version),
    ).fetchall()

    candidate_inputs: list[_CandidateScoreInput] = []
    unsupported_sizes: list[int] = []
    for row in rows:
        ngram_size = int(row["ngram_size"])
        lane_spec = _LANE_SPECS.get(ngram_size)
        if lane_spec is None:
            unsupported_sizes.append(ngram_size)
            continue

        raw_frequency = int(row["raw_frequency"])
        episode_dispersion = int(row["episode_dispersion"])
        candidate_inputs.append(
            _CandidateScoreInput(
                candidate_id=int(row["candidate_id"]),
                candidate_key=str(row["candidate_key"]),
                ngram_size=ngram_size,
                raw_frequency=raw_frequency,
                episode_dispersion=episode_dispersion,
                t_score=_optional_float(row["t_score"]),
                npmi=_optional_float(row["npmi"]),
                left_entropy=_optional_float(row["left_entropy"]),
                right_entropy=_optional_float(row["right_entropy"]),
                dominant_parent_share=_optional_float(row["dominant_parent_share"]),
                ranking_lane=lane_spec.ranking_lane,
                is_eligible=(
                    raw_frequency >= lane_spec.min_raw_frequency
                    and episode_dispersion >= lane_spec.min_episode_dispersion
                ),
            )
        )

    if unsupported_sizes:
        raise CandidateScoresError(
            "unsupported ngram sizes found for scoring lanes: "
            + ", ".join(str(size) for size in sorted(set(unsupported_sizes)))
        )

    return tuple(candidate_inputs)


def _build_scored_rows(
    *,
    inventory_version: str,
    score_version: str,
    candidate_inputs: tuple[_CandidateScoreInput, ...],
) -> tuple[_CandidateScoreRow, ...]:
    eligible_by_lane = {
        lane_spec.ranking_lane: [
            candidate
            for candidate in candidate_inputs
            if candidate.ranking_lane == lane_spec.ranking_lane and candidate.is_eligible
        ]
        for lane_spec in _LANE_SPECS.values()
    }

    _validate_lane_metrics(eligible_by_lane["2gram"], lane_name="2gram")
    _validate_lane_metrics(eligible_by_lane["3gram"], lane_name="3gram")

    scored_by_id: dict[int, _CandidateScoreRow] = {}
    for lane_name, eligible_candidates in eligible_by_lane.items():
        lane_rows = _score_lane(
            inventory_version=inventory_version,
            score_version=score_version,
            lane_name=lane_name,
            eligible_candidates=eligible_candidates,
        )
        scored_by_id.update({row.candidate_id: row for row in lane_rows})

    rows: list[_CandidateScoreRow] = []
    for candidate in candidate_inputs:
        scored_row = scored_by_id.get(candidate.candidate_id)
        if scored_row is not None:
            rows.append(scored_row)
            continue

        rows.append(
            _CandidateScoreRow(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_id=candidate.candidate_id,
                ranking_lane=candidate.ranking_lane,
                is_eligible=0,
                frequency_score=None,
                dispersion_score=None,
                association_score=None,
                boundary_score=None,
                redundancy_penalty=None,
                final_score=None,
                lane_rank=None,
            )
        )

    return tuple(rows)


def _validate_lane_metrics(
    candidates: list[_CandidateScoreInput],
    *,
    lane_name: str,
) -> None:
    for candidate in candidates:
        if candidate.t_score is None or candidate.npmi is None:
            raise CandidateScoresError(
                f"{lane_name} candidate {candidate.candidate_key!r} is missing association metrics"
            )
        if candidate.left_entropy is None or candidate.right_entropy is None:
            raise CandidateScoresError(
                f"{lane_name} candidate {candidate.candidate_key!r} is missing boundary metrics"
            )


def _score_lane(
    *,
    inventory_version: str,
    score_version: str,
    lane_name: str,
    eligible_candidates: list[_CandidateScoreInput],
) -> tuple[_CandidateScoreRow, ...]:
    if not eligible_candidates:
        return ()

    frequency_scores = _min_max_normalize(
        {
            candidate.candidate_id: math.log1p(candidate.raw_frequency)
            for candidate in eligible_candidates
        }
    )
    dispersion_scores = _min_max_normalize(
        {
            candidate.candidate_id: float(candidate.episode_dispersion)
            for candidate in eligible_candidates
        }
    )

    if lane_name == "1gram":
        scored_rows = [
            _CandidateScoreRow(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_id=candidate.candidate_id,
                ranking_lane=lane_name,
                is_eligible=1,
                frequency_score=frequency_scores[candidate.candidate_id],
                dispersion_score=dispersion_scores[candidate.candidate_id],
                association_score=None,
                boundary_score=None,
                redundancy_penalty=0.0,
                final_score=(
                    0.65 * frequency_scores[candidate.candidate_id]
                    + 0.35 * dispersion_scores[candidate.candidate_id]
                ),
                lane_rank=None,
            )
            for candidate in eligible_candidates
        ]
        return _assign_lane_ranks(scored_rows, eligible_candidates)

    association_scores = _association_scores(eligible_candidates)
    boundary_scores = _boundary_scores(eligible_candidates)

    scored_rows = []
    for candidate in eligible_candidates:
        frequency_score = frequency_scores[candidate.candidate_id]
        dispersion_score = dispersion_scores[candidate.candidate_id]
        association_score = association_scores[candidate.candidate_id]
        boundary_score = boundary_scores[candidate.candidate_id]

        if lane_name == "2gram":
            support_score = 0.60 * frequency_score + 0.40 * dispersion_score
            redundancy_penalty = _redundancy_penalty(candidate.dominant_parent_share)
            final_score = (
                0.20 * support_score
                + 0.50 * association_score
                + 0.20 * boundary_score
                - 0.10 * redundancy_penalty
            )
        else:
            support_score = 0.55 * frequency_score + 0.45 * dispersion_score
            redundancy_penalty = 0.0
            final_score = (
                0.20 * support_score
                + 0.55 * association_score
                + 0.25 * boundary_score
            )

        scored_rows.append(
            _CandidateScoreRow(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_id=candidate.candidate_id,
                ranking_lane=lane_name,
                is_eligible=1,
                frequency_score=frequency_score,
                dispersion_score=dispersion_score,
                association_score=association_score,
                boundary_score=boundary_score,
                redundancy_penalty=redundancy_penalty,
                final_score=final_score,
                lane_rank=None,
            )
        )

    return _assign_lane_ranks(scored_rows, eligible_candidates)


def _association_scores(
    candidates: list[_CandidateScoreInput],
) -> dict[int, float]:
    t_score_norms = _min_max_normalize(
        {
            candidate.candidate_id: float(candidate.t_score)
            for candidate in candidates
            if candidate.t_score is not None
        }
    )
    npmi_norms = _min_max_normalize(
        {
            candidate.candidate_id: float(candidate.npmi)
            for candidate in candidates
            if candidate.npmi is not None
        }
    )
    return {
        candidate.candidate_id: (
            0.65 * npmi_norms[candidate.candidate_id]
            + 0.35 * t_score_norms[candidate.candidate_id]
        )
        for candidate in candidates
    }


def _boundary_scores(
    candidates: list[_CandidateScoreInput],
) -> dict[int, float]:
    weaker_side_norms = _min_max_normalize(
        {
            candidate.candidate_id: min(
                float(candidate.left_entropy),
                float(candidate.right_entropy),
            )
            for candidate in candidates
            if candidate.left_entropy is not None and candidate.right_entropy is not None
        }
    )
    return {
        candidate.candidate_id: weaker_side_norms[candidate.candidate_id]
        for candidate in candidates
    }


def _redundancy_penalty(dominant_parent_share: float | None) -> float:
    if dominant_parent_share is None or dominant_parent_share < _REDUNDANCY_THRESHOLD:
        return 0.0
    return min(1.0, (dominant_parent_share - _REDUNDANCY_THRESHOLD) / (1.0 - _REDUNDANCY_THRESHOLD))


def _assign_lane_ranks(
    scored_rows: list[_CandidateScoreRow],
    candidates: list[_CandidateScoreInput],
) -> tuple[_CandidateScoreRow, ...]:
    candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    ordered_rows = sorted(
        scored_rows,
        key=lambda row: (
            -float(row.final_score),
            -candidate_by_id[row.candidate_id].raw_frequency,
            -candidate_by_id[row.candidate_id].episode_dispersion,
            candidate_by_id[row.candidate_id].candidate_key,
        ),
    )
    return tuple(
        _CandidateScoreRow(
            inventory_version=row.inventory_version,
            score_version=row.score_version,
            candidate_id=row.candidate_id,
            ranking_lane=row.ranking_lane,
            is_eligible=row.is_eligible,
            frequency_score=row.frequency_score,
            dispersion_score=row.dispersion_score,
            association_score=row.association_score,
            boundary_score=row.boundary_score,
            redundancy_penalty=row.redundancy_penalty,
            final_score=row.final_score,
            lane_rank=rank,
        )
        for rank, row in enumerate(ordered_rows, start=1)
    )


def _min_max_normalize(values: dict[int, float]) -> dict[int, float]:
    if not values:
        return {}

    min_value = min(values.values())
    max_value = max(values.values())
    if max_value == min_value:
        return {candidate_id: 1.0 for candidate_id in values}

    scale = max_value - min_value
    return {
        candidate_id: (value - min_value) / scale
        for candidate_id, value in values.items()
    }


_SCORE_SUMMARY_COLUMNS_SQL = f"""
    cand.candidate_key,
    cand.display_text,
    cand.ngram_size,
    cand.raw_frequency,
    cand.episode_dispersion,
    cand.show_dispersion,
    cand.t_score,
    cand.npmi,
    cand.left_context_type_count,
    cand.right_context_type_count,
    cand.left_entropy,
    cand.right_entropy,
    CASE
        WHEN cand.ngram_size < {DEFAULT_MAX_NGRAM_SIZE}
        THEN COALESCE(covered_occurrences.covered_by_any_count, 0)
    END AS covered_by_any_count,
    CASE
        WHEN cand.ngram_size < {DEFAULT_MAX_NGRAM_SIZE}
        AND cand.raw_frequency > 0
        THEN 1.0 * COALESCE(covered_occurrences.covered_by_any_count, 0) / cand.raw_frequency
    END AS covered_by_any_ratio,
    CASE
        WHEN cand.ngram_size < {DEFAULT_MAX_NGRAM_SIZE}
        THEN cand.raw_frequency - COALESCE(covered_occurrences.covered_by_any_count, 0)
    END AS independent_occurrence_count,
    CASE
        WHEN cand.ngram_size < {DEFAULT_MAX_NGRAM_SIZE}
        THEN COALESCE(dominant_parent.direct_parent_count, 0)
    END AS direct_parent_count,
    CASE
        WHEN cand.ngram_size < {DEFAULT_MAX_NGRAM_SIZE}
        THEN dominant_parent.dominant_parent_key
    END AS dominant_parent_key,
    CASE
        WHEN cand.ngram_size < {DEFAULT_MAX_NGRAM_SIZE}
        THEN dominant_parent.dominant_parent_shared_count
    END AS dominant_parent_shared_count,
    CASE
        WHEN cand.ngram_size < {DEFAULT_MAX_NGRAM_SIZE}
        AND cand.raw_frequency > 0
        AND dominant_parent.dominant_parent_shared_count IS NOT NULL
        THEN 1.0 * dominant_parent.dominant_parent_shared_count / cand.raw_frequency
    END AS dominant_parent_share,
    CASE
        WHEN cand.ngram_size < {DEFAULT_MAX_NGRAM_SIZE}
        THEN dominant_parent.dominant_parent_side
    END AS dominant_parent_side,
    score.score_version,
    score.ranking_lane,
    score.is_eligible,
    score.frequency_score,
    score.dispersion_score,
    score.association_score,
    score.boundary_score,
    score.redundancy_penalty,
    score.final_score,
    score.lane_rank
"""
_SCORE_SUMMARY_CTES_SQL = """
WITH covered_occurrences AS (
    SELECT
        small_occ.candidate_id,
        COUNT(DISTINCT small_occ.occurrence_id) AS covered_by_any_count
    FROM token_occurrences small_occ
    JOIN token_candidates small_cand
        ON small_cand.candidate_id = small_occ.candidate_id
        AND small_cand.inventory_version = small_occ.inventory_version
    JOIN token_occurrences big_occ
        ON big_occ.inventory_version = small_occ.inventory_version
        AND big_occ.sentence_id = small_occ.sentence_id
        AND (
            (
                big_occ.token_start_index = small_occ.token_start_index - 1
                AND big_occ.token_end_index = small_occ.token_end_index
            )
            OR (
                big_occ.token_start_index = small_occ.token_start_index
                AND big_occ.token_end_index = small_occ.token_end_index + 1
            )
        )
    JOIN token_candidates big_cand
        ON big_cand.candidate_id = big_occ.candidate_id
        AND big_cand.inventory_version = big_occ.inventory_version
    WHERE small_occ.inventory_version = ?
    AND big_cand.ngram_size = small_cand.ngram_size + 1
    GROUP BY small_occ.candidate_id
),
ranked_containment AS (
    SELECT
        cc.smaller_candidate_id AS candidate_id,
        larger.candidate_key AS dominant_parent_key,
        cc.shared_occurrence_count AS dominant_parent_shared_count,
        cc.extension_side AS dominant_parent_side,
        COUNT(*) OVER (PARTITION BY cc.smaller_candidate_id) AS direct_parent_count,
        ROW_NUMBER() OVER (
            PARTITION BY cc.smaller_candidate_id
            ORDER BY
                cc.shared_occurrence_count DESC,
                larger.raw_frequency DESC,
                larger.candidate_key
        ) AS parent_rank
    FROM candidate_containment cc
    JOIN token_candidates larger
        ON larger.candidate_id = cc.larger_candidate_id
        AND larger.inventory_version = cc.inventory_version
    WHERE cc.inventory_version = ?
),
dominant_parent AS (
    SELECT
        candidate_id,
        direct_parent_count,
        dominant_parent_key,
        dominant_parent_shared_count,
        dominant_parent_side
    FROM ranked_containment
    WHERE parent_rank = 1
)
"""


class _CandidateScoreSummaryStore:
    def __init__(
        self,
        *,
        connection: Connection,
        inventory_version: str,
        score_version: str,
    ) -> None:
        self.connection = connection
        self.inventory_version = inventory_version
        self.score_version = score_version

    def list_candidates_by_key(
        self,
        candidate_keys: Iterable[str],
    ) -> tuple[CandidateSummaryRow, ...]:
        ordered_keys = _normalize_candidate_keys(candidate_keys)
        if not ordered_keys:
            return ()

        rows = self._list_rows(
            where_sql=f"cand.candidate_key IN ({_sql_placeholders(len(ordered_keys))})",
            parameters=ordered_keys,
        )
        row_by_key = {row.candidate_key: row for row in rows}
        return tuple(
            row_by_key[candidate_key]
            for candidate_key in ordered_keys
            if candidate_key in row_by_key
        )

    def list_top_candidates(
        self,
        *,
        ngram_size: int,
        limit: int,
    ) -> tuple[CandidateSummaryRow, ...]:
        lane_name = _LANE_SPECS[ngram_size].ranking_lane
        return self._list_rows(
            where_sql="score.ranking_lane = ? AND score.is_eligible = 1",
            parameters=(lane_name,),
            order_sql="score.lane_rank ASC",
            limit=limit,
        )

    def _list_rows(
        self,
        *,
        where_sql: str = "",
        parameters: tuple[object, ...] = (),
        order_sql: str = "",
        limit: int | None = None,
    ) -> tuple[CandidateSummaryRow, ...]:
        sql_lines = [
            _SCORE_SUMMARY_CTES_SQL,
            "SELECT",
            _SCORE_SUMMARY_COLUMNS_SQL,
            "FROM candidate_scores score",
            "JOIN token_candidates cand",
            "    ON cand.candidate_id = score.candidate_id",
            "    AND cand.inventory_version = score.inventory_version",
            "LEFT JOIN covered_occurrences",
            "    ON covered_occurrences.candidate_id = cand.candidate_id",
            "LEFT JOIN dominant_parent",
            "    ON dominant_parent.candidate_id = cand.candidate_id",
            "WHERE score.inventory_version = ?",
            "AND score.score_version = ?",
        ]
        query_parameters: list[object] = [
            self.inventory_version,
            self.inventory_version,
            self.inventory_version,
            self.score_version,
            *parameters,
        ]

        if where_sql:
            sql_lines.append(f"AND {where_sql}")

        if order_sql:
            sql_lines.append(f"ORDER BY {order_sql}")

        if limit is not None:
            sql_lines.append("LIMIT ?")
            query_parameters.append(limit)

        rows = self.connection.execute(
            "\n".join(sql_lines),
            tuple(query_parameters),
        ).fetchall()
        return tuple(_row_to_candidate_summary(row) for row in rows)


def _validate_ngram_size(ngram_size: int) -> None:
    if ngram_size not in _LANE_SPECS:
        raise CandidateScoresError(
            f"ngram_size must be one of {', '.join(str(size) for size in sorted(_LANE_SPECS))}"
        )


def _validate_limit(limit: int) -> None:
    if limit < 1:
        raise CandidateScoresError("limit must be positive")


def _normalize_candidate_keys(candidate_keys: Iterable[str]) -> tuple[str, ...]:
    ordered_keys: list[str] = []
    seen_keys: set[str] = set()

    for candidate_key in candidate_keys:
        normalized_key = str(candidate_key).strip()
        if not normalized_key or normalized_key in seen_keys:
            continue
        ordered_keys.append(normalized_key)
        seen_keys.add(normalized_key)

    return tuple(ordered_keys)


def _sql_placeholders(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def _row_to_candidate_summary(row: Row) -> CandidateSummaryRow:
    return CandidateSummaryRow(
        candidate_key=str(row["candidate_key"]),
        display_text=str(row["display_text"]),
        ngram_size=int(row["ngram_size"]),
        raw_frequency=int(row["raw_frequency"]),
        episode_dispersion=int(row["episode_dispersion"]),
        show_dispersion=int(row["show_dispersion"]),
        t_score=_optional_float(row["t_score"]),
        npmi=_optional_float(row["npmi"]),
        left_context_type_count=_optional_int(row["left_context_type_count"]),
        right_context_type_count=_optional_int(row["right_context_type_count"]),
        left_entropy=_optional_float(row["left_entropy"]),
        right_entropy=_optional_float(row["right_entropy"]),
        covered_by_any_count=_optional_int(row["covered_by_any_count"]),
        covered_by_any_ratio=_optional_float(row["covered_by_any_ratio"]),
        independent_occurrence_count=_optional_int(row["independent_occurrence_count"]),
        direct_parent_count=_optional_int(row["direct_parent_count"]),
        dominant_parent_key=_optional_str(row["dominant_parent_key"]),
        dominant_parent_shared_count=_optional_int(row["dominant_parent_shared_count"]),
        dominant_parent_share=_optional_float(row["dominant_parent_share"]),
        dominant_parent_side=_optional_str(row["dominant_parent_side"]),
        score_version=_optional_str(row["score_version"]),
        ranking_lane=_optional_str(row["ranking_lane"]),
        is_eligible=_optional_int(row["is_eligible"]),
        frequency_score=_optional_float(row["frequency_score"]),
        dispersion_score=_optional_float(row["dispersion_score"]),
        association_score=_optional_float(row["association_score"]),
        boundary_score=_optional_float(row["boundary_score"]),
        redundancy_penalty=_optional_float(row["redundancy_penalty"]),
        final_score=_optional_float(row["final_score"]),
        lane_rank=_optional_int(row["lane_rank"]),
    )


def _optional_int(value: object | None) -> int | None:
    return None if value is None else int(value)


def _optional_str(value: object | None) -> str | None:
    return None if value is None else str(value)


def _optional_float(value: object | None) -> float | None:
    return None if value is None else float(value)
