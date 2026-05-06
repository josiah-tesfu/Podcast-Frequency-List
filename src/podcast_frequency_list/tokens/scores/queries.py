from __future__ import annotations

from collections.abc import Iterable
from sqlite3 import Connection, Row

from podcast_frequency_list.tokens.models import CandidateSummaryRow
from podcast_frequency_list.tokens.scores.policy import _LANE_SPECS
from podcast_frequency_list.tokens.spans import DEFAULT_MAX_NGRAM_SIZE

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
