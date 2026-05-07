from __future__ import annotations

from collections.abc import Iterable
from sqlite3 import Connection, Row

from podcast_frequency_list.tokens.models import CandidateSummaryRow
from podcast_frequency_list.tokens.spans import DEFAULT_MAX_NGRAM_SIZE

_SUMMARY_COLUMNS_SQL = f"""
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
    cand.punctuation_gap_occurrence_count,
    cand.punctuation_gap_occurrence_ratio,
    cand.punctuation_gap_edge_clitic_count,
    cand.punctuation_gap_edge_clitic_ratio,
    cand.max_component_information,
    cand.min_component_information,
    cand.high_information_token_count,
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
    END AS dominant_parent_side
"""
_SUMMARY_CTES_SQL = """
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
_SUMMARY_ORDER_SQL = (
    "cand.raw_frequency DESC, cand.episode_dispersion DESC, cand.candidate_key"
)


class _CandidateSummaryStore:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version

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
        offset: int = 0,
    ) -> tuple[CandidateSummaryRow, ...]:
        return self._list_rows(
            where_sql="cand.ngram_size = ?",
            parameters=(ngram_size,),
            order_sql=_SUMMARY_ORDER_SQL,
            limit=limit,
            offset=offset,
        )

    def _list_rows(
        self,
        *,
        where_sql: str = "",
        parameters: tuple[object, ...] = (),
        order_sql: str = "",
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[CandidateSummaryRow, ...]:
        sql_lines = [
            _SUMMARY_CTES_SQL,
            "SELECT",
            _SUMMARY_COLUMNS_SQL,
            "FROM token_candidates cand",
            "LEFT JOIN covered_occurrences",
            "    ON covered_occurrences.candidate_id = cand.candidate_id",
            "LEFT JOIN dominant_parent",
            "    ON dominant_parent.candidate_id = cand.candidate_id",
            "WHERE cand.inventory_version = ?",
        ]
        query_parameters: list[object] = [
            self.inventory_version,
            self.inventory_version,
            self.inventory_version,
            *parameters,
        ]

        if where_sql:
            sql_lines.append(f"AND {where_sql}")

        if order_sql:
            sql_lines.append(f"ORDER BY {order_sql}")

        if limit is not None:
            sql_lines.append("LIMIT ?")
            query_parameters.append(limit)

        if offset > 0:
            if limit is None:
                sql_lines.append("LIMIT -1")
            sql_lines.append("OFFSET ?")
            query_parameters.append(offset)

        rows = self.connection.execute(
            "\n".join(sql_lines),
            tuple(query_parameters),
        ).fetchall()
        return tuple(_row_to_summary(row) for row in rows)


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


def _row_to_summary(row: Row) -> CandidateSummaryRow:
    return CandidateSummaryRow(
        candidate_key=str(row["candidate_key"]),
        display_text=str(row["display_text"]),
        ngram_size=int(row["ngram_size"]),
        raw_frequency=int(row["raw_frequency"]),
        episode_dispersion=int(row["episode_dispersion"]),
        show_dispersion=int(row["show_dispersion"]),
        t_score=_row_float(row, "t_score"),
        npmi=_row_float(row, "npmi"),
        left_context_type_count=_row_int(row, "left_context_type_count"),
        right_context_type_count=_row_int(row, "right_context_type_count"),
        left_entropy=_row_float(row, "left_entropy"),
        right_entropy=_row_float(row, "right_entropy"),
        punctuation_gap_occurrence_count=_row_int(row, "punctuation_gap_occurrence_count"),
        punctuation_gap_occurrence_ratio=_row_float(row, "punctuation_gap_occurrence_ratio"),
        punctuation_gap_edge_clitic_count=_row_int(row, "punctuation_gap_edge_clitic_count"),
        punctuation_gap_edge_clitic_ratio=_row_float(row, "punctuation_gap_edge_clitic_ratio"),
        max_component_information=_row_float(row, "max_component_information"),
        min_component_information=_row_float(row, "min_component_information"),
        high_information_token_count=_row_int(row, "high_information_token_count"),
        covered_by_any_count=_row_int(row, "covered_by_any_count"),
        covered_by_any_ratio=_row_float(row, "covered_by_any_ratio"),
        independent_occurrence_count=_row_int(row, "independent_occurrence_count"),
        direct_parent_count=_row_int(row, "direct_parent_count"),
        dominant_parent_key=_row_text(row, "dominant_parent_key"),
        dominant_parent_shared_count=_row_int(row, "dominant_parent_shared_count"),
        dominant_parent_share=_row_float(row, "dominant_parent_share"),
        dominant_parent_side=_row_text(row, "dominant_parent_side"),
    )


def _sql_placeholders(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def _row_float(row: Row, column_name: str) -> float | None:
    value = row[column_name]
    return None if value is None else float(value)


def _row_int(row: Row, column_name: str) -> int | None:
    value = row[column_name]
    return None if value is None else int(value)


def _row_text(row: Row, column_name: str) -> str | None:
    value = row[column_name]
    return None if value is None else str(value)
