from __future__ import annotations

from collections.abc import Iterable
from sqlite3 import Connection, Row

from podcast_frequency_list.tokens.models import CandidateSummaryRow

_SUMMARY_COLUMNS_SQL = """
    candidate_key,
    display_text,
    ngram_size,
    raw_frequency,
    episode_dispersion,
    show_dispersion,
    t_score,
    npmi,
    left_context_type_count,
    right_context_type_count,
    left_entropy,
    right_entropy
"""
_SUMMARY_ORDER_SQL = "raw_frequency DESC, episode_dispersion DESC, candidate_key"


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
            where_sql=f"candidate_key IN ({_sql_placeholders(len(ordered_keys))})",
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
        return self._list_rows(
            where_sql="ngram_size = ?",
            parameters=(ngram_size,),
            order_sql=_SUMMARY_ORDER_SQL,
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
            "SELECT",
            _SUMMARY_COLUMNS_SQL,
            "FROM token_candidates",
            "WHERE inventory_version = ?",
        ]
        query_parameters: list[object] = [self.inventory_version, *parameters]

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
    )


def _sql_placeholders(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def _row_float(row: Row, column_name: str) -> float | None:
    value = row[column_name]
    return None if value is None else float(value)


def _row_int(row: Row, column_name: str) -> int | None:
    value = row[column_name]
    return None if value is None else int(value)
