from __future__ import annotations

import math
from dataclasses import dataclass
from sqlite3 import Connection

from podcast_frequency_list.tokens.metrics.store import _replace_temp_table, _temp_table_value_sql

_SPECIFICITY_REFRESH_TABLE = "candidate_specificity_refresh"
_SPECIFICITY_COLUMNS = (
    "max_show_share",
    "top2_show_share",
    "show_entropy",
)


@dataclass(frozen=True)
class _CandidateShowCountRow:
    candidate_id: int
    show_occurrence_count: int


@dataclass(frozen=True)
class _SpecificityRefreshRow:
    candidate_id: int
    max_show_share: float | None
    top2_show_share: float | None
    show_entropy: float | None


class _ShowSpecificityStore:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version

    def refresh(self) -> None:
        total_show_count = self._count_active_shows()
        show_count_rows = self._load_candidate_show_count_rows()
        refresh_rows = tuple(
            _build_refresh_rows(
                show_count_rows=show_count_rows,
                total_show_count=total_show_count,
            )
        )
        self._populate_refresh_table(refresh_rows)
        self._refresh_metrics()

    def _count_active_shows(self) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(DISTINCT e.show_id)
            FROM token_occurrences occ
            JOIN episodes e
                ON e.episode_id = occ.episode_id
            WHERE occ.inventory_version = ?
            """,
            (self.inventory_version,),
        ).fetchone()
        return int(row[0] or 0)

    def _load_candidate_show_count_rows(self) -> tuple[_CandidateShowCountRow, ...]:
        rows = self.connection.execute(
            """
            SELECT
                occ.candidate_id,
                COUNT(*) AS show_occurrence_count
            FROM token_occurrences occ
            JOIN episodes e
                ON e.episode_id = occ.episode_id
            WHERE occ.inventory_version = ?
            GROUP BY occ.candidate_id, e.show_id
            ORDER BY occ.candidate_id, show_occurrence_count DESC, e.show_id
            """,
            (self.inventory_version,),
        ).fetchall()
        return tuple(
            _CandidateShowCountRow(
                candidate_id=int(row["candidate_id"]),
                show_occurrence_count=int(row["show_occurrence_count"]),
            )
            for row in rows
        )

    def _populate_refresh_table(
        self,
        refresh_rows: tuple[_SpecificityRefreshRow, ...],
    ) -> None:
        _replace_temp_table(
            self.connection,
            table_name=_SPECIFICITY_REFRESH_TABLE,
            columns_sql="""
                candidate_id INTEGER PRIMARY KEY,
                max_show_share REAL,
                top2_show_share REAL,
                show_entropy REAL
            """,
        )
        self.connection.executemany(
            f"""
            INSERT INTO {_SPECIFICITY_REFRESH_TABLE} (
                candidate_id,
                max_show_share,
                top2_show_share,
                show_entropy
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                (
                    row.candidate_id,
                    row.max_show_share,
                    row.top2_show_share,
                    row.show_entropy,
                )
                for row in refresh_rows
            ),
        )

    def _refresh_metrics(self) -> None:
        self.connection.execute(
            f"""
            UPDATE token_candidates
            SET {self._assignment_sql()},
                updated_at = CURRENT_TIMESTAMP
            WHERE inventory_version = ?
            AND (
                candidate_id IN (
                    SELECT candidate_id
                    FROM {_SPECIFICITY_REFRESH_TABLE}
                )
                OR {self._nonnull_predicate_sql()}
            )
            """,
            (self.inventory_version,),
        )

    def _assignment_sql(self) -> str:
        return ",\n                ".join(
            f"{column_name} = {_temp_table_value_sql(_SPECIFICITY_REFRESH_TABLE, column_name)}"
            for column_name in _SPECIFICITY_COLUMNS
        )

    def _nonnull_predicate_sql(self) -> str:
        return " OR ".join(f"{column_name} IS NOT NULL" for column_name in _SPECIFICITY_COLUMNS)


def _build_refresh_rows(
    *,
    show_count_rows: tuple[_CandidateShowCountRow, ...],
    total_show_count: int,
) -> tuple[_SpecificityRefreshRow, ...]:
    refresh_rows: list[_SpecificityRefreshRow] = []
    current_candidate_id: int | None = None
    current_counts: list[int] = []

    def flush_current() -> None:
        if current_candidate_id is None:
            return
        refresh_rows.append(
            _build_refresh_row(
                candidate_id=current_candidate_id,
                show_occurrence_counts=tuple(current_counts),
                total_show_count=total_show_count,
            )
        )

    for row in show_count_rows:
        if row.candidate_id != current_candidate_id:
            flush_current()
            current_candidate_id = row.candidate_id
            current_counts = [row.show_occurrence_count]
            continue
        current_counts.append(row.show_occurrence_count)

    flush_current()
    return tuple(refresh_rows)


def _build_refresh_row(
    *,
    candidate_id: int,
    show_occurrence_counts: tuple[int, ...],
    total_show_count: int,
) -> _SpecificityRefreshRow:
    total_occurrence_count = sum(show_occurrence_counts)
    if total_occurrence_count <= 0 or not show_occurrence_counts:
        return _SpecificityRefreshRow(
            candidate_id=candidate_id,
            max_show_share=None,
            top2_show_share=None,
            show_entropy=None,
        )

    max_show_share = show_occurrence_counts[0] / total_occurrence_count
    top2_show_share = sum(show_occurrence_counts[:2]) / total_occurrence_count
    show_entropy = _normalized_entropy(
        show_occurrence_counts=show_occurrence_counts,
        total_occurrence_count=total_occurrence_count,
        total_show_count=total_show_count,
    )

    return _SpecificityRefreshRow(
        candidate_id=candidate_id,
        max_show_share=max_show_share,
        top2_show_share=top2_show_share,
        show_entropy=show_entropy,
    )


def _normalized_entropy(
    *,
    show_occurrence_counts: tuple[int, ...],
    total_occurrence_count: int,
    total_show_count: int,
) -> float | None:
    if total_occurrence_count <= 0 or total_show_count <= 1:
        return None

    entropy = -sum(
        probability * math.log(probability)
        for probability in (
            show_occurrence_count / total_occurrence_count
            for show_occurrence_count in show_occurrence_counts
            if show_occurrence_count > 0
        )
    )
    normalized_entropy = entropy / math.log(total_show_count)
    return max(0.0, min(1.0, normalized_entropy))
