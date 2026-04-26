from __future__ import annotations

import math
from sqlite3 import Connection

from podcast_frequency_list.tokens.service import TOKENIZATION_VERSION

_BOUNDARY_REFRESH_TABLE = "candidate_boundary_refresh"
_BOUNDARY_COLUMNS = (
    "left_context_type_count",
    "right_context_type_count",
    "left_entropy",
    "right_entropy",
)


class _BoundaryStore:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version

    def refresh(self) -> None:
        self._populate_refresh_table()
        self._clear_one_gram_metrics()
        self._refresh_multiword_metrics()

    def _populate_refresh_table(self) -> None:
        _replace_temp_table(
            self.connection,
            table_name=_BOUNDARY_REFRESH_TABLE,
            columns_sql="""
                candidate_id INTEGER PRIMARY KEY,
                left_context_type_count INTEGER,
                right_context_type_count INTEGER,
                left_entropy REAL,
                right_entropy REAL
            """,
        )
        self.connection.create_function("natural_log", 1, _natural_log)
        self.connection.execute(
            f"""
            INSERT INTO {_BOUNDARY_REFRESH_TABLE} (
                candidate_id,
                left_context_type_count,
                right_context_type_count,
                left_entropy,
                right_entropy
            )
            WITH sentence_bounds AS (
                SELECT
                    sentence_id,
                    MIN(token_index) AS min_token_index,
                    MAX(token_index) AS max_token_index
                FROM sentence_tokens
                WHERE tokenization_version = ?
                GROUP BY sentence_id
            ),
            context_rows AS (
                SELECT
                    occ.candidate_id,
                    'left' AS side,
                    COALESCE(prev.token_key, '__BOS__') AS context_key,
                    COUNT(*) AS context_count
                FROM token_occurrences occ
                JOIN token_candidates cand
                    ON cand.candidate_id = occ.candidate_id
                    AND cand.inventory_version = occ.inventory_version
                JOIN sentence_bounds bounds
                    ON bounds.sentence_id = occ.sentence_id
                LEFT JOIN sentence_tokens prev
                    ON prev.sentence_id = occ.sentence_id
                    AND prev.tokenization_version = ?
                    AND prev.token_index = occ.token_start_index - 1
                WHERE occ.inventory_version = ?
                AND cand.ngram_size >= 2
                AND (
                    prev.token_key IS NOT NULL
                    OR occ.token_start_index = bounds.min_token_index
                )
                GROUP BY occ.candidate_id, context_key

                UNION ALL

                SELECT
                    occ.candidate_id,
                    'right' AS side,
                    COALESCE(next.token_key, '__EOS__') AS context_key,
                    COUNT(*) AS context_count
                FROM token_occurrences occ
                JOIN token_candidates cand
                    ON cand.candidate_id = occ.candidate_id
                    AND cand.inventory_version = occ.inventory_version
                JOIN sentence_bounds bounds
                    ON bounds.sentence_id = occ.sentence_id
                LEFT JOIN sentence_tokens next
                    ON next.sentence_id = occ.sentence_id
                    AND next.tokenization_version = ?
                    AND next.token_index = occ.token_end_index
                WHERE occ.inventory_version = ?
                AND cand.ngram_size >= 2
                AND (
                    next.token_key IS NOT NULL
                    OR occ.token_end_index = bounds.max_token_index + 1
                )
                GROUP BY occ.candidate_id, context_key
            ),
            context_stats AS (
                SELECT
                    candidate_id,
                    side,
                    COUNT(*) AS context_type_count,
                    -SUM(probability * natural_log(probability)) AS entropy
                FROM (
                    SELECT
                        candidate_id,
                        side,
                        context_count,
                        1.0 * context_count
                            / SUM(context_count) OVER (PARTITION BY candidate_id, side)
                            AS probability
                    FROM context_rows
                )
                GROUP BY candidate_id, side
            )
            SELECT
                candidate_id,
                MAX(CASE WHEN side = 'left' THEN context_type_count END),
                MAX(CASE WHEN side = 'right' THEN context_type_count END),
                MAX(CASE WHEN side = 'left' THEN entropy END),
                MAX(CASE WHEN side = 'right' THEN entropy END)
            FROM context_stats
            GROUP BY candidate_id
            """,
            (
                TOKENIZATION_VERSION,
                TOKENIZATION_VERSION,
                self.inventory_version,
                TOKENIZATION_VERSION,
                self.inventory_version,
            ),
        )

    def _clear_one_gram_metrics(self) -> None:
        self.connection.execute(
            """
            UPDATE token_candidates
            SET left_context_type_count = NULL,
                right_context_type_count = NULL,
                left_entropy = NULL,
                right_entropy = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE inventory_version = ?
            AND ngram_size = 1
            AND (
                left_context_type_count IS NOT NULL
                OR right_context_type_count IS NOT NULL
                OR left_entropy IS NOT NULL
                OR right_entropy IS NOT NULL
            )
            """,
            (self.inventory_version,),
        )

    def _refresh_multiword_metrics(self) -> None:
        self.connection.execute(
            f"""
            UPDATE token_candidates
            SET {self._assignment_sql()},
                updated_at = CURRENT_TIMESTAMP
            WHERE inventory_version = ?
            AND ngram_size >= 2
            AND (
                candidate_id IN (
                    SELECT candidate_id
                    FROM {_BOUNDARY_REFRESH_TABLE}
                )
                OR left_context_type_count IS NOT NULL
                OR right_context_type_count IS NOT NULL
                OR left_entropy IS NOT NULL
                OR right_entropy IS NOT NULL
            )
            """,
            (self.inventory_version,),
        )

    def _assignment_sql(self) -> str:
        return ",\n                ".join(
            f"{column_name} = {_temp_table_value_sql(column_name)}"
            for column_name in _BOUNDARY_COLUMNS
        )


def _replace_temp_table(
    connection: Connection,
    *,
    table_name: str,
    columns_sql: str,
) -> None:
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.execute(
        f"""
        CREATE TEMP TABLE {table_name} (
            {columns_sql}
        )
        """
    )


def _temp_table_value_sql(column_name: str) -> str:
    return (
        "(\n"
        f"                    SELECT temp_rows.{column_name}\n"
        f"                    FROM {_BOUNDARY_REFRESH_TABLE} temp_rows\n"
        "                    WHERE temp_rows.candidate_id = token_candidates.candidate_id\n"
        "                )"
    )


def _natural_log(value: float) -> float:
    return math.log(float(value))
