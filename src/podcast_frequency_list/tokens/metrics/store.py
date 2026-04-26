from __future__ import annotations

from sqlite3 import Connection

_METRIC_REFRESH_TABLE = "candidate_metric_refresh"
_METRIC_COLUMNS = (
    "raw_frequency",
    "episode_dispersion",
    "show_dispersion",
)


class _MetricsStore:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version

    def count_candidates(self) -> int:
        return _fetch_int(
            self.connection,
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE inventory_version = ?
            """,
            (self.inventory_version,),
        )

    def count_mismatches(self) -> dict[str, int]:
        self._populate_metric_refresh_table()
        return {
            column_name: self._count_metric_mismatches(column_name)
            for column_name in _METRIC_COLUMNS
        }

    def refresh(self) -> None:
        self._populate_metric_refresh_table()
        self.connection.execute(
            f"""
            UPDATE token_candidates
            SET {self._metric_assignment_sql()},
                updated_at = CURRENT_TIMESTAMP
            WHERE inventory_version = ?
            AND candidate_id IN (
                SELECT candidate_id
                FROM {_METRIC_REFRESH_TABLE}
            )
            """,
            (self.inventory_version,),
        )

    def load_summary(self) -> dict[str, int]:
        row = self.connection.execute(
            """
            SELECT
                COUNT(*) AS candidate_count,
                COALESCE(SUM(raw_frequency), 0) AS raw_frequency_total,
                COALESCE(SUM(episode_dispersion), 0) AS episode_dispersion_total,
                COALESCE(SUM(show_dispersion), 0) AS show_dispersion_total,
                (
                    SELECT COUNT(*)
                    FROM token_occurrences occ
                    WHERE occ.inventory_version = ?
                ) AS occurrence_count
            FROM token_candidates
            WHERE inventory_version = ?
            """,
            (self.inventory_version, self.inventory_version),
        ).fetchone()
        return {
            "candidate_count": int(row["candidate_count"]),
            "raw_frequency_total": int(row["raw_frequency_total"] or 0),
            "episode_dispersion_total": int(row["episode_dispersion_total"] or 0),
            "show_dispersion_total": int(row["show_dispersion_total"] or 0),
            "occurrence_count": int(row["occurrence_count"] or 0),
        }

    def _populate_metric_refresh_table(self) -> None:
        _replace_temp_table(
            self.connection,
            table_name=_METRIC_REFRESH_TABLE,
            columns_sql="""
                candidate_id INTEGER PRIMARY KEY,
                raw_frequency INTEGER NOT NULL,
                episode_dispersion INTEGER NOT NULL,
                show_dispersion INTEGER NOT NULL
            """,
        )
        self.connection.execute(
            f"""
            INSERT INTO {_METRIC_REFRESH_TABLE} (
                candidate_id,
                raw_frequency,
                episode_dispersion,
                show_dispersion
            )
            SELECT
                occ.candidate_id,
                COUNT(*) AS raw_frequency,
                COUNT(DISTINCT occ.episode_id) AS episode_dispersion,
                COUNT(DISTINCT e.show_id) AS show_dispersion
            FROM token_occurrences occ
            JOIN episodes e
                ON e.episode_id = occ.episode_id
            WHERE occ.inventory_version = ?
            GROUP BY occ.candidate_id
            """,
            (self.inventory_version,),
        )

    def _metric_assignment_sql(self) -> str:
        return ",\n                ".join(
            f"{column_name} = {_temp_table_value_sql(_METRIC_REFRESH_TABLE, column_name)}"
            for column_name in _METRIC_COLUMNS
        )

    def _count_metric_mismatches(self, candidate_column: str) -> int:
        return _fetch_int(
            self.connection,
            f"""
            SELECT COUNT(*)
            FROM token_candidates cand
            LEFT JOIN {_METRIC_REFRESH_TABLE} metric_rows
                ON metric_rows.candidate_id = cand.candidate_id
            WHERE cand.inventory_version = ?
            AND cand.{candidate_column} != COALESCE(metric_rows.{candidate_column}, 0)
            """,
            (self.inventory_version,),
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


def _fetch_int(
    connection: Connection,
    sql: str,
    parameters: tuple[object, ...] = (),
) -> int:
    return int(connection.execute(sql, parameters).fetchone()[0])


def _temp_table_value_sql(table_name: str, column_name: str) -> str:
    return (
        "(\n"
        f"                    SELECT temp_rows.{column_name}\n"
        f"                    FROM {table_name} temp_rows\n"
        "                    WHERE temp_rows.candidate_id = token_candidates.candidate_id\n"
        "                )"
    )
