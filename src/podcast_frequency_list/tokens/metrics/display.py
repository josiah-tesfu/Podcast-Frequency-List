from __future__ import annotations

from sqlite3 import Connection

_DISPLAY_REFRESH_TABLE = "candidate_display_refresh"


class _DisplayStore:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version

    def refresh(self) -> int:
        self._populate_refresh_table()
        if _fetch_int(self.connection, f"SELECT COUNT(*) FROM {_DISPLAY_REFRESH_TABLE}") == 0:
            return 0

        updated_count = self._count_updates()
        self.connection.execute(
            f"""
            UPDATE token_candidates
            SET display_text = {_temp_table_value_sql("display_text")},
                updated_at = CURRENT_TIMESTAMP
            WHERE inventory_version = ?
            AND candidate_id IN (
                SELECT candidate_id
                FROM {_DISPLAY_REFRESH_TABLE}
            )
            AND display_text != {_temp_table_value_sql("display_text")}
            """,
            (self.inventory_version,),
        )
        return updated_count

    def count_mismatches(self) -> int:
        self._populate_refresh_table()
        return _fetch_int(
            self.connection,
            f"""
            SELECT COUNT(*)
            FROM token_candidates cand
            LEFT JOIN {_DISPLAY_REFRESH_TABLE} display
                ON display.candidate_id = cand.candidate_id
            WHERE cand.inventory_version = ?
            AND (
                display.display_text IS NULL
                OR cand.display_text != display.display_text
            )
            """,
            (self.inventory_version,),
        )

    def _populate_refresh_table(self) -> None:
        _replace_temp_table(
            self.connection,
            table_name=_DISPLAY_REFRESH_TABLE,
            columns_sql="""
                candidate_id INTEGER PRIMARY KEY,
                display_text TEXT NOT NULL
            """,
        )
        self.connection.create_function("leading_case_priority", 1, _leading_case_priority)
        self.connection.create_function("casefold_text", 1, _casefold_text)
        self.connection.execute(
            f"""
            INSERT INTO {_DISPLAY_REFRESH_TABLE} (candidate_id, display_text)
            SELECT candidate_id, surface_text
            FROM (
                SELECT
                    candidate_id,
                    surface_text,
                    ROW_NUMBER() OVER (
                        PARTITION BY candidate_id
                        ORDER BY
                            surface_count DESC,
                            leading_case_priority(surface_text),
                            first_occurrence_id,
                            casefold_text(surface_text),
                            surface_text
                    ) AS display_rank
                FROM (
                    SELECT
                        candidate_id,
                        surface_text,
                        COUNT(*) AS surface_count,
                        MIN(occurrence_id) AS first_occurrence_id
                    FROM token_occurrences
                    WHERE inventory_version = ?
                    GROUP BY candidate_id, surface_text
                )
            )
            WHERE display_rank = 1
            """,
            (self.inventory_version,),
        )

    def _count_updates(self) -> int:
        return _fetch_int(
            self.connection,
            f"""
            SELECT COUNT(*)
            FROM token_candidates cand
            JOIN {_DISPLAY_REFRESH_TABLE} display
                ON display.candidate_id = cand.candidate_id
            WHERE cand.inventory_version = ?
            AND cand.display_text != display.display_text
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


def _temp_table_value_sql(column_name: str) -> str:
    return (
        "(\n"
        f"                    SELECT temp_rows.{column_name}\n"
        f"                    FROM {_DISPLAY_REFRESH_TABLE} temp_rows\n"
        "                    WHERE temp_rows.candidate_id = token_candidates.candidate_id\n"
        "                )"
    )


def _casefold_text(text: str) -> str:
    return str(text).casefold()


def _leading_case_priority(text: str) -> int:
    first_character = str(text)[:1]
    if first_character.isalpha() and first_character == first_character.casefold():
        return 0
    return 1
