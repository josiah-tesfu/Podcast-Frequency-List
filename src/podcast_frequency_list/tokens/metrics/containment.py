from __future__ import annotations

from sqlite3 import Connection

_CONTAINMENT_REFRESH_TABLE = "candidate_containment_refresh"


class _ContainmentStore:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version

    def refresh(self) -> None:
        self._populate_refresh_table()
        self._replace_inventory_rows()

    def _populate_refresh_table(self) -> None:
        self.connection.execute(f"DROP TABLE IF EXISTS {_CONTAINMENT_REFRESH_TABLE}")
        self.connection.execute(
            f"""
            CREATE TEMP TABLE {_CONTAINMENT_REFRESH_TABLE} (
                smaller_candidate_id INTEGER NOT NULL,
                larger_candidate_id INTEGER NOT NULL,
                extension_side TEXT NOT NULL,
                shared_occurrence_count INTEGER NOT NULL,
                shared_episode_count INTEGER NOT NULL,
                PRIMARY KEY (smaller_candidate_id, larger_candidate_id)
            )
            """
        )
        self.connection.execute(
            f"""
            WITH pair_matches AS (
                SELECT
                    small_occ.candidate_id AS smaller_candidate_id,
                    big_occ.candidate_id AS larger_candidate_id,
                    CASE
                        WHEN big_occ.token_start_index = small_occ.token_start_index - 1
                            AND big_occ.token_end_index = small_occ.token_end_index
                        THEN 'left'
                        ELSE 'right'
                    END AS extension_side,
                    small_occ.occurrence_id,
                    small_occ.episode_id
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
            )
            INSERT INTO {_CONTAINMENT_REFRESH_TABLE} (
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            )
            SELECT
                smaller_candidate_id,
                larger_candidate_id,
                CASE
                    WHEN COUNT(DISTINCT extension_side) = 2 THEN 'both'
                    ELSE MIN(extension_side)
                END AS extension_side,
                COUNT(DISTINCT occurrence_id) AS shared_occurrence_count,
                COUNT(DISTINCT episode_id) AS shared_episode_count
            FROM pair_matches
            GROUP BY
                smaller_candidate_id,
                larger_candidate_id
            """,
            (self.inventory_version,),
        )

    def _replace_inventory_rows(self) -> None:
        self.connection.execute(
            """
            DELETE FROM candidate_containment
            WHERE inventory_version = ?
            """,
            (self.inventory_version,),
        )
        self.connection.execute(
            f"""
            INSERT INTO candidate_containment (
                inventory_version,
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            )
            SELECT
                ?,
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            FROM {_CONTAINMENT_REFRESH_TABLE}
            """,
            (self.inventory_version,),
        )
