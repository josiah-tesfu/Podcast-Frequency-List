from __future__ import annotations

from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from sqlite3 import Connection, Row

from podcast_frequency_list.db import connect
from podcast_frequency_list.sentences.service import SPLIT_VERSION
from podcast_frequency_list.tokens.models import CandidateInventoryResult, SentenceToken
from podcast_frequency_list.tokens.service import TOKENIZATION_VERSION
from podcast_frequency_list.tokens.spans import generate_sentence_spans
from podcast_frequency_list.transcript_scope import TranscriptScope, resolve_transcript_scope

INVENTORY_VERSION = "1"


class CandidateInventoryError(RuntimeError):
    pass


@dataclass(frozen=True)
class _ScopeSql:
    join_sql: str
    where_sql: str
    order_sql: str
    parameters: tuple[object, ...]


@dataclass(frozen=True)
class _InventoryTarget:
    sentence_id: int
    episode_id: int
    segment_id: int
    sentence_text: str
    tokens: tuple[SentenceToken, ...]
    existing_occurrence_count: int


class CandidateInventoryService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def generate(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> CandidateInventoryResult:
        scope = resolve_transcript_scope(
            pilot_name=pilot_name,
            episode_id=episode_id,
            error_type=CandidateInventoryError,
        )
        targets = self._load_targets(scope)
        if not targets:
            raise CandidateInventoryError("no tokenized sentences found for candidate generation")

        created_candidates = 0
        created_occurrences = 0
        processed_sentences = 0
        skipped_sentences = 0
        candidate_cache: dict[str, int] = {}

        with connect(self.db_path) as connection:
            if force:
                self._delete_scope_occurrences(connection, scope)

            for target in targets:
                if target.existing_occurrence_count > 0 and not force:
                    skipped_sentences += 1
                    continue

                candidate_count, occurrence_count = self._persist_target(
                    connection,
                    target=target,
                    candidate_cache=candidate_cache,
                )
                created_candidates += candidate_count
                created_occurrences += occurrence_count
                processed_sentences += 1

            if force or processed_sentences:
                self._refresh_inventory(connection)

            connection.commit()

        return CandidateInventoryResult(
            scope=scope.kind,
            scope_value=scope.scope_value,
            inventory_version=INVENTORY_VERSION,
            selected_sentences=len(targets),
            processed_sentences=processed_sentences,
            skipped_sentences=skipped_sentences,
            created_candidates=created_candidates,
            created_occurrences=created_occurrences,
            episode_count=len({target.episode_id for target in targets}),
        )

    def _load_targets(self, scope: TranscriptScope) -> list[_InventoryTarget]:
        scoped_sql = _build_scope_sql(scope)

        with connect(self.db_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    ss.sentence_id,
                    ss.episode_id,
                    ss.segment_id,
                    ss.sentence_text,
                    COALESCE(occ.occurrence_count, 0) AS existing_occurrence_count,
                    st.token_index,
                    st.token_key,
                    st.surface_text,
                    st.char_start,
                    st.char_end,
                    st.token_type
                FROM segment_sentences ss
                {scoped_sql.join_sql}
                JOIN sentence_tokens st
                    ON st.sentence_id = ss.sentence_id
                    AND st.tokenization_version = ?
                LEFT JOIN (
                    SELECT sentence_id, COUNT(*) AS occurrence_count
                    FROM token_occurrences
                    WHERE inventory_version = ?
                    GROUP BY sentence_id
                ) occ
                    ON occ.sentence_id = ss.sentence_id
                WHERE {scoped_sql.where_sql}
                AND ss.split_version = ?
                ORDER BY {scoped_sql.order_sql}
                """,
                (TOKENIZATION_VERSION, INVENTORY_VERSION, *scoped_sql.parameters, SPLIT_VERSION),
            ).fetchall()

        return [
            _InventoryTarget(
                sentence_id=int(first_row["sentence_id"]),
                episode_id=int(first_row["episode_id"]),
                segment_id=int(first_row["segment_id"]),
                sentence_text=str(first_row["sentence_text"]),
                tokens=tuple(_row_to_token(row) for row in sentence_rows),
                existing_occurrence_count=int(first_row["existing_occurrence_count"]),
            )
            for _, sentence_rows_iter in groupby(rows, key=lambda row: row["sentence_id"])
            for sentence_rows in [list(sentence_rows_iter)]
            for first_row in [sentence_rows[0]]
        ]

    def _delete_scope_occurrences(self, connection: Connection, scope: TranscriptScope) -> None:
        scoped_sql = _build_scope_sql(scope)
        connection.execute(
            f"""
            DELETE FROM token_occurrences
            WHERE inventory_version = ?
            AND sentence_id IN (
                SELECT ss.sentence_id
                FROM segment_sentences ss
                {scoped_sql.join_sql}
                JOIN sentence_tokens st
                    ON st.sentence_id = ss.sentence_id
                    AND st.tokenization_version = ?
                WHERE {scoped_sql.where_sql}
                AND ss.split_version = ?
            )
            """,
            (INVENTORY_VERSION, TOKENIZATION_VERSION, *scoped_sql.parameters, SPLIT_VERSION),
        )

    def _persist_target(
        self,
        connection: Connection,
        *,
        target: _InventoryTarget,
        candidate_cache: dict[str, int],
    ) -> tuple[int, int]:
        created_candidates = 0
        created_occurrences = 0

        for span in generate_sentence_spans(
            sentence_id=target.sentence_id,
            episode_id=target.episode_id,
            segment_id=target.segment_id,
            sentence_text=target.sentence_text,
            tokens=target.tokens,
        ):
            candidate_id, was_created = self._get_candidate_id(
                connection,
                candidate_cache=candidate_cache,
                candidate_key=span.candidate_key,
                display_text=span.display_text,
                ngram_size=span.ngram_size,
            )
            created_candidates += int(was_created)

            connection.execute(
                """
                INSERT INTO token_occurrences (
                    candidate_id,
                    sentence_id,
                    episode_id,
                    segment_id,
                    inventory_version,
                    token_start_index,
                    token_end_index,
                    char_start,
                    char_end,
                    surface_text
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate_id,
                    span.sentence_id,
                    span.episode_id,
                    span.segment_id,
                    INVENTORY_VERSION,
                    span.token_start_index,
                    span.token_end_index,
                    span.char_start,
                    span.char_end,
                    span.surface_text,
                ),
            )
            created_occurrences += 1

        return created_candidates, created_occurrences

    def _get_candidate_id(
        self,
        connection: Connection,
        *,
        candidate_cache: dict[str, int],
        candidate_key: str,
        display_text: str,
        ngram_size: int,
    ) -> tuple[int, bool]:
        cached_id = candidate_cache.get(candidate_key)
        if cached_id is not None:
            return cached_id, False

        cursor = connection.execute(
            """
            INSERT INTO token_candidates (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency
            )
            VALUES (?, ?, ?, ?, 0)
            ON CONFLICT(inventory_version, candidate_key) DO NOTHING
            """,
            (INVENTORY_VERSION, candidate_key, display_text, ngram_size),
        )
        row = connection.execute(
            """
            SELECT candidate_id
            FROM token_candidates
            WHERE inventory_version = ?
            AND candidate_key = ?
            """,
            (INVENTORY_VERSION, candidate_key),
        ).fetchone()
        if row is None:
            raise CandidateInventoryError(f"failed to resolve candidate_id for {candidate_key!r}")

        candidate_id = int(row["candidate_id"])
        candidate_cache[candidate_key] = candidate_id
        return candidate_id, cursor.rowcount == 1

    def _refresh_inventory(self, connection: Connection) -> None:
        connection.execute(
            """
            UPDATE token_candidates
            SET raw_frequency = (
                SELECT COUNT(*)
                FROM token_occurrences occ
                WHERE occ.candidate_id = token_candidates.candidate_id
                AND occ.inventory_version = token_candidates.inventory_version
            )
            WHERE inventory_version = ?
            """,
            (INVENTORY_VERSION,),
        )
        connection.execute(
            """
            DELETE FROM token_candidates
            WHERE inventory_version = ?
            AND NOT EXISTS (
                SELECT 1
                FROM token_occurrences occ
                WHERE occ.candidate_id = token_candidates.candidate_id
                AND occ.inventory_version = token_candidates.inventory_version
            )
            """,
            (INVENTORY_VERSION,),
        )


def _build_scope_sql(scope: TranscriptScope) -> _ScopeSql:
    if scope.kind == "pilot":
        return _ScopeSql(
            join_sql="""
            JOIN pilot_run_episodes pre
                ON pre.episode_id = ss.episode_id
            JOIN pilot_runs pr
                ON pr.pilot_run_id = pre.pilot_run_id
            """,
            where_sql="pr.name = ?",
            order_sql="pre.position, ss.segment_id, ss.sentence_index, st.token_index",
            parameters=(scope.pilot_name,),
        )

    return _ScopeSql(
        join_sql="",
        where_sql="ss.episode_id = ?",
        order_sql="ss.segment_id, ss.sentence_index, st.token_index",
        parameters=(scope.episode_id,),
    )


def _row_to_token(row: Row) -> SentenceToken:
    return SentenceToken(
        token_index=int(row["token_index"]),
        token_key=str(row["token_key"]),
        surface_text=str(row["surface_text"]),
        char_start=int(row["char_start"]),
        char_end=int(row["char_end"]),
        token_type=str(row["token_type"]),
    )
