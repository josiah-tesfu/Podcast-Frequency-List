from __future__ import annotations

import math
from dataclasses import dataclass
from sqlite3 import Connection

from podcast_frequency_list.tokens.metrics.store import _replace_temp_table, _temp_table_value_sql
from podcast_frequency_list.tokens.service import TOKENIZATION_VERSION
from podcast_frequency_list.tokens.spans import STANDALONE_CLITIC_JUNK

_IDENTITY_REFRESH_TABLE = "candidate_identity_refresh"
_IDENTITY_COLUMNS = (
    "punctuation_gap_occurrence_count",
    "punctuation_gap_occurrence_ratio",
    "punctuation_gap_edge_clitic_count",
    "punctuation_gap_edge_clitic_ratio",
    "starts_with_standalone_clitic",
    "ends_with_standalone_clitic",
    "max_component_information",
    "min_component_information",
    "high_information_token_count",
)
_HIGH_INFORMATION_PERCENTILE = 0.75
_CLAUSE_GAP_PUNCTUATION = ","


@dataclass(frozen=True)
class _CandidateIdentityRow:
    candidate_id: int
    candidate_key: str
    ngram_size: int
    raw_frequency: int


@dataclass(frozen=True)
class _OccurrenceRow:
    candidate_id: int
    sentence_id: int
    token_start_index: int
    token_end_index: int


@dataclass(frozen=True)
class _SentenceTokenRow:
    token_key: str
    char_start: int
    char_end: int


@dataclass(frozen=True)
class _IdentityRefreshRow:
    candidate_id: int
    punctuation_gap_occurrence_count: int | None
    punctuation_gap_occurrence_ratio: float | None
    punctuation_gap_edge_clitic_count: int | None
    punctuation_gap_edge_clitic_ratio: float | None
    starts_with_standalone_clitic: int | None
    ends_with_standalone_clitic: int | None
    max_component_information: float | None
    min_component_information: float | None
    high_information_token_count: int | None


class _UnitIdentityStore:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version

    def refresh(self) -> None:
        candidate_rows = self._load_candidate_rows()
        sentence_text_by_id = self._load_sentence_text_by_id()
        sentence_tokens_by_id = self._load_sentence_tokens_by_id()
        token_information_by_key = _build_token_information_by_key(sentence_tokens_by_id)
        high_information_threshold = _high_information_threshold(token_information_by_key)
        refresh_rows = tuple(
            self._build_refresh_rows(
                candidate_rows=candidate_rows,
                sentence_text_by_id=sentence_text_by_id,
                sentence_tokens_by_id=sentence_tokens_by_id,
                token_information_by_key=token_information_by_key,
                high_information_threshold=high_information_threshold,
            )
        )

        self._populate_refresh_table(refresh_rows)
        self._clear_one_gram_metrics()
        self._refresh_multiword_metrics()

    def _load_candidate_rows(self) -> tuple[_CandidateIdentityRow, ...]:
        rows = self.connection.execute(
            """
            SELECT candidate_id, candidate_key, ngram_size, raw_frequency
            FROM token_candidates
            WHERE inventory_version = ?
            """,
            (self.inventory_version,),
        ).fetchall()
        return tuple(
            _CandidateIdentityRow(
                candidate_id=int(row["candidate_id"]),
                candidate_key=str(row["candidate_key"]),
                ngram_size=int(row["ngram_size"]),
                raw_frequency=int(row["raw_frequency"]),
            )
            for row in rows
        )

    def _load_sentence_text_by_id(self) -> dict[int, str]:
        rows = self.connection.execute(
            """
            SELECT DISTINCT sent.sentence_id, sent.sentence_text
            FROM token_occurrences occ
            JOIN segment_sentences sent
                ON sent.sentence_id = occ.sentence_id
            WHERE occ.inventory_version = ?
            """,
            (self.inventory_version,),
        ).fetchall()
        return {int(row["sentence_id"]): str(row["sentence_text"]) for row in rows}

    def _load_sentence_tokens_by_id(self) -> dict[int, dict[int, _SentenceTokenRow]]:
        rows = self.connection.execute(
            """
            SELECT
                st.sentence_id,
                st.token_index,
                st.token_key,
                st.char_start,
                st.char_end
            FROM sentence_tokens st
            JOIN (
                SELECT DISTINCT sentence_id
                FROM token_occurrences
                WHERE inventory_version = ?
            ) scoped_sentences
                ON scoped_sentences.sentence_id = st.sentence_id
            WHERE st.tokenization_version = ?
            """,
            (self.inventory_version, TOKENIZATION_VERSION),
        ).fetchall()

        tokens_by_sentence: dict[int, dict[int, _SentenceTokenRow]] = {}
        for row in rows:
            tokens_by_sentence.setdefault(int(row["sentence_id"]), {})[int(row["token_index"])] = (
                _SentenceTokenRow(
                    token_key=str(row["token_key"]),
                    char_start=int(row["char_start"]),
                    char_end=int(row["char_end"]),
                )
            )
        return tokens_by_sentence

    def _build_refresh_rows(
        self,
        *,
        candidate_rows: tuple[_CandidateIdentityRow, ...],
        sentence_text_by_id: dict[int, str],
        sentence_tokens_by_id: dict[int, dict[int, _SentenceTokenRow]],
        token_information_by_key: dict[str, float],
        high_information_threshold: float | None,
    ) -> tuple[_IdentityRefreshRow, ...]:
        multiword_candidates = {
            row.candidate_id: row for row in candidate_rows if row.ngram_size >= 2
        }
        punctuation_gap_counts = {candidate_id: 0 for candidate_id in multiword_candidates}
        punctuation_gap_edge_clitic_counts = {
            candidate_id: 0 for candidate_id in multiword_candidates
        }

        occurrence_rows = self.connection.execute(
            """
            SELECT candidate_id, sentence_id, token_start_index, token_end_index
            FROM token_occurrences
            WHERE inventory_version = ?
            """,
            (self.inventory_version,),
        ).fetchall()
        for row in occurrence_rows:
            occurrence = _OccurrenceRow(
                candidate_id=int(row["candidate_id"]),
                sentence_id=int(row["sentence_id"]),
                token_start_index=int(row["token_start_index"]),
                token_end_index=int(row["token_end_index"]),
            )
            candidate = multiword_candidates.get(occurrence.candidate_id)
            if candidate is None:
                continue

            sentence_text = sentence_text_by_id.get(occurrence.sentence_id)
            sentence_tokens = sentence_tokens_by_id.get(occurrence.sentence_id)
            if sentence_text is None or sentence_tokens is None:
                continue

            gap_flags = _gap_flags_for_occurrence(
                occurrence=occurrence,
                sentence_text=sentence_text,
                sentence_tokens=sentence_tokens,
            )
            if gap_flags[0]:
                punctuation_gap_counts[occurrence.candidate_id] += 1
            if gap_flags[1]:
                punctuation_gap_edge_clitic_counts[occurrence.candidate_id] += 1

        return tuple(
            _build_refresh_row(
                candidate_row=row,
                punctuation_gap_occurrence_count=punctuation_gap_counts.get(row.candidate_id, 0),
                punctuation_gap_edge_clitic_count=punctuation_gap_edge_clitic_counts.get(
                    row.candidate_id, 0
                ),
                token_information_by_key=token_information_by_key,
                high_information_threshold=high_information_threshold,
            )
            for row in multiword_candidates.values()
        )

    def _populate_refresh_table(self, refresh_rows: tuple[_IdentityRefreshRow, ...]) -> None:
        _replace_temp_table(
            self.connection,
            table_name=_IDENTITY_REFRESH_TABLE,
            columns_sql="""
                candidate_id INTEGER PRIMARY KEY,
                punctuation_gap_occurrence_count INTEGER,
                punctuation_gap_occurrence_ratio REAL,
                punctuation_gap_edge_clitic_count INTEGER,
                punctuation_gap_edge_clitic_ratio REAL,
                starts_with_standalone_clitic INTEGER
                    CHECK (
                        starts_with_standalone_clitic IS NULL
                        OR starts_with_standalone_clitic IN (0, 1)
                    ),
                ends_with_standalone_clitic INTEGER
                    CHECK (
                        ends_with_standalone_clitic IS NULL
                        OR ends_with_standalone_clitic IN (0, 1)
                    ),
                max_component_information REAL,
                min_component_information REAL,
                high_information_token_count INTEGER
            """,
        )
        self.connection.executemany(
            f"""
            INSERT INTO {_IDENTITY_REFRESH_TABLE} (
                candidate_id,
                punctuation_gap_occurrence_count,
                punctuation_gap_occurrence_ratio,
                punctuation_gap_edge_clitic_count,
                punctuation_gap_edge_clitic_ratio,
                starts_with_standalone_clitic,
                ends_with_standalone_clitic,
                max_component_information,
                min_component_information,
                high_information_token_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    row.candidate_id,
                    row.punctuation_gap_occurrence_count,
                    row.punctuation_gap_occurrence_ratio,
                    row.punctuation_gap_edge_clitic_count,
                    row.punctuation_gap_edge_clitic_ratio,
                    row.starts_with_standalone_clitic,
                    row.ends_with_standalone_clitic,
                    row.max_component_information,
                    row.min_component_information,
                    row.high_information_token_count,
                )
                for row in refresh_rows
            ),
        )

    def _clear_one_gram_metrics(self) -> None:
        self.connection.execute(
            f"""
            UPDATE token_candidates
            SET {self._clear_assignment_sql()},
                updated_at = CURRENT_TIMESTAMP
            WHERE inventory_version = ?
            AND ngram_size = 1
            AND ({self._nonnull_predicate_sql()})
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
                    FROM {_IDENTITY_REFRESH_TABLE}
                )
                OR {self._nonnull_predicate_sql()}
            )
            """,
            (self.inventory_version,),
        )

    def _assignment_sql(self) -> str:
        return ",\n                ".join(
            f"{column_name} = {_temp_table_value_sql(_IDENTITY_REFRESH_TABLE, column_name)}"
            for column_name in _IDENTITY_COLUMNS
        )

    def _clear_assignment_sql(self) -> str:
        return ",\n                ".join(
            f"{column_name} = NULL" for column_name in _IDENTITY_COLUMNS
        )

    def _nonnull_predicate_sql(self) -> str:
        return " OR ".join(f"{column_name} IS NOT NULL" for column_name in _IDENTITY_COLUMNS)


def _gap_flags_for_occurrence(
    *,
    occurrence: _OccurrenceRow,
    sentence_text: str,
    sentence_tokens: dict[int, _SentenceTokenRow],
) -> tuple[bool, bool]:
    has_punctuation_gap = False
    has_edge_clitic_gap = False

    for token_index in range(occurrence.token_start_index, occurrence.token_end_index - 1):
        left_token = sentence_tokens.get(token_index)
        right_token = sentence_tokens.get(token_index + 1)
        if left_token is None or right_token is None:
            continue

        gap_text = sentence_text[left_token.char_end : right_token.char_start]
        if _CLAUSE_GAP_PUNCTUATION not in gap_text:
            continue

        has_punctuation_gap = True
        if (
            left_token.token_key in STANDALONE_CLITIC_JUNK
            or right_token.token_key in STANDALONE_CLITIC_JUNK
        ):
            has_edge_clitic_gap = True

    return has_punctuation_gap, has_edge_clitic_gap


def _build_token_information_by_key(
    sentence_tokens_by_id: dict[int, dict[int, _SentenceTokenRow]]
) -> dict[str, float]:
    token_frequency_by_key: dict[str, int] = {}
    total_token_count = 0

    for sentence_tokens in sentence_tokens_by_id.values():
        for token in sentence_tokens.values():
            token_frequency_by_key[token.token_key] = (
                token_frequency_by_key.get(token.token_key, 0) + 1
            )
            total_token_count += 1

    if total_token_count <= 0:
        return {}

    return {
        token_key: -math.log(token_frequency / total_token_count)
        for token_key, token_frequency in token_frequency_by_key.items()
        if token_frequency > 0
    }


def _high_information_threshold(token_information_by_key: dict[str, float]) -> float | None:
    if not token_information_by_key:
        return None

    ordered_values = sorted(token_information_by_key.values())
    threshold_index = max(0, math.ceil(_HIGH_INFORMATION_PERCENTILE * len(ordered_values)) - 1)
    return ordered_values[threshold_index]


def _build_refresh_row(
    *,
    candidate_row: _CandidateIdentityRow,
    punctuation_gap_occurrence_count: int,
    punctuation_gap_edge_clitic_count: int,
    token_information_by_key: dict[str, float],
    high_information_threshold: float | None,
) -> _IdentityRefreshRow:
    candidate_tokens = candidate_row.candidate_key.split()
    token_information_values = [
        token_information_by_key.get(token_key) for token_key in candidate_tokens
    ]
    information_values = [value for value in token_information_values if value is not None]

    max_component_information = max(information_values) if information_values else None
    min_component_information = min(information_values) if information_values else None
    high_information_token_count = (
        None
        if high_information_threshold is None or not information_values
        else sum(value >= high_information_threshold for value in information_values)
    )

    punctuation_gap_occurrence_ratio = _ratio(
        numerator=punctuation_gap_occurrence_count,
        denominator=candidate_row.raw_frequency,
    )
    punctuation_gap_edge_clitic_ratio = _ratio(
        numerator=punctuation_gap_edge_clitic_count,
        denominator=candidate_row.raw_frequency,
    )

    return _IdentityRefreshRow(
        candidate_id=candidate_row.candidate_id,
        punctuation_gap_occurrence_count=punctuation_gap_occurrence_count,
        punctuation_gap_occurrence_ratio=punctuation_gap_occurrence_ratio,
        punctuation_gap_edge_clitic_count=punctuation_gap_edge_clitic_count,
        punctuation_gap_edge_clitic_ratio=punctuation_gap_edge_clitic_ratio,
        starts_with_standalone_clitic=int(
            bool(candidate_tokens) and candidate_tokens[0] in STANDALONE_CLITIC_JUNK
        ),
        ends_with_standalone_clitic=int(
            bool(candidate_tokens) and candidate_tokens[-1] in STANDALONE_CLITIC_JUNK
        ),
        max_component_information=max_component_information,
        min_component_information=min_component_information,
        high_information_token_count=high_information_token_count,
    )


def _ratio(*, numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator
