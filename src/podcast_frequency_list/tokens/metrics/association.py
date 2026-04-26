from __future__ import annotations

import math
from dataclasses import dataclass
from sqlite3 import Connection

_SUPPORTED_NGRAM_SIZES = frozenset({2, 3})


@dataclass(frozen=True)
class _CandidateFrequencyRow:
    candidate_id: int
    candidate_key: str
    ngram_size: int
    raw_frequency: int


@dataclass(frozen=True)
class _AssociationRefreshRow:
    candidate_id: int
    t_score: float | None
    npmi: float | None


class _AssociationStore:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version

    def refresh(self) -> None:
        candidate_rows = self._load_candidate_rows()
        unigram_total = sum(
            row.raw_frequency for row in candidate_rows if row.ngram_size == 1
        )
        refresh_rows = tuple(
            self._build_refresh_rows(
                candidate_rows=candidate_rows,
                unigram_total=unigram_total,
            )
        )

        self._clear_unsupported_metrics()
        self.connection.executemany(
            """
            UPDATE token_candidates
            SET t_score = ?,
                npmi = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE candidate_id = ?
            """,
            ((row.t_score, row.npmi, row.candidate_id) for row in refresh_rows),
        )

    def _load_candidate_rows(self) -> tuple[_CandidateFrequencyRow, ...]:
        rows = self.connection.execute(
            """
            SELECT candidate_id, candidate_key, ngram_size, raw_frequency
            FROM token_candidates
            WHERE inventory_version = ?
            """,
            (self.inventory_version,),
        ).fetchall()
        return tuple(
            _CandidateFrequencyRow(
                candidate_id=int(row["candidate_id"]),
                candidate_key=str(row["candidate_key"]),
                ngram_size=int(row["ngram_size"]),
                raw_frequency=int(row["raw_frequency"]),
            )
            for row in rows
        )

    def _build_refresh_rows(
        self,
        *,
        candidate_rows: tuple[_CandidateFrequencyRow, ...],
        unigram_total: int,
    ) -> tuple[_AssociationRefreshRow, ...]:
        frequency_by_key = {row.candidate_key: row.raw_frequency for row in candidate_rows}
        return tuple(
            _AssociationRefreshRow(
                candidate_id=row.candidate_id,
                t_score=t_score,
                npmi=npmi,
            )
            for row in candidate_rows
            if row.ngram_size in _SUPPORTED_NGRAM_SIZES
            for t_score, npmi in (
                _calculate_candidate_metrics(
                    candidate_row=row,
                    frequency_by_key=frequency_by_key,
                    unigram_total=unigram_total,
                ),
            )
        )

    def _clear_unsupported_metrics(self) -> None:
        self.connection.execute(
            """
            UPDATE token_candidates
            SET t_score = NULL,
                npmi = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE inventory_version = ?
            AND ngram_size NOT IN (2, 3)
            AND (
                t_score IS NOT NULL
                OR npmi IS NOT NULL
            )
            """,
            (self.inventory_version,),
        )


def _calculate_candidate_metrics(
    *,
    candidate_row: _CandidateFrequencyRow,
    frequency_by_key: dict[str, int],
    unigram_total: int,
) -> tuple[float | None, float | None]:
    split_keys = _split_candidate_key(
        candidate_key=candidate_row.candidate_key,
        ngram_size=candidate_row.ngram_size,
    )
    if not split_keys or unigram_total <= 0 or candidate_row.raw_frequency <= 0:
        return None, None

    observed = candidate_row.raw_frequency
    t_scores: list[float] = []
    npmi_scores: list[float] = []

    for left_key, right_key in split_keys:
        left_frequency = frequency_by_key.get(left_key)
        right_frequency = frequency_by_key.get(right_key)
        if left_frequency is None or right_frequency is None:
            return None, None

        t_score = _calculate_t_score(
            observed=observed,
            left_frequency=left_frequency,
            right_frequency=right_frequency,
            unigram_total=unigram_total,
        )
        npmi = _calculate_npmi(
            observed=observed,
            left_frequency=left_frequency,
            right_frequency=right_frequency,
            unigram_total=unigram_total,
        )

        if t_score is None:
            return None, None
        t_scores.append(t_score)

        if npmi is None:
            return min(t_scores), None
        npmi_scores.append(npmi)

    return min(t_scores), min(npmi_scores)


def _split_candidate_key(*, candidate_key: str, ngram_size: int) -> tuple[tuple[str, str], ...]:
    token_keys = tuple(candidate_key.split())
    if len(token_keys) != ngram_size:
        return ()
    if ngram_size == 2:
        return ((token_keys[0], token_keys[1]),)
    if ngram_size == 3:
        return (
            (token_keys[0], " ".join(token_keys[1:])),
            (" ".join(token_keys[:-1]), token_keys[-1]),
        )
    return ()


def _calculate_t_score(
    *,
    observed: int,
    left_frequency: int,
    right_frequency: int,
    unigram_total: int,
) -> float | None:
    if (
        observed <= 0
        or left_frequency <= 0
        or right_frequency <= 0
        or unigram_total <= 0
    ):
        return None
    expected = (left_frequency * right_frequency) / unigram_total
    return (observed - expected) / math.sqrt(observed)


def _calculate_npmi(
    *,
    observed: int,
    left_frequency: int,
    right_frequency: int,
    unigram_total: int,
) -> float | None:
    if (
        observed <= 0
        or left_frequency <= 0
        or right_frequency <= 0
        or unigram_total <= 0
    ):
        return None

    observed_probability = observed / unigram_total
    if observed_probability <= 0 or observed_probability >= 1:
        return None

    association_ratio = (observed * unigram_total) / (left_frequency * right_frequency)
    if association_ratio <= 0:
        return None

    return math.log(association_ratio) / -math.log(observed_probability)
