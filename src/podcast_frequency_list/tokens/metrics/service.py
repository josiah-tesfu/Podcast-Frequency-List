from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.tokens.inventory import INVENTORY_VERSION
from podcast_frequency_list.tokens.models import (
    CandidateMetricsResult,
    CandidateMetricsValidationResult,
    CandidateSummaryRow,
)

from .queries import _CandidateSummaryStore
from .workflow import _CandidateMetricsWorkflow


class CandidateMetricsError(RuntimeError):
    pass


MIN_NGRAM_SIZE = 1
MAX_NGRAM_SIZE = 4
DEFAULT_SUMMARY_LIMIT = 20
DEFAULT_SUMMARY_OFFSET = 0


class CandidateMetricsService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def summarize(
        self,
        *,
        inventory_version: str = INVENTORY_VERSION,
    ) -> CandidateMetricsResult:
        with connect(self.db_path) as connection:
            workflow = _CandidateMetricsWorkflow(
                connection=connection,
                inventory_version=inventory_version,
            )
            selected_candidates = workflow.count_candidates()
            return workflow.summarize(selected_candidates=selected_candidates)

    def list_candidates_by_key(
        self,
        *,
        candidate_keys: Iterable[str],
        inventory_version: str = INVENTORY_VERSION,
    ) -> tuple[CandidateSummaryRow, ...]:
        with connect(self.db_path) as connection:
            return _CandidateSummaryStore(
                connection=connection,
                inventory_version=inventory_version,
            ).list_candidates_by_key(candidate_keys)

    def list_top_candidates(
        self,
        *,
        ngram_size: int,
        limit: int = DEFAULT_SUMMARY_LIMIT,
        offset: int = DEFAULT_SUMMARY_OFFSET,
        inventory_version: str = INVENTORY_VERSION,
    ) -> tuple[CandidateSummaryRow, ...]:
        _validate_ngram_size(ngram_size)
        _validate_limit(limit)
        _validate_offset(offset)

        with connect(self.db_path) as connection:
            return _CandidateSummaryStore(
                connection=connection,
                inventory_version=inventory_version,
            ).list_top_candidates(ngram_size=ngram_size, limit=limit, offset=offset)

    def validate(
        self,
        *,
        inventory_version: str = INVENTORY_VERSION,
    ) -> CandidateMetricsValidationResult:
        with connect(self.db_path) as connection:
            return _CandidateMetricsWorkflow(
                connection=connection,
                inventory_version=inventory_version,
            ).validate()

    def refresh(self, *, inventory_version: str = INVENTORY_VERSION) -> CandidateMetricsResult:
        with connect(self.db_path) as connection:
            workflow = _CandidateMetricsWorkflow(
                connection=connection,
                inventory_version=inventory_version,
            )
            selected_candidates = workflow.count_candidates()
            if selected_candidates == 0:
                raise CandidateMetricsError(
                    f"no token candidates found for inventory_version={inventory_version!r}"
                )

            result = workflow.refresh(selected_candidates=selected_candidates)
            connection.commit()
            return result


def _validate_ngram_size(ngram_size: int) -> None:
    if ngram_size < MIN_NGRAM_SIZE or ngram_size > MAX_NGRAM_SIZE:
        raise CandidateMetricsError(
            f"ngram_size must be between {MIN_NGRAM_SIZE} and {MAX_NGRAM_SIZE}"
        )


def _validate_limit(limit: int) -> None:
    if limit < 1:
        raise CandidateMetricsError("limit must be positive")


def _validate_offset(offset: int) -> None:
    if offset < 0:
        raise CandidateMetricsError("offset must be non-negative")
