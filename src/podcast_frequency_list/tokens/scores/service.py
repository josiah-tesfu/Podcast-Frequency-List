from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.tokens.inventory import INVENTORY_VERSION
from podcast_frequency_list.tokens.models import CandidateScoresResult, CandidateSummaryRow
from podcast_frequency_list.tokens.scores.errors import CandidateScoresError
from podcast_frequency_list.tokens.scores.policy import (
    _LANE_SPECS,
    DEFAULT_SUMMARY_LIMIT,
    SCORE_VERSION,
)
from podcast_frequency_list.tokens.scores.queries import _CandidateScoreSummaryStore
from podcast_frequency_list.tokens.scores.workflow import _CandidateScoresWorkflow


class CandidateScoresService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def summarize(
        self,
        *,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> CandidateScoresResult:
        with connect(self.db_path) as connection:
            workflow = _CandidateScoresWorkflow(
                connection=connection,
                inventory_version=inventory_version,
                score_version=score_version,
            )
            selected_candidates = workflow.count_candidates()
            return workflow.summarize(selected_candidates=selected_candidates)

    def list_candidates_by_key(
        self,
        *,
        candidate_keys: Iterable[str],
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> tuple[CandidateSummaryRow, ...]:
        with connect(self.db_path) as connection:
            return _CandidateScoreSummaryStore(
                connection=connection,
                inventory_version=inventory_version,
                score_version=score_version,
            ).list_candidates_by_key(candidate_keys)

    def list_top_candidates(
        self,
        *,
        ngram_size: int,
        limit: int = DEFAULT_SUMMARY_LIMIT,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> tuple[CandidateSummaryRow, ...]:
        _validate_ngram_size(ngram_size)
        _validate_limit(limit)

        with connect(self.db_path) as connection:
            return _CandidateScoreSummaryStore(
                connection=connection,
                inventory_version=inventory_version,
                score_version=score_version,
            ).list_top_candidates(ngram_size=ngram_size, limit=limit)

    def list_global_candidates(
        self,
        *,
        limit: int = DEFAULT_SUMMARY_LIMIT,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> tuple[CandidateSummaryRow, ...]:
        _validate_limit(limit)

        with connect(self.db_path) as connection:
            return _CandidateScoreSummaryStore(
                connection=connection,
                inventory_version=inventory_version,
                score_version=score_version,
            ).list_global_candidates(limit=limit)

    def refresh(
        self,
        *,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> CandidateScoresResult:
        with connect(self.db_path) as connection:
            workflow = _CandidateScoresWorkflow(
                connection=connection,
                inventory_version=inventory_version,
                score_version=score_version,
            )
            selected_candidates = workflow.count_candidates()
            if selected_candidates == 0:
                raise CandidateScoresError(
                    f"no token candidates found for inventory_version={inventory_version!r}"
                )

            result = workflow.refresh(selected_candidates=selected_candidates)
            connection.commit()
            return result


def _validate_ngram_size(ngram_size: int) -> None:
    if ngram_size not in _LANE_SPECS:
        raise CandidateScoresError(
            f"ngram_size must be one of {', '.join(str(size) for size in sorted(_LANE_SPECS))}"
        )


def _validate_limit(limit: int) -> None:
    if limit < 1:
        raise CandidateScoresError("limit must be positive")
