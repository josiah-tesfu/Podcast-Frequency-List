from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.tokens import (
    INVENTORY_VERSION,
    CandidateMetricsResult,
    CandidateMetricsService,
    CandidateMetricsValidationResult,
    CandidateScoresResult,
    CandidateScoresService,
    CandidateSummaryRow,
)
from podcast_frequency_list.tokens.scores import SCORE_VERSION

DEFAULT_REVIEW_LIMIT = 10
_FOCUS_KEYS = (
    "faut que",
    "train de",
    "en fait",
    "ce moment",
    "ai envie",
    "du coup",
    "est pour",
    "est que",
    "de le",
    "en a",
    "en fait c",
    "moi j",
    "je pense",
    "il y a",
    "tu vois",
)


class CorpusMilestoneReviewError(RuntimeError):
    pass


@dataclass(frozen=True)
class CorpusDispersionRow:
    ngram_size: int
    total_candidates: int
    multi_show_candidates: int
    cross_show_3plus_candidates: int
    eligible_candidates: int
    eligible_multi_show_candidates: int
    eligible_cross_show_3plus_candidates: int
    max_show_dispersion: int


@dataclass(frozen=True)
class CorpusMilestoneReviewResult:
    metrics_result: CandidateMetricsResult
    metrics_validation: CandidateMetricsValidationResult | None
    metrics_is_deterministic: bool | None
    scores_result: CandidateScoresResult
    scores_is_deterministic: bool | None
    middle_offset: int
    tail_offset: int
    dispersion_rows: tuple[CorpusDispersionRow, ...]
    top_rows: tuple[CandidateSummaryRow, ...]
    middle_rows: tuple[CandidateSummaryRow, ...]
    tail_rows: tuple[CandidateSummaryRow, ...]
    focus_rows: tuple[CandidateSummaryRow, ...]


class CorpusMilestoneReviewService:
    def __init__(
        self,
        *,
        db_path: Path,
        candidate_metrics_service: CandidateMetricsService,
        candidate_scores_service: CandidateScoresService,
    ) -> None:
        self.db_path = db_path
        self.candidate_metrics_service = candidate_metrics_service
        self.candidate_scores_service = candidate_scores_service

    def close(self) -> None:
        for service in (self.candidate_metrics_service, self.candidate_scores_service):
            close = getattr(service, "close", None)
            if close is not None:
                close()

    def review(
        self,
        *,
        limit: int = DEFAULT_REVIEW_LIMIT,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
        refresh_first: bool = False,
        check_determinism: bool = False,
        validate_metrics: bool = False,
    ) -> CorpusMilestoneReviewResult:
        if limit < 1:
            raise CorpusMilestoneReviewError("limit must be positive")

        if check_determinism:
            refresh_first = True

        if refresh_first:
            metrics_result = self.candidate_metrics_service.refresh(
                inventory_version=inventory_version
            )
            repeated_metrics_result = None
            if check_determinism:
                repeated_metrics_result = self.candidate_metrics_service.refresh(
                    inventory_version=inventory_version
                )

            scores_result = self.candidate_scores_service.refresh(
                inventory_version=inventory_version,
                score_version=score_version,
            )
            repeated_scores_result = None
            if check_determinism:
                repeated_scores_result = self.candidate_scores_service.refresh(
                    inventory_version=inventory_version,
                    score_version=score_version,
                )
        else:
            metrics_result = self.candidate_metrics_service.summarize(
                inventory_version=inventory_version
            )
            repeated_metrics_result = None
            scores_result = self.candidate_scores_service.summarize(
                inventory_version=inventory_version,
                score_version=score_version,
            )
            repeated_scores_result = None

        metrics_validation = None
        if validate_metrics:
            metrics_validation = self.candidate_metrics_service.validate(
                inventory_version=inventory_version
            )

        eligible_candidates = scores_result.eligible_candidates
        middle_offset = max((eligible_candidates // 2) - (limit // 2), 0)
        tail_offset = max(eligible_candidates - limit, 0)

        return CorpusMilestoneReviewResult(
            metrics_result=metrics_result,
            metrics_validation=metrics_validation,
            metrics_is_deterministic=(
                metrics_result == repeated_metrics_result
                if repeated_metrics_result is not None
                else None
            ),
            scores_result=scores_result,
            scores_is_deterministic=(
                scores_result == repeated_scores_result
                if repeated_scores_result is not None
                else None
            ),
            middle_offset=middle_offset,
            tail_offset=tail_offset,
            dispersion_rows=self._load_dispersion_rows(
                inventory_version=inventory_version,
                score_version=score_version,
            ),
            top_rows=self.candidate_scores_service.list_global_candidates(
                limit=limit,
                offset=0,
                inventory_version=inventory_version,
                score_version=score_version,
            ),
            middle_rows=self.candidate_scores_service.list_global_candidates(
                limit=limit,
                offset=middle_offset,
                inventory_version=inventory_version,
                score_version=score_version,
            ),
            tail_rows=self.candidate_scores_service.list_global_candidates(
                limit=limit,
                offset=tail_offset,
                inventory_version=inventory_version,
                score_version=score_version,
            ),
            focus_rows=self.candidate_scores_service.list_candidates_by_key(
                candidate_keys=_FOCUS_KEYS,
                inventory_version=inventory_version,
                score_version=score_version,
            ),
        )

    def _load_dispersion_rows(
        self,
        *,
        inventory_version: str,
        score_version: str,
    ) -> tuple[CorpusDispersionRow, ...]:
        with connect(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    cand.ngram_size,
                    COUNT(*) AS total_candidates,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN cand.show_dispersion >= 2 THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS multi_show_candidates,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN cand.show_dispersion >= 3 THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS cross_show_3plus_candidates,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN score.is_eligible = 1 THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS eligible_candidates,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN score.is_eligible = 1
                                AND cand.show_dispersion >= 2 THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS eligible_multi_show_candidates,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN score.is_eligible = 1
                                AND cand.show_dispersion >= 3 THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS eligible_cross_show_3plus_candidates,
                    MAX(cand.show_dispersion) AS max_show_dispersion
                FROM token_candidates cand
                LEFT JOIN candidate_scores score
                    ON score.candidate_id = cand.candidate_id
                    AND score.inventory_version = cand.inventory_version
                    AND score.score_version = ?
                WHERE cand.inventory_version = ?
                GROUP BY cand.ngram_size
                ORDER BY cand.ngram_size
                """,
                (score_version, inventory_version),
            ).fetchall()

        return tuple(
            CorpusDispersionRow(
                ngram_size=int(row["ngram_size"]),
                total_candidates=int(row["total_candidates"]),
                multi_show_candidates=int(row["multi_show_candidates"]),
                cross_show_3plus_candidates=int(row["cross_show_3plus_candidates"]),
                eligible_candidates=int(row["eligible_candidates"]),
                eligible_multi_show_candidates=int(row["eligible_multi_show_candidates"]),
                eligible_cross_show_3plus_candidates=int(
                    row["eligible_cross_show_3plus_candidates"]
                ),
                max_show_dispersion=int(row["max_show_dispersion"]),
            )
            for row in rows
        )
