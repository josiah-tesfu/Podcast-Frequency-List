from __future__ import annotations

from pathlib import Path

from podcast_frequency_list.corpus_review import CorpusMilestoneReviewService
from podcast_frequency_list.db import bootstrap_database, connect
from podcast_frequency_list.tokens import INVENTORY_VERSION
from podcast_frequency_list.tokens.models import (
    CandidateMetricsResult,
    CandidateMetricsValidationResult,
    CandidateScoresResult,
    CandidateSummaryRow,
)
from podcast_frequency_list.tokens.scores import SCORE_VERSION


def _insert_candidate(
    connection,
    *,
    candidate_id: int,
    candidate_key: str,
    ngram_size: int,
    show_dispersion: int,
) -> None:
    connection.execute(
        """
        INSERT INTO token_candidates (
            candidate_id,
            inventory_version,
            candidate_key,
            display_text,
            ngram_size,
            raw_frequency,
            episode_dispersion,
            show_dispersion
        )
        VALUES (?, ?, ?, ?, ?, 10, 3, ?)
        """,
        (
            candidate_id,
            INVENTORY_VERSION,
            candidate_key,
            candidate_key,
            ngram_size,
            show_dispersion,
        ),
    )


def _insert_candidate_score(
    connection,
    *,
    candidate_id: int,
    ranking_lane: str,
    is_eligible: int,
) -> None:
    connection.execute(
        """
        INSERT INTO candidate_scores (
            inventory_version,
            score_version,
            candidate_id,
            ranking_lane,
            passes_support_gate,
            passes_quality_gate,
            discard_family,
            is_eligible,
            frequency_score,
            dispersion_score,
            association_score,
            boundary_score,
            redundancy_penalty,
            final_score,
            lane_rank
        )
        VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            INVENTORY_VERSION,
            SCORE_VERSION,
            candidate_id,
            ranking_lane,
            is_eligible,
            None if is_eligible else "support_floor",
            is_eligible,
            0.5 if is_eligible else None,
            0.5 if is_eligible else None,
            0.5 if ranking_lane != "1gram" and is_eligible else None,
            0.5 if ranking_lane != "1gram" and is_eligible else None,
            0.0 if is_eligible else None,
            0.5 if is_eligible else None,
            candidate_id if is_eligible else None,
        ),
    )


def _row(candidate_key: str, ngram_size: int, final_score: float) -> CandidateSummaryRow:
    return CandidateSummaryRow(
        candidate_key=candidate_key,
        display_text=candidate_key,
        ngram_size=ngram_size,
        raw_frequency=10,
        episode_dispersion=3,
        show_dispersion=2,
        t_score=1.0 if ngram_size >= 2 else None,
        npmi=0.4 if ngram_size >= 2 else None,
        left_context_type_count=2 if ngram_size >= 2 else None,
        right_context_type_count=2 if ngram_size >= 2 else None,
        left_entropy=0.8 if ngram_size >= 2 else None,
        right_entropy=0.8 if ngram_size >= 2 else None,
        punctuation_gap_occurrence_count=0 if ngram_size >= 2 else None,
        punctuation_gap_occurrence_ratio=0.0 if ngram_size >= 2 else None,
        punctuation_gap_edge_clitic_count=0 if ngram_size >= 2 else None,
        punctuation_gap_edge_clitic_ratio=0.0 if ngram_size >= 2 else None,
        max_component_information=4.0 if ngram_size >= 2 else None,
        min_component_information=2.0 if ngram_size >= 2 else None,
        high_information_token_count=1 if ngram_size >= 2 else None,
        covered_by_any_count=1 if ngram_size <= 2 else None,
        covered_by_any_ratio=0.1 if ngram_size <= 2 else None,
        independent_occurrence_count=9 if ngram_size <= 2 else None,
        direct_parent_count=1 if ngram_size <= 2 else None,
        dominant_parent_key="x" if ngram_size <= 2 else None,
        dominant_parent_shared_count=1 if ngram_size <= 2 else None,
        dominant_parent_share=0.1 if ngram_size <= 2 else None,
        dominant_parent_side="left" if ngram_size <= 2 else None,
        score_version=SCORE_VERSION,
        ranking_lane=f"{ngram_size}gram",
        passes_support_gate=1,
        passes_quality_gate=1,
        discard_family=None,
        is_eligible=1,
        frequency_score=0.5,
        dispersion_score=0.5,
        association_score=0.5 if ngram_size >= 2 else None,
        boundary_score=0.5 if ngram_size >= 2 else None,
        redundancy_penalty=0.0,
        final_score=final_score,
        lane_rank=1,
    )


class _FakeCandidateMetricsService:
    def __init__(self) -> None:
        self.refresh_calls = 0
        self.summarize_calls = 0
        self.validate_calls = 0

    def summarize(
        self, *, inventory_version: str = INVENTORY_VERSION
    ) -> CandidateMetricsResult:
        assert inventory_version == INVENTORY_VERSION
        self.summarize_calls += 1
        return CandidateMetricsResult(
            inventory_version=inventory_version,
            selected_candidates=4,
            refreshed_candidates=4,
            deleted_orphan_candidates=0,
            occurrence_count=20,
            raw_frequency_total=40,
            episode_dispersion_total=12,
            show_dispersion_total=8,
            display_text_updates=0,
        )

    def refresh(self, *, inventory_version: str = INVENTORY_VERSION) -> CandidateMetricsResult:
        assert inventory_version == INVENTORY_VERSION
        self.refresh_calls += 1
        return CandidateMetricsResult(
            inventory_version=inventory_version,
            selected_candidates=4,
            refreshed_candidates=4,
            deleted_orphan_candidates=0,
            occurrence_count=20,
            raw_frequency_total=40,
            episode_dispersion_total=12,
            show_dispersion_total=8,
            display_text_updates=0,
        )

    def validate(
        self, *, inventory_version: str = INVENTORY_VERSION
    ) -> CandidateMetricsValidationResult:
        assert inventory_version == INVENTORY_VERSION
        self.validate_calls += 1
        return CandidateMetricsValidationResult(
            inventory_version=inventory_version,
            candidate_count=4,
            occurrence_count=20,
            raw_frequency_mismatch_count=0,
            episode_dispersion_mismatch_count=0,
            show_dispersion_mismatch_count=0,
            display_text_mismatch_count=0,
            foreign_key_issue_count=0,
        )


class _FakeCandidateScoresService:
    def __init__(self) -> None:
        self.refresh_calls = 0
        self.summarize_calls = 0
        self.rows = (
            _row("alpha", 1, 0.95),
            _row("beta", 2, 0.80),
            _row("gamma", 3, 0.60),
            _row("delta", 2, 0.40),
        )

    def summarize(
        self,
        *,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> CandidateScoresResult:
        assert inventory_version == INVENTORY_VERSION
        assert score_version == SCORE_VERSION
        self.summarize_calls += 1
        return CandidateScoresResult(
            inventory_version=inventory_version,
            score_version=score_version,
            selected_candidates=4,
            stored_candidates=4,
            support_pass_candidates=4,
            quality_pass_candidates=4,
            eligible_candidates=4,
            eligible_1gram_candidates=1,
            eligible_2gram_candidates=2,
            eligible_3gram_candidates=1,
        )

    def refresh(
        self,
        *,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
    ) -> CandidateScoresResult:
        assert inventory_version == INVENTORY_VERSION
        assert score_version == SCORE_VERSION
        self.refresh_calls += 1
        return CandidateScoresResult(
            inventory_version=inventory_version,
            score_version=score_version,
            selected_candidates=4,
            stored_candidates=4,
            support_pass_candidates=4,
            quality_pass_candidates=4,
            eligible_candidates=4,
            eligible_1gram_candidates=1,
            eligible_2gram_candidates=2,
            eligible_3gram_candidates=1,
        )

    def list_global_candidates(
        self,
        *,
        limit: int,
        offset: int,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
        include_step5: bool = True,
    ) -> tuple[CandidateSummaryRow, ...]:
        assert inventory_version == INVENTORY_VERSION
        assert score_version == SCORE_VERSION
        return self.rows[offset : offset + limit]

    def list_candidates_by_key(
        self,
        *,
        candidate_keys,
        inventory_version: str = INVENTORY_VERSION,
        score_version: str = SCORE_VERSION,
        include_step5: bool = True,
    ) -> tuple[CandidateSummaryRow, ...]:
        assert inventory_version == INVENTORY_VERSION
        assert score_version == SCORE_VERSION
        wanted = set(candidate_keys)
        return tuple(row for row in self.rows if row.candidate_key in wanted)


def test_corpus_milestone_review_defaults_to_inspect_only(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_id=1,
            candidate_key="alpha",
            ngram_size=1,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_id=2,
            candidate_key="beta",
            ngram_size=2,
            show_dispersion=2,
        )
        _insert_candidate(
            connection,
            candidate_id=3,
            candidate_key="gamma",
            ngram_size=2,
            show_dispersion=3,
        )
        _insert_candidate(
            connection,
            candidate_id=4,
            candidate_key="delta",
            ngram_size=3,
            show_dispersion=2,
        )
        _insert_candidate_score(connection, candidate_id=1, ranking_lane="1gram", is_eligible=1)
        _insert_candidate_score(connection, candidate_id=2, ranking_lane="2gram", is_eligible=1)
        _insert_candidate_score(connection, candidate_id=3, ranking_lane="2gram", is_eligible=0)
        _insert_candidate_score(connection, candidate_id=4, ranking_lane="3gram", is_eligible=1)
        connection.commit()

    metrics_service = _FakeCandidateMetricsService()
    scores_service = _FakeCandidateScoresService()
    service = CorpusMilestoneReviewService(
        db_path=db_path,
        candidate_metrics_service=metrics_service,
        candidate_scores_service=scores_service,
    )

    result = service.review(limit=2)

    assert metrics_service.summarize_calls == 1
    assert scores_service.summarize_calls == 1
    assert metrics_service.refresh_calls == 0
    assert scores_service.refresh_calls == 0
    assert metrics_service.validate_calls == 0
    assert result.metrics_validation is None
    assert result.metrics_is_deterministic is None
    assert result.scores_is_deterministic is None
    assert result.middle_offset == 1
    assert result.tail_offset == 2
    assert tuple(row.candidate_key for row in result.top_rows) == ("alpha", "beta")
    assert tuple(row.candidate_key for row in result.middle_rows) == ("beta", "gamma")
    assert tuple(row.candidate_key for row in result.tail_rows) == ("gamma", "delta")

    dispersion_rows = {row.ngram_size: row for row in result.dispersion_rows}
    assert dispersion_rows[1].total_candidates == 1
    assert dispersion_rows[1].multi_show_candidates == 0
    assert dispersion_rows[2].total_candidates == 2
    assert dispersion_rows[2].multi_show_candidates == 2
    assert dispersion_rows[2].cross_show_3plus_candidates == 1
    assert dispersion_rows[2].eligible_candidates == 1
    assert dispersion_rows[2].eligible_multi_show_candidates == 1
    assert dispersion_rows[3].eligible_candidates == 1


def test_corpus_milestone_review_refresh_mode_supports_determinism_and_validation(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_id=1,
            candidate_key="alpha",
            ngram_size=1,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_id=2,
            candidate_key="beta",
            ngram_size=2,
            show_dispersion=2,
        )
        _insert_candidate(
            connection,
            candidate_id=3,
            candidate_key="gamma",
            ngram_size=2,
            show_dispersion=3,
        )
        _insert_candidate(
            connection,
            candidate_id=4,
            candidate_key="delta",
            ngram_size=3,
            show_dispersion=2,
        )
        _insert_candidate_score(connection, candidate_id=1, ranking_lane="1gram", is_eligible=1)
        _insert_candidate_score(connection, candidate_id=2, ranking_lane="2gram", is_eligible=1)
        _insert_candidate_score(connection, candidate_id=3, ranking_lane="2gram", is_eligible=0)
        _insert_candidate_score(connection, candidate_id=4, ranking_lane="3gram", is_eligible=1)
        connection.commit()

    metrics_service = _FakeCandidateMetricsService()
    scores_service = _FakeCandidateScoresService()
    service = CorpusMilestoneReviewService(
        db_path=db_path,
        candidate_metrics_service=metrics_service,
        candidate_scores_service=scores_service,
    )

    result = service.review(
        limit=2,
        refresh_first=True,
        check_determinism=True,
        validate_metrics=True,
    )

    assert metrics_service.refresh_calls == 2
    assert scores_service.refresh_calls == 2
    assert metrics_service.summarize_calls == 0
    assert scores_service.summarize_calls == 0
    assert metrics_service.validate_calls == 1
    assert result.metrics_is_deterministic is True
    assert result.scores_is_deterministic is True
    assert result.middle_offset == 1
    assert result.tail_offset == 2
    assert tuple(row.candidate_key for row in result.top_rows) == ("alpha", "beta")
    assert tuple(row.candidate_key for row in result.middle_rows) == ("beta", "gamma")
    assert tuple(row.candidate_key for row in result.tail_rows) == ("gamma", "delta")

    dispersion_rows = {row.ngram_size: row for row in result.dispersion_rows}
    assert dispersion_rows[1].total_candidates == 1
    assert dispersion_rows[1].multi_show_candidates == 0
    assert dispersion_rows[2].total_candidates == 2
    assert dispersion_rows[2].multi_show_candidates == 2
    assert dispersion_rows[2].cross_show_3plus_candidates == 1
    assert dispersion_rows[2].eligible_candidates == 1
    assert dispersion_rows[2].eligible_multi_show_candidates == 1
    assert dispersion_rows[3].eligible_candidates == 1
