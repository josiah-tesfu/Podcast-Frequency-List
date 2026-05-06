from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SentenceToken:
    token_index: int
    token_key: str
    surface_text: str
    char_start: int
    char_end: int
    token_type: str


@dataclass(frozen=True)
class CandidateSpan:
    sentence_id: int
    episode_id: int
    segment_id: int
    candidate_key: str
    display_text: str
    ngram_size: int
    token_start_index: int
    token_end_index: int
    char_start: int
    char_end: int
    surface_text: str


@dataclass(frozen=True)
class TokenizationResult:
    scope: str
    scope_value: str
    tokenization_version: str
    selected_sentences: int
    tokenized_sentences: int
    created_tokens: int
    skipped_sentences: int
    episode_count: int


@dataclass(frozen=True)
class CandidateInventoryResult:
    scope: str
    scope_value: str
    inventory_version: str
    selected_sentences: int
    processed_sentences: int
    skipped_sentences: int
    created_candidates: int
    created_occurrences: int
    episode_count: int


@dataclass(frozen=True)
class CandidateMetricsResult:
    inventory_version: str
    selected_candidates: int
    refreshed_candidates: int
    deleted_orphan_candidates: int
    occurrence_count: int
    raw_frequency_total: int
    episode_dispersion_total: int
    show_dispersion_total: int
    display_text_updates: int


@dataclass(frozen=True)
class CandidateMetricsValidationResult:
    inventory_version: str
    candidate_count: int
    occurrence_count: int
    raw_frequency_mismatch_count: int
    episode_dispersion_mismatch_count: int
    show_dispersion_mismatch_count: int
    display_text_mismatch_count: int
    foreign_key_issue_count: int


@dataclass(frozen=True)
class CandidateScoresResult:
    inventory_version: str
    score_version: str
    selected_candidates: int
    stored_candidates: int
    eligible_candidates: int
    eligible_1gram_candidates: int
    eligible_2gram_candidates: int
    eligible_3gram_candidates: int


@dataclass(frozen=True)
class CandidateSummaryRow:
    candidate_key: str
    display_text: str
    ngram_size: int
    raw_frequency: int
    episode_dispersion: int
    show_dispersion: int
    t_score: float | None = None
    npmi: float | None = None
    left_context_type_count: int | None = None
    right_context_type_count: int | None = None
    left_entropy: float | None = None
    right_entropy: float | None = None
    covered_by_any_count: int | None = None
    covered_by_any_ratio: float | None = None
    independent_occurrence_count: int | None = None
    direct_parent_count: int | None = None
    dominant_parent_key: str | None = None
    dominant_parent_shared_count: int | None = None
    dominant_parent_share: float | None = None
    dominant_parent_side: str | None = None
    score_version: str | None = None
    ranking_lane: str | None = None
    is_eligible: int | None = None
    frequency_score: float | None = None
    dispersion_score: float | None = None
    association_score: float | None = None
    boundary_score: float | None = None
    redundancy_penalty: float | None = None
    final_score: float | None = None
    lane_rank: int | None = None
