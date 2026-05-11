from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class _CandidateScoreInput:
    candidate_id: int
    candidate_key: str
    ngram_size: int
    raw_frequency: int
    episode_dispersion: int
    show_dispersion: int
    t_score: float | None
    npmi: float | None
    left_entropy: float | None
    right_entropy: float | None
    punctuation_gap_occurrence_ratio: float | None
    punctuation_gap_edge_clitic_ratio: float | None
    max_component_information: float | None
    max_show_share: float | None
    top2_show_share: float | None
    show_entropy: float | None
    dominant_parent_share: float | None
    ranking_lane: str
    passes_support_gate: bool


@dataclass(frozen=True)
class _CandidateScoreRow:
    inventory_version: str
    score_version: str
    candidate_id: int
    ranking_lane: str
    passes_support_gate: int
    passes_quality_gate: int
    discard_family: str | None
    is_eligible: int
    frequency_score: float | None
    dispersion_score: float | None
    association_score: float | None
    boundary_score: float | None
    redundancy_penalty: float | None
    final_score: float | None
    lane_rank: int | None
