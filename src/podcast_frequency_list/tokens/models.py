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
