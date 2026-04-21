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
class TokenizationResult:
    scope: str
    scope_value: str
    tokenization_version: str
    selected_sentences: int
    tokenized_sentences: int
    created_tokens: int
    skipped_sentences: int
    episode_count: int
