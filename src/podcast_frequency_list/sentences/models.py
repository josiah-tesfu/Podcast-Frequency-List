from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SentenceSpan:
    sentence_index: int
    char_start: int
    char_end: int
    sentence_text: str


@dataclass(frozen=True)
class SentenceSplitResult:
    scope: str
    scope_value: str
    split_version: str
    selected_segments: int
    created_sentences: int
    skipped_segments: int
    episode_count: int
