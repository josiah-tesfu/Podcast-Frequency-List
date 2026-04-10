from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizationRunResult:
    scope: str
    scope_value: str
    normalization_version: str
    selected_segments: int
    normalized_segments: int
    skipped_segments: int
    episode_count: int
