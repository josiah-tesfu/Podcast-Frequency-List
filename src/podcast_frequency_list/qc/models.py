from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SegmentQcFlag:
    flag: str
    rule_name: str
    details: str
    status: str


@dataclass(frozen=True)
class SegmentQcEvaluation:
    segment_id: int
    episode_id: int
    status: str
    reason_summary: str
    flags: tuple[SegmentQcFlag, ...]


@dataclass(frozen=True)
class QcRunResult:
    scope: str
    scope_value: str
    qc_version: str
    selected_segments: int
    processed_segments: int
    skipped_segments: int
    keep_segments: int
    review_segments: int
    remove_segments: int
