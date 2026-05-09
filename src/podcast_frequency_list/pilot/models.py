from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PilotEpisode:
    episode_id: int
    title: str
    published_at: str | None
    duration_seconds: int
    cumulative_seconds: int


@dataclass(frozen=True)
class PilotSelectionResult:
    pilot_run_id: int
    name: str
    show_id: int
    show_title: str
    target_seconds: int
    total_seconds: int
    selected_count: int
    skipped_count: int
    estimated_cost_usd: float
    model: str
    selection_order: str
    first_published_at: str | None
    last_published_at: str | None
    episodes: tuple[PilotEpisode, ...]


@dataclass(frozen=True)
class CorpusStatusRow:
    show_id: int
    title: str
    feed_url: str
    episode_count: int
    total_seconds: int
    episodes_with_transcript_tag: int
    slice_id: int | None
    slice_name: str | None
    slice_selection_order: str | None
    selected_episodes: int
    selected_seconds: int
    needs_asr_episodes: int
    in_progress_asr_episodes: int
    ready_asr_episodes: int
    failed_asr_episodes: int


@dataclass(frozen=True)
class CorpusStatusResult:
    show_count: int
    slice_count: int
    episode_count: int
    total_seconds: int
    episodes_with_transcript_tag: int
    selected_slice_episodes: int
    selected_slice_seconds: int
    needs_asr_episodes: int
    in_progress_asr_episodes: int
    ready_asr_episodes: int
    failed_asr_episodes: int
    rows: tuple[CorpusStatusRow, ...]
