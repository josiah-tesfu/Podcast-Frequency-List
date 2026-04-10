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
