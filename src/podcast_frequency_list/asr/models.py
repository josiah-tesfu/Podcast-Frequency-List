from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AudioChunk:
    path: Path
    chunk_index: int
    start_seconds: int
    end_seconds: int


@dataclass(frozen=True)
class AsrEpisodeResult:
    episode_id: int
    title: str
    status: str
    audio_path: Path | None
    transcript_path: Path | None
    chunk_count: int
    text_chars: int
    preview: str
    error: str | None = None


@dataclass(frozen=True)
class AsrRunResult:
    pilot_name: str
    model: str
    requested_limit: int | None
    selected_count: int
    completed_count: int
    skipped_count: int
    failed_count: int
    chunk_count: int
    episode_results: tuple[AsrEpisodeResult, ...]
