from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FeedShowMetadata:
    title: str
    feed_url: str
    site_url: str | None = None
    language: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class EpisodeRecord:
    guid: str
    title: str
    published_at: str | None = None
    audio_url: str | None = None
    episode_url: str | None = None
    duration_seconds: int | None = None
    summary: str | None = None
    has_transcript_tag: bool = False
    transcript_url: str | None = None


@dataclass(frozen=True)
class ParsedFeed:
    show: FeedShowMetadata
    episodes: list[EpisodeRecord]


@dataclass(frozen=True)
class SyncFeedResult:
    show_id: int
    title: str
    episodes_seen: int
    episodes_inserted: int
    episodes_updated: int
    episodes_skipped_no_audio: int
    episodes_with_transcript_tag: int
