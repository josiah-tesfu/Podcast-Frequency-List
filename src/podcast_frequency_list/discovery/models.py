from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PodcastCandidate:
    podcast_index_id: int | None
    title: str
    feed_url: str
    site_url: str | None = None
    author: str | None = None
    language: str | None = None
    description: str | None = None
    dead: bool = False
    score: float = 0.0
    source: str = ""


@dataclass(frozen=True)
class VerifiedFeed:
    feed_url: str
    feed_title: str
    content_type: str | None = None
    site_url: str | None = None
    language: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class SavedShow:
    show_id: int
    title: str
    feed_url: str
