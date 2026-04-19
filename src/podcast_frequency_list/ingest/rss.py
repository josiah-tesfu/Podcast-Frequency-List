from __future__ import annotations

import hashlib
from calendar import timegm
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

import feedparser
import httpx

from podcast_frequency_list.discovery.common import DEFAULT_USER_AGENT
from podcast_frequency_list.feed_parsing import (
    extract_feed_metadata,
    extract_transcript_tags,
    fetch_feed_document,
)
from podcast_frequency_list.ingest.models import EpisodeRecord, FeedShowMetadata, ParsedFeed


class RssFeedError(RuntimeError):
    pass


def parse_duration_seconds(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value

    raw_value = str(value).strip()
    if not raw_value:
        return None
    if raw_value.isdigit():
        return int(raw_value)

    parts = raw_value.split(":")
    if len(parts) not in {2, 3} or not all(part.isdigit() for part in parts):
        return None

    numbers = [int(part) for part in parts]
    if len(numbers) == 2:
        minutes, seconds = numbers
        return (minutes * 60) + seconds

    hours, minutes, seconds = numbers
    return (hours * 3600) + (minutes * 60) + seconds


def _normalize_datetime(raw_value: str | None, parsed_value: object | None) -> str | None:
    if raw_value:
        try:
            parsed_datetime = parsedate_to_datetime(raw_value)
        except (TypeError, ValueError, IndexError):
            parsed_datetime = None
        else:
            if parsed_datetime.tzinfo is None:
                parsed_datetime = parsed_datetime.replace(tzinfo=UTC)
            return parsed_datetime.isoformat()

    if parsed_value is not None:
        return datetime.fromtimestamp(timegm(parsed_value), tz=UTC).isoformat()

    return None


def _extract_audio_url(entry: feedparser.FeedParserDict) -> str | None:
    for enclosure in entry.get("enclosures", []):
        href = (enclosure.get("href") or enclosure.get("url") or "").strip()
        media_type = (enclosure.get("type") or "").strip().lower()
        if href and (not media_type or media_type.startswith("audio/")):
            return href

    for link in entry.get("links", []):
        href = (link.get("href") or "").strip()
        media_type = (link.get("type") or "").strip().lower()
        if href and link.get("rel") == "enclosure" and (
            not media_type or media_type.startswith("audio/")
        ):
            return href

    return None


def _extract_episode_url(entry: feedparser.FeedParserDict) -> str | None:
    direct_link = (entry.get("link") or "").strip()
    if direct_link:
        return direct_link

    for link in entry.get("links", []):
        href = (link.get("href") or "").strip()
        rel = (link.get("rel") or "alternate").strip().lower()
        if href and rel == "alternate":
            return href

    return None


def _extract_summary(entry: feedparser.FeedParserDict) -> str | None:
    for key in ("summary", "description"):
        value = entry.get(key)
        if value:
            return str(value).strip()

    for content in entry.get("content", []):
        value = content.get("value")
        if value:
            return str(value).strip()

    return None


def _derive_guid(
    entry: feedparser.FeedParserDict,
    *,
    audio_url: str | None,
    episode_url: str | None,
    published_at: str | None,
) -> str:
    for key in ("id", "guid"):
        raw_value = (entry.get(key) or "").strip()
        if raw_value:
            return raw_value

    if audio_url:
        return f"audio:{audio_url}"
    if episode_url:
        return f"url:{episode_url}"

    title = (entry.get("title") or "").strip()
    digest = hashlib.sha1(f"{title}|{published_at or ''}".encode()).hexdigest()
    return f"generated:{digest}"


class RssFeedClient:
    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = 20.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            follow_redirects=True,
            timeout=timeout,
            transport=transport,
            headers={"User-Agent": user_agent},
        )

    def close(self) -> None:
        self._client.close()

    def parse_feed(self, feed_url: str, *, limit: int | None = None) -> ParsedFeed:
        response, document = fetch_feed_document(
            self._client,
            feed_url=feed_url,
            error_type=RssFeedError,
        )

        parsed = feedparser.parse(response.content)
        if parsed.bozo and not parsed.entries and not parsed.feed:
            raise RssFeedError(f"feed parsing failed: {parsed.bozo_exception}")

        try:
            metadata = extract_feed_metadata(document)
        except ElementTree.ParseError as exc:
            raise RssFeedError(f"feed XML could not be parsed: {exc}") from exc
        except ValueError as exc:
            raise RssFeedError(str(exc)) from exc
        feed_title = (parsed.feed.get("title") or metadata.get("title") or "").strip()
        if not feed_title:
            raise RssFeedError("feed title could not be determined")

        transcript_items = extract_transcript_tags(document)
        parsed_entries = parsed.entries[:limit] if limit is not None else parsed.entries

        episodes: list[EpisodeRecord] = []
        for index, entry in enumerate(parsed_entries):
            audio_url = _extract_audio_url(entry)
            episode_url = _extract_episode_url(entry)
            published_at = _normalize_datetime(
                entry.get("published") or entry.get("updated"),
                entry.get("published_parsed") or entry.get("updated_parsed"),
            )
            transcript_item = (
                transcript_items[index]
                if index < len(transcript_items)
                else None
            )
            duration_seconds = parse_duration_seconds(
                entry.get("itunes_duration") or entry.get("duration")
            )

            episodes.append(
                EpisodeRecord(
                    guid=_derive_guid(
                        entry,
                        audio_url=audio_url,
                        episode_url=episode_url,
                        published_at=published_at,
                    ),
                    title=(entry.get("title") or "").strip() or "Untitled episode",
                    published_at=published_at,
                    audio_url=audio_url,
                    episode_url=episode_url,
                    duration_seconds=duration_seconds,
                    summary=_extract_summary(entry),
                    has_transcript_tag=bool(
                        transcript_item.has_transcript_tag if transcript_item else False
                    ),
                    transcript_url=transcript_item.transcript_url if transcript_item else None,
                )
            )

        return ParsedFeed(
            show=FeedShowMetadata(
                title=feed_title,
                feed_url=str(response.url),
                site_url=(
                    parsed.feed.get("link") or metadata.get("site_url") or ""
                ).strip()
                or None,
                language=(
                    parsed.feed.get("language") or metadata.get("language") or ""
                ).strip()
                or None,
                description=(
                    parsed.feed.get("subtitle")
                    or parsed.feed.get("description")
                    or metadata.get("description")
                    or ""
                ).strip()
                or None,
            ),
            episodes=episodes,
        )
