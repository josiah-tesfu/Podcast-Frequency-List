from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import replace
from difflib import SequenceMatcher
from time import time
from typing import Any

import httpx

from podcast_frequency_list.discovery.models import PodcastCandidate

DEFAULT_PODCAST_INDEX_BASE_URL = "https://api.podcastindex.org/api/1.0"
DEFAULT_USER_AGENT = "podcast-frequency-list/0.1.0"

# Kept as a fallback. The active workflow currently uses manual feed URLs.

class PodcastIndexError(RuntimeError):
    pass


class PodcastIndexCredentialsError(PodcastIndexError):
    pass


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", ascii_value.lower()).strip()


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return False


def score_candidate(query: str, candidate: PodcastCandidate) -> float:
    normalized_query = normalize_text(query)
    normalized_title = normalize_text(candidate.title)

    if not normalized_query or not normalized_title:
        return -100.0

    similarity = SequenceMatcher(None, normalized_query, normalized_title).ratio()
    query_tokens = set(normalized_query.split())
    title_tokens = set(normalized_title.split())
    overlap = len(query_tokens & title_tokens)
    union = len(query_tokens | title_tokens) or 1
    token_score = overlap / union

    score = (similarity * 100) + (token_score * 40)

    if normalized_title == normalized_query:
        score += 100
    elif normalized_query in normalized_title:
        score += 25

    if candidate.language and candidate.language.lower().startswith("fr"):
        score += 15
    if candidate.feed_url:
        score += 5
    if candidate.site_url:
        score += 3
    if candidate.dead:
        score -= 50

    return score


def rank_candidates(query: str, candidates: list[PodcastCandidate]) -> list[PodcastCandidate]:
    deduped: dict[str, PodcastCandidate] = {}

    for candidate in candidates:
        if not candidate.title or not candidate.feed_url:
            continue

        scored_candidate = replace(candidate, score=score_candidate(query, candidate))
        dedupe_key = (
            f"id:{candidate.podcast_index_id}"
            if candidate.podcast_index_id is not None
            else f"url:{candidate.feed_url}"
        )

        existing = deduped.get(dedupe_key)
        if existing is None or scored_candidate.score > existing.score:
            deduped[dedupe_key] = scored_candidate

    return sorted(
        deduped.values(),
        key=lambda candidate: (-candidate.score, normalize_text(candidate.title)),
    )


class PodcastIndexClient:
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        user_agent: str = DEFAULT_USER_AGENT,
        base_url: str = DEFAULT_PODCAST_INDEX_BASE_URL,
        timeout: float = 20.0,
        transport: httpx.BaseTransport | None = None,
        time_provider: callable | None = None,
    ) -> None:
        if not api_key or not api_secret:
            raise PodcastIndexCredentialsError("missing Podcast Index credentials")

        self.api_key = api_key
        self.api_secret = api_secret
        self.user_agent = user_agent
        self.time_provider = time_provider or time
        self._client = httpx.Client(
            base_url=base_url,
            follow_redirects=True,
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def search_by_title(self, query: str, *, max_results: int = 10) -> list[PodcastCandidate]:
        payload = self._request_json("/search/bytitle", {"q": query, "max": max_results})
        return self._parse_feed_items(payload.get("feeds", []), source="search/bytitle")

    def search_by_term(self, query: str, *, max_results: int = 10) -> list[PodcastCandidate]:
        payload = self._request_json("/search/byterm", {"q": query, "max": max_results})
        return self._parse_feed_items(payload.get("feeds", []), source="search/byterm")

    def get_podcast_by_feed_id(self, feed_id: int) -> PodcastCandidate:
        payload = self._request_json("/podcasts/byfeedid", {"id": feed_id})
        return self._parse_detail(payload, source="podcasts/byfeedid")

    def get_podcast_by_feed_url(self, feed_url: str) -> PodcastCandidate:
        payload = self._request_json("/podcasts/byfeedurl", {"url": feed_url})
        return self._parse_detail(payload, source="podcasts/byfeedurl")

    def _request_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        timestamp = str(int(self.time_provider()))
        token = f"{self.api_key}{self.api_secret}{timestamp}"
        authorization = hashlib.sha1(token.encode()).hexdigest()
        headers = {
            "User-Agent": self.user_agent,
            "X-Auth-Key": self.api_key,
            "X-Auth-Date": timestamp,
            "Authorization": authorization,
        }

        response = self._client.get(path, params=params, headers=headers)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise PodcastIndexError(f"Podcast Index request failed: {exc}") from exc

        payload = response.json()
        if str(payload.get("status", "")).lower() == "false":
            message = payload.get("description") or payload.get("message") or "unknown error"
            raise PodcastIndexError(f"Podcast Index request failed: {message}")

        return payload

    def _parse_feed_items(
        self,
        items: list[dict[str, Any]],
        *,
        source: str,
    ) -> list[PodcastCandidate]:
        return [self._parse_feed_item(item, source=source) for item in items]

    def _parse_detail(self, payload: dict[str, Any], *, source: str) -> PodcastCandidate:
        item = payload.get("feed")
        if item is None:
            feeds = payload.get("feeds", [])
            if not feeds:
                raise PodcastIndexError("Podcast Index returned no feed details")
            item = feeds[0]
        return self._parse_feed_item(item, source=source)

    def _parse_feed_item(self, item: dict[str, Any], *, source: str) -> PodcastCandidate:
        return PodcastCandidate(
            podcast_index_id=_coerce_int(item.get("id") or item.get("feedId")),
            title=(item.get("title") or item.get("titleOriginal") or "").strip(),
            feed_url=(
                item.get("url") or item.get("feedUrl") or item.get("originalUrl") or ""
            ).strip(),
            site_url=(item.get("link") or item.get("website") or item.get("siteUrl") or "").strip()
            or None,
            author=(item.get("author") or item.get("ownerName") or "").strip() or None,
            language=(item.get("language") or "").strip() or None,
            description=(item.get("description") or item.get("descriptionOriginal") or "").strip()
            or None,
            dead=_coerce_bool(item.get("dead")),
            source=source,
        )
