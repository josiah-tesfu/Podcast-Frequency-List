from __future__ import annotations

from difflib import SequenceMatcher
from xml.etree import ElementTree

import httpx

from podcast_frequency_list.discovery.common import DEFAULT_USER_AGENT, normalize_text
from podcast_frequency_list.discovery.models import VerifiedFeed
from podcast_frequency_list.feed_parsing import extract_feed_metadata, fetch_feed_document


class FeedVerificationError(RuntimeError):
    pass


def extract_feed_title(document: str) -> str | None:
    return extract_feed_metadata(document).get("title")


def titles_roughly_match(expected_title: str, actual_title: str) -> bool:
    normalized_expected = normalize_text(expected_title)
    normalized_actual = normalize_text(actual_title)

    if not normalized_expected or not normalized_actual:
        return False
    if normalized_expected == normalized_actual:
        return True
    if normalized_expected in normalized_actual or normalized_actual in normalized_expected:
        return True

    similarity = SequenceMatcher(None, normalized_expected, normalized_actual).ratio()
    expected_tokens = set(normalized_expected.split())
    actual_tokens = set(normalized_actual.split())
    overlap = len(expected_tokens & actual_tokens)
    overlap_ratio = overlap / (len(expected_tokens) or 1)

    return similarity >= 0.55 or overlap_ratio >= 0.6


class FeedVerifier:
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

    def inspect(self, feed_url: str) -> VerifiedFeed:
        response, document = fetch_feed_document(
            self._client,
            feed_url=feed_url,
            error_type=FeedVerificationError,
        )

        try:
            metadata = extract_feed_metadata(document)
        except ElementTree.ParseError as exc:
            raise FeedVerificationError(f"feed XML could not be parsed: {exc}") from exc
        except ValueError as exc:
            raise FeedVerificationError(str(exc)) from exc

        feed_title = metadata.get("title")
        if not feed_title:
            raise FeedVerificationError("feed title could not be determined")

        return VerifiedFeed(
            feed_url=str(response.url),
            feed_title=feed_title,
            content_type=response.headers.get("content-type"),
            site_url=metadata.get("site_url"),
            language=metadata.get("language"),
            description=metadata.get("description"),
        )

    def verify(self, feed_url: str, *, expected_title: str) -> VerifiedFeed:
        verified_feed = self.inspect(feed_url)
        if not titles_roughly_match(expected_title, verified_feed.feed_title):
            raise FeedVerificationError(
                f"feed title mismatch: expected '{expected_title}' got '{verified_feed.feed_title}'"
            )

        return verified_feed
