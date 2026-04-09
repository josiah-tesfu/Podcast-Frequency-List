from __future__ import annotations

from difflib import SequenceMatcher
from xml.etree import ElementTree

import httpx

from podcast_frequency_list.discovery.models import VerifiedFeed
from podcast_frequency_list.discovery.podcast_index import DEFAULT_USER_AGENT, normalize_text


class FeedVerificationError(RuntimeError):
    pass


def _strip_namespace(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _extract_child_text(element: ElementTree.Element, child_name: str) -> str | None:
    for child in element:
        if _strip_namespace(child.tag).lower() == child_name and child.text:
            return child.text.strip()
    return None


def _find_child(element: ElementTree.Element, child_name: str) -> ElementTree.Element | None:
    for child in element:
        if _strip_namespace(child.tag).lower() == child_name:
            return child
    return None


def _extract_atom_link(element: ElementTree.Element) -> str | None:
    for child in element:
        if _strip_namespace(child.tag).lower() != "link":
            continue

        href = child.attrib.get("href")
        rel = child.attrib.get("rel", "alternate")
        if href and rel in {"alternate", "self"}:
            return href.strip()

    return None


def _get_feed_container(root: ElementTree.Element) -> ElementTree.Element:
    root_name = _strip_namespace(root.tag).lower()

    if root_name == "rss":
        channel = root.find("channel")
        if channel is None:
            raise FeedVerificationError("rss feed is missing channel metadata")
        return channel

    if root_name in {"rdf", "rdf:rdf"}:
        for element in root:
            if _strip_namespace(element.tag).lower() == "channel":
                return element
        raise FeedVerificationError("rdf feed is missing channel metadata")

    if root_name == "feed":
        return root

    raise FeedVerificationError(f"unsupported feed root element: {root.tag}")


def extract_feed_metadata(document: str) -> dict[str, str | None]:
    root = ElementTree.fromstring(document)
    container = _get_feed_container(root)
    root_name = _strip_namespace(root.tag).lower()

    if root_name == "feed":
        title = _extract_child_text(container, "title")
        site_url = _extract_atom_link(container)
        description = _extract_child_text(container, "subtitle")
        language = container.attrib.get(
            "{http://www.w3.org/XML/1998/namespace}lang"
        ) or container.attrib.get("lang")
    else:
        title = _extract_child_text(container, "title")
        site_url = _extract_child_text(container, "link")
        description = _extract_child_text(container, "description")
        language = _extract_child_text(container, "language")

    if not title:
        title = _extract_child_text(container, "itunes:title")
    if not description:
        summary = _find_child(container, "summary")
        description = summary.text.strip() if summary is not None and summary.text else None

    return {
        "title": title,
        "site_url": site_url,
        "description": description,
        "language": language,
    }


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
        try:
            response = self._client.get(feed_url)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise FeedVerificationError(f"feed request failed: {exc}") from exc

        document = response.text.lstrip("\ufeff").strip()
        if not document:
            raise FeedVerificationError("feed returned an empty response body")

        try:
            metadata = extract_feed_metadata(document)
        except ElementTree.ParseError as exc:
            raise FeedVerificationError(f"feed XML could not be parsed: {exc}") from exc

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
