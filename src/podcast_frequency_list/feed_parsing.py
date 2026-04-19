from __future__ import annotations

from dataclasses import dataclass
from xml.etree import ElementTree

import httpx


@dataclass(frozen=True)
class TranscriptTag:
    has_transcript_tag: bool
    transcript_url: str | None


def fetch_feed_document(
    client: httpx.Client,
    *,
    feed_url: str,
    error_type: type[Exception],
) -> tuple[httpx.Response, str]:
    try:
        response = client.get(feed_url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise error_type(f"feed request failed: {exc}") from exc

    document = response.text.lstrip("\ufeff").strip()
    if not document:
        raise error_type("feed returned an empty response body")

    return response, document


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


def extract_transcript_tags(document: str) -> tuple[TranscriptTag, ...]:
    try:
        root = ElementTree.fromstring(document)
    except ElementTree.ParseError:
        return ()

    transcript_items: list[TranscriptTag] = []
    for element in _iter_entry_elements(root):
        transcript_url: str | None = None

        for child in element:
            child_name = _strip_namespace(child.tag).lower()
            if child_name == "transcript":
                transcript_url = (child.attrib.get("url") or "").strip() or None
                if transcript_url is None and child.text:
                    transcript_url = child.text.strip() or None
                break

            if child_name == "link" and child.attrib.get("rel", "").lower() == "transcript":
                transcript_url = (child.attrib.get("href") or "").strip() or None
                break

        transcript_items.append(
            TranscriptTag(
                has_transcript_tag=transcript_url is not None,
                transcript_url=transcript_url,
            )
        )

    return tuple(transcript_items)


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
            raise ValueError("rss feed is missing channel metadata")
        return channel

    if root_name in {"rdf", "rdf:rdf"}:
        for element in root:
            if _strip_namespace(element.tag).lower() == "channel":
                return element
        raise ValueError("rdf feed is missing channel metadata")

    if root_name == "feed":
        return root

    raise ValueError(f"unsupported feed root element: {root.tag}")


def _iter_entry_elements(root: ElementTree.Element) -> list[ElementTree.Element]:
    root_name = _strip_namespace(root.tag).lower()

    if root_name == "rss":
        channel = root.find("channel")
        if channel is None:
            return []
        return [child for child in channel if _strip_namespace(child.tag).lower() == "item"]

    if root_name == "feed":
        return [child for child in root if _strip_namespace(child.tag).lower() == "entry"]

    return [child for child in root if _strip_namespace(child.tag).lower() == "item"]
