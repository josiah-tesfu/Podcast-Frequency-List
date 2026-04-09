import hashlib

import httpx
import pytest

from podcast_frequency_list.db import bootstrap_database, connect
from podcast_frequency_list.discovery.feed_verifier import FeedVerificationError, FeedVerifier
from podcast_frequency_list.discovery.models import PodcastCandidate
from podcast_frequency_list.discovery.podcast_index import PodcastIndexClient, rank_candidates
from podcast_frequency_list.discovery.service import ShowDiscoveryService


def test_podcast_index_client_sends_expected_auth_headers() -> None:
    expected_timestamp = "1700000000"
    expected_authorization = hashlib.sha1(b"keysecret1700000000").hexdigest()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Auth-Key"] == "key"
        assert request.headers["X-Auth-Date"] == expected_timestamp
        assert request.headers["Authorization"] == expected_authorization
        return httpx.Response(200, json={"status": "true", "feeds": []})

    client = PodcastIndexClient(
        api_key="key",
        api_secret="secret",
        transport=httpx.MockTransport(handler),
        time_provider=lambda: 1700000000,
    )

    try:
        client.search_by_title("InnerFrench")
    finally:
        client.close()


def test_rank_candidates_prefers_exact_french_match() -> None:
    candidates = [
        PodcastCandidate(
            podcast_index_id=1,
            title="InnerFrench",
            feed_url="https://example.com/inner.xml",
            language="fr",
        ),
        PodcastCandidate(
            podcast_index_id=2,
            title="InnerFrench Podcast",
            feed_url="https://example.com/other.xml",
            language="en",
        ),
    ]

    ranked = rank_candidates("InnerFrench", candidates)

    assert ranked[0].podcast_index_id == 1
    assert ranked[0].score > ranked[1].score


def test_feed_verifier_accepts_matching_rss_feed() -> None:
    feed_xml = """
    <rss version="2.0">
      <channel>
        <title>InnerFrench</title>
        <link>https://example.com</link>
        <description>French listening practice</description>
        <language>fr</language>
      </channel>
    </rss>
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=feed_xml,
            headers={"content-type": "application/rss+xml"},
        )

    verifier = FeedVerifier(transport=httpx.MockTransport(handler))

    try:
        verified_feed = verifier.verify(
            "https://example.com/feed.xml",
            expected_title="InnerFrench",
        )
    finally:
        verifier.close()

    assert verified_feed.feed_url == "https://example.com/feed.xml"
    assert verified_feed.feed_title == "InnerFrench"
    assert verified_feed.site_url == "https://example.com"
    assert verified_feed.description == "French listening practice"
    assert verified_feed.language == "fr"


def test_feed_verifier_rejects_mismatched_title() -> None:
    feed_xml = """
    <rss version="2.0">
      <channel>
        <title>Completely Different Show</title>
      </channel>
    </rss>
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=feed_xml,
            headers={"content-type": "application/rss+xml"},
        )

    verifier = FeedVerifier(transport=httpx.MockTransport(handler))

    try:
        with pytest.raises(FeedVerificationError):
            verifier.verify("https://example.com/feed.xml", expected_title="InnerFrench")
    finally:
        verifier.close()


def test_manual_feed_save_persists_show_metadata(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    feed_xml = """
    <rss version="2.0">
      <channel>
        <title>Zack en roue libre</title>
        <link>https://zack.example.com</link>
        <description>desc</description>
        <language>fr</language>
      </channel>
    </rss>
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=feed_xml,
            headers={"content-type": "application/rss+xml"},
        )

    verifier = FeedVerifier(transport=httpx.MockTransport(handler))
    service = ShowDiscoveryService(
        db_path=db_path,
        podcast_index_client=None,
        feed_verifier=verifier,
    )

    try:
        saved_show = service.save_manual_feed(
            feed_url="https://example.com/zack.xml",
            bucket="native",
        )
    finally:
        service.close()

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT title, feed_url, site_url, language, bucket, description
            FROM shows
            WHERE show_id = ?
            """,
            (saved_show.show_id,),
        ).fetchone()

    assert saved_show.title == "Zack en roue libre"
    assert row["title"] == "Zack en roue libre"
    assert row["feed_url"] == "https://example.com/zack.xml"
    assert row["site_url"] == "https://zack.example.com"
    assert row["language"] == "fr"
    assert row["bucket"] == "native"
    assert row["description"] == "desc"
