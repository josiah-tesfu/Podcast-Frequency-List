from pathlib import Path

import httpx

from podcast_frequency_list.db import bootstrap_database, connect
from podcast_frequency_list.discovery.feed_verifier import FeedVerifier
from podcast_frequency_list.discovery.service import ShowDiscoveryService
from podcast_frequency_list.ingest import RssFeedClient, SyncFeedService
from podcast_frequency_list.show_bootstrap import ShowBootstrapService

FEED_ONE = """
<rss version="2.0"
     xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
     xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Zack en Roue Libre</title>
    <link>https://example.com/zack</link>
    <description>Zack desc</description>
    <language>fr</language>
    <item>
      <guid>z-1</guid>
      <title>Episode Zack 1</title>
      <link>https://example.com/zack/1</link>
      <pubDate>Wed, 01 Jan 2025 12:00:00 GMT</pubDate>
      <description>Summary 1</description>
      <enclosure url="https://cdn.example.com/zack-1.mp3" type="audio/mpeg"/>
      <itunes:duration>1800</itunes:duration>
      <podcast:transcript url="https://example.com/zack/1.vtt" type="text/vtt"/>
    </item>
    <item>
      <guid>z-2</guid>
      <title>Episode Zack 2</title>
      <link>https://example.com/zack/2</link>
      <pubDate>Thu, 02 Jan 2025 12:00:00 GMT</pubDate>
      <description>Summary 2</description>
      <enclosure url="https://cdn.example.com/zack-2.mp3" type="audio/mpeg"/>
      <itunes:duration>1200</itunes:duration>
    </item>
  </channel>
</rss>
"""

FEED_TWO = """
<rss version="2.0"
     xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>FloodCast</title>
    <link>https://example.com/floodcast</link>
    <description>Flood desc</description>
    <language>fr</language>
    <item>
      <guid>f-1</guid>
      <title>Episode Flood 1</title>
      <link>https://example.com/flood/1</link>
      <pubDate>Fri, 03 Jan 2025 12:00:00 GMT</pubDate>
      <description>Summary 1</description>
      <enclosure url="https://cdn.example.com/flood-1.mp3" type="audio/mpeg"/>
      <itunes:duration>2400</itunes:duration>
    </item>
    <item>
      <guid>f-2</guid>
      <title>Episode Flood 2</title>
      <link>https://example.com/flood/2</link>
      <pubDate>Sat, 04 Jan 2025 12:00:00 GMT</pubDate>
      <description>Summary 2</description>
      <itunes:duration>600</itunes:duration>
    </item>
  </channel>
</rss>
"""


def _build_manifest(tmp_path: Path) -> Path:
    manifest_path = tmp_path / "show_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "slug,title,feed_url,language,bucket,family,target_hours,selection_order,enabled,notes",
                "zack,Zack en Roue Libre,https://example.com/zack.xml,fr,native,test,10,newest,1,one",
                "flood,FloodCast,https://example.com/flood.xml,fr,native,test,10,newest,1,two",
            ]
        ),
        encoding="utf-8",
    )
    return manifest_path


def _transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == "https://example.com/zack.xml":
            return httpx.Response(
                200,
                text=FEED_ONE,
                headers={"content-type": "application/rss+xml"},
            )
        if str(request.url) == "https://example.com/flood.xml":
            return httpx.Response(
                200,
                text=FEED_TWO,
                headers={"content-type": "application/rss+xml"},
            )
        raise AssertionError(f"unexpected URL: {request.url}")

    return httpx.MockTransport(handler)


def test_show_bootstrap_service_bootstraps_manifest_and_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    manifest_path = _build_manifest(tmp_path)

    service = ShowBootstrapService(
        show_discovery_service=ShowDiscoveryService(
            db_path=db_path,
            feed_verifier=FeedVerifier(transport=_transport()),
        ),
        sync_feed_service=SyncFeedService(
            db_path=db_path,
            rss_feed_client=RssFeedClient(transport=_transport()),
        ),
    )

    try:
        first_result = service.bootstrap_manifest(manifest_path=manifest_path)
        second_result = service.bootstrap_manifest(manifest_path=manifest_path)
    finally:
        service.close()

    with connect(db_path) as connection:
        show_count = connection.execute("SELECT COUNT(*) FROM shows").fetchone()[0]
        episode_count = connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]

    assert first_result.selected_shows == 2
    assert first_result.bootstrapped_shows == 2
    assert first_result.episodes_seen == 4
    assert first_result.episodes_inserted == 3
    assert first_result.episodes_updated == 0
    assert first_result.episodes_skipped_no_audio == 1
    assert first_result.episodes_with_transcript_tag == 1

    assert second_result.selected_shows == 2
    assert second_result.bootstrapped_shows == 2
    assert second_result.episodes_seen == 4
    assert second_result.episodes_inserted == 0
    assert second_result.episodes_updated == 3
    assert second_result.episodes_skipped_no_audio == 1
    assert second_result.episodes_with_transcript_tag == 1

    assert show_count == 2
    assert episode_count == 3
    assert [row.slug for row in first_result.rows] == ["zack", "flood"]
