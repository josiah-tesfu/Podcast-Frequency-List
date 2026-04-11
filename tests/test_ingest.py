import httpx

from podcast_frequency_list.db import bootstrap_database, connect, upsert_show
from podcast_frequency_list.ingest import RssFeedClient, SyncFeedService, parse_duration_seconds

SAMPLE_FEED = """
<rss version="2.0"
     xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
     xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Test Feed</title>
    <link>https://example.com/show</link>
    <description>Feed description</description>
    <language>fr</language>
    <item>
      <guid>ep-1</guid>
      <title>Episode One</title>
      <link>https://example.com/ep1</link>
      <pubDate>Wed, 01 Jan 2025 12:00:00 GMT</pubDate>
      <description>Summary 1</description>
      <enclosure url="https://cdn.example.com/ep1.mp3" type="audio/mpeg"/>
      <itunes:duration>01:02:03</itunes:duration>
      <podcast:transcript url="https://example.com/ep1.vtt" type="text/vtt"/>
    </item>
    <item>
      <title>Episode Two</title>
      <link>https://example.com/ep2</link>
      <pubDate>Thu, 02 Jan 2025 12:00:00 GMT</pubDate>
      <description>Summary 2</description>
      <enclosure url="https://cdn.example.com/ep2.mp3" type="audio/mpeg"/>
      <itunes:duration>75</itunes:duration>
    </item>
    <item>
      <guid>ep-3</guid>
      <title>Episode Three</title>
      <pubDate>Fri, 03 Jan 2025 12:00:00 GMT</pubDate>
      <description>Summary 3</description>
      <itunes:duration>00:10:00</itunes:duration>
    </item>
  </channel>
</rss>
"""


def _mock_transport(feed_xml: str) -> httpx.MockTransport:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=feed_xml,
            headers={"content-type": "application/rss+xml"},
        )

    return httpx.MockTransport(handler)


def test_parse_duration_seconds() -> None:
    assert parse_duration_seconds("01:02:03") == 3723
    assert parse_duration_seconds("05:30") == 330
    assert parse_duration_seconds("75") == 75
    assert parse_duration_seconds(None) is None


def test_rss_feed_client_parses_feed_entries() -> None:
    client = RssFeedClient(transport=_mock_transport(SAMPLE_FEED))

    try:
        parsed_feed = client.parse_feed("https://example.com/feed.xml")
    finally:
        client.close()

    assert parsed_feed.show.title == "Test Feed"
    assert parsed_feed.show.site_url == "https://example.com/show"
    assert parsed_feed.show.language == "fr"
    assert len(parsed_feed.episodes) == 3
    assert parsed_feed.episodes[0].guid == "ep-1"
    assert parsed_feed.episodes[0].duration_seconds == 3723
    assert parsed_feed.episodes[0].transcript_url == "https://example.com/ep1.vtt"
    assert parsed_feed.episodes[1].guid == "audio:https://cdn.example.com/ep2.mp3"
    assert parsed_feed.episodes[1].duration_seconds == 75
    assert parsed_feed.episodes[2].audio_url is None


def test_sync_feed_service_upserts_episodes_and_skips_missing_audio(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Manual Title",
            feed_url="https://example.com/feed.xml",
            bucket="native",
        )
        connection.commit()

    service = SyncFeedService(
        db_path=db_path,
        rss_feed_client=RssFeedClient(transport=_mock_transport(SAMPLE_FEED)),
    )

    try:
        first_result = service.sync_show(show_id=show_id)
        second_result = service.sync_show(show_id=show_id)
    finally:
        service.close()

    with connect(db_path) as connection:
        episodes = connection.execute(
            """
            SELECT guid, title, audio_url, duration_seconds, has_transcript_tag, transcript_url
            FROM episodes
            WHERE show_id = ?
            ORDER BY episode_id
            """,
            (show_id,),
        ).fetchall()

    assert first_result.episodes_seen == 3
    assert first_result.episodes_inserted == 2
    assert first_result.episodes_updated == 0
    assert first_result.episodes_skipped_no_audio == 1
    assert first_result.episodes_with_transcript_tag == 1
    assert second_result.episodes_inserted == 0
    assert second_result.episodes_updated == 2
    assert len(episodes) == 2
    assert episodes[0]["guid"] == "ep-1"
    assert episodes[0]["has_transcript_tag"] == 1
    assert episodes[0]["transcript_url"] == "https://example.com/ep1.vtt"
    assert episodes[1]["guid"] == "audio:https://cdn.example.com/ep2.mp3"
