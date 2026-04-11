from __future__ import annotations

from pathlib import Path

from podcast_frequency_list.db import connect, get_show_by_id, update_show, upsert_episode
from podcast_frequency_list.ingest.models import SyncFeedResult
from podcast_frequency_list.ingest.rss import RssFeedClient


class SyncFeedError(RuntimeError):
    pass


class SyncFeedService:
    def __init__(self, *, db_path: Path, rss_feed_client: RssFeedClient) -> None:
        self.db_path = db_path
        self.rss_feed_client = rss_feed_client

    def close(self) -> None:
        self.rss_feed_client.close()

    def sync_show(self, *, show_id: int, limit: int | None = None) -> SyncFeedResult:
        with connect(self.db_path) as connection:
            show_row = get_show_by_id(connection, show_id)
            if show_row is None:
                raise SyncFeedError(f"show_id={show_id} was not found")

            parsed_feed = self.rss_feed_client.parse_feed(show_row["feed_url"], limit=limit)

            update_show(
                connection,
                show_id=show_id,
                title=show_row["title"],
                feed_url=parsed_feed.show.feed_url,
                site_url=show_row["site_url"] or parsed_feed.show.site_url,
                language=show_row["language"] or parsed_feed.show.language,
                bucket=show_row["bucket"],
                description=show_row["description"] or parsed_feed.show.description,
            )

            inserted = 0
            updated = 0
            skipped_no_audio = 0
            with_transcript_tag = 0

            for episode in parsed_feed.episodes:
                if not episode.audio_url:
                    skipped_no_audio += 1
                    continue

                was_inserted = upsert_episode(
                    connection,
                    show_id=show_id,
                    guid=episode.guid,
                    title=episode.title,
                    published_at=episode.published_at,
                    audio_url=episode.audio_url,
                    episode_url=episode.episode_url,
                    duration_seconds=episode.duration_seconds,
                    summary=episode.summary,
                    has_transcript_tag=episode.has_transcript_tag,
                    transcript_url=episode.transcript_url,
                )
                if was_inserted:
                    inserted += 1
                else:
                    updated += 1

                if episode.has_transcript_tag:
                    with_transcript_tag += 1

            connection.commit()

        return SyncFeedResult(
            show_id=show_id,
            title=show_row["title"],
            episodes_seen=len(parsed_feed.episodes),
            episodes_inserted=inserted,
            episodes_updated=updated,
            episodes_skipped_no_audio=skipped_no_audio,
            episodes_with_transcript_tag=with_transcript_tag,
        )
