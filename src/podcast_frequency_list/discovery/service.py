from __future__ import annotations

from pathlib import Path

from podcast_frequency_list.db import connect, upsert_show
from podcast_frequency_list.discovery.feed_verifier import FeedVerifier
from podcast_frequency_list.discovery.models import SavedShow


class ShowDiscoveryService:
    def __init__(
        self,
        *,
        db_path: Path,
        feed_verifier: FeedVerifier,
    ) -> None:
        self.db_path = db_path
        self.feed_verifier = feed_verifier

    def close(self) -> None:
        self.feed_verifier.close()

    def save_manual_feed(
        self,
        *,
        feed_url: str,
        title: str | None = None,
        language: str | None = None,
        bucket: str | None = None,
        site_url: str | None = None,
        description: str | None = None,
    ) -> SavedShow:
        verified_feed = self.feed_verifier.inspect(feed_url)
        final_title = title or verified_feed.feed_title

        with connect(self.db_path) as connection:
            show_id = upsert_show(
                connection,
                title=final_title,
                feed_url=verified_feed.feed_url,
                site_url=site_url or verified_feed.site_url,
                language=language or verified_feed.language,
                bucket=bucket,
                description=description or verified_feed.description,
            )
            connection.commit()

        return SavedShow(
            show_id=show_id,
            title=final_title,
            feed_url=verified_feed.feed_url,
        )
