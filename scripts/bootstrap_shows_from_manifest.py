from __future__ import annotations

from podcast_frequency_list.cli.output import emit_fields, emit_record
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.discovery import DEFAULT_USER_AGENT
from podcast_frequency_list.discovery.feed_verifier import FeedVerifier
from podcast_frequency_list.discovery.service import ShowDiscoveryService
from podcast_frequency_list.ingest import RssFeedClient
from podcast_frequency_list.ingest.service import SyncFeedService
from podcast_frequency_list.show_bootstrap import ShowBootstrapService


def main() -> None:
    settings = load_settings()
    service = ShowBootstrapService(
        show_discovery_service=ShowDiscoveryService(
            db_path=settings.db_path,
            feed_verifier=FeedVerifier(user_agent=DEFAULT_USER_AGENT),
        ),
        sync_feed_service=SyncFeedService(
            db_path=settings.db_path,
            rss_feed_client=RssFeedClient(user_agent=DEFAULT_USER_AGENT),
        ),
    )
    try:
        result = service.bootstrap_manifest()
    finally:
        service.close()

    emit_fields(
        (
            ("selected_shows", result.selected_shows),
            ("bootstrapped_shows", result.bootstrapped_shows),
            ("episodes_seen", result.episodes_seen),
            ("episodes_inserted", result.episodes_inserted),
            ("episodes_updated", result.episodes_updated),
            ("episodes_skipped_no_audio", result.episodes_skipped_no_audio),
            ("episodes_with_transcript_tag", result.episodes_with_transcript_tag),
        )
    )
    for row in result.rows:
        emit_record(
            (
                ("record", "show_bootstrap"),
                ("slug", row.slug),
                ("show_id", row.show_id),
                ("title", row.title),
                ("feed_url", row.feed_url),
                ("episodes_seen", row.episodes_seen),
                ("episodes_inserted", row.episodes_inserted),
                ("episodes_updated", row.episodes_updated),
                ("episodes_skipped_no_audio", row.episodes_skipped_no_audio),
                ("episodes_with_transcript_tag", row.episodes_with_transcript_tag),
            )
        )


if __name__ == "__main__":
    main()
