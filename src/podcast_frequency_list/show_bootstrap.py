from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.discovery.service import ShowDiscoveryService
from podcast_frequency_list.ingest.service import SyncFeedService
from podcast_frequency_list.show_manifest import load_show_manifest


class ShowBootstrapError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShowBootstrapRow:
    slug: str
    show_id: int
    title: str
    feed_url: str
    episodes_seen: int
    episodes_inserted: int
    episodes_updated: int
    episodes_skipped_no_audio: int
    episodes_with_transcript_tag: int


@dataclass(frozen=True)
class ShowBootstrapResult:
    selected_shows: int
    bootstrapped_shows: int
    episodes_seen: int
    episodes_inserted: int
    episodes_updated: int
    episodes_skipped_no_audio: int
    episodes_with_transcript_tag: int
    rows: tuple[ShowBootstrapRow, ...]


class ShowBootstrapService:
    def __init__(
        self,
        *,
        show_discovery_service: ShowDiscoveryService,
        sync_feed_service: SyncFeedService,
    ) -> None:
        self.show_discovery_service = show_discovery_service
        self.sync_feed_service = sync_feed_service

    def close(self) -> None:
        self.show_discovery_service.close()
        self.sync_feed_service.close()

    def bootstrap_manifest(self, *, manifest_path: Path | None = None) -> ShowBootstrapResult:
        manifest_rows = tuple(row for row in load_show_manifest(manifest_path) if row.enabled)
        if not manifest_rows:
            raise ShowBootstrapError("no enabled shows found in manifest")

        rows: list[ShowBootstrapRow] = []
        for manifest_row in manifest_rows:
            saved_show = self.show_discovery_service.save_manual_feed(
                feed_url=manifest_row.feed_url,
                title=manifest_row.title,
                language=manifest_row.language,
                bucket=manifest_row.bucket,
            )
            sync_result = self.sync_feed_service.sync_show(show_id=saved_show.show_id)
            rows.append(
                ShowBootstrapRow(
                    slug=manifest_row.slug,
                    show_id=saved_show.show_id,
                    title=saved_show.title,
                    feed_url=saved_show.feed_url,
                    episodes_seen=sync_result.episodes_seen,
                    episodes_inserted=sync_result.episodes_inserted,
                    episodes_updated=sync_result.episodes_updated,
                    episodes_skipped_no_audio=sync_result.episodes_skipped_no_audio,
                    episodes_with_transcript_tag=sync_result.episodes_with_transcript_tag,
                )
            )

        return ShowBootstrapResult(
            selected_shows=len(manifest_rows),
            bootstrapped_shows=len(rows),
            episodes_seen=sum(row.episodes_seen for row in rows),
            episodes_inserted=sum(row.episodes_inserted for row in rows),
            episodes_updated=sum(row.episodes_updated for row in rows),
            episodes_skipped_no_audio=sum(row.episodes_skipped_no_audio for row in rows),
            episodes_with_transcript_tag=sum(row.episodes_with_transcript_tag for row in rows),
            rows=tuple(rows),
        )
