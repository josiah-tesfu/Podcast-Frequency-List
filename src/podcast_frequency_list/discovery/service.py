from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from podcast_frequency_list.db import connect, upsert_show
from podcast_frequency_list.discovery.feed_verifier import FeedVerifier
from podcast_frequency_list.discovery.models import PodcastCandidate, SavedShow
from podcast_frequency_list.discovery.podcast_index import PodcastIndexClient, rank_candidates


class ShowDiscoveryService:
    def __init__(
        self,
        *,
        db_path: Path,
        podcast_index_client: PodcastIndexClient | None,
        feed_verifier: FeedVerifier,
    ) -> None:
        self.db_path = db_path
        self.podcast_index_client = podcast_index_client
        self.feed_verifier = feed_verifier

    def close(self) -> None:
        if self.podcast_index_client is not None:
            self.podcast_index_client.close()
        self.feed_verifier.close()

    def search(self, query: str, *, limit: int = 5) -> list[PodcastCandidate]:
        if self.podcast_index_client is None:
            raise RuntimeError("Podcast Index discovery is not configured")

        search_limit = max(limit * 2, 10)
        candidates = [
            *self.podcast_index_client.search_by_title(query, max_results=search_limit),
            *self.podcast_index_client.search_by_term(query, max_results=search_limit),
        ]
        return rank_candidates(query, candidates)[:limit]

    def save_selected_candidate(self, candidate: PodcastCandidate) -> SavedShow:
        if self.podcast_index_client is None:
            raise RuntimeError("Podcast Index discovery is not configured")

        if candidate.podcast_index_id is not None:
            detailed_candidate = self.podcast_index_client.get_podcast_by_feed_id(
                candidate.podcast_index_id
            )
        else:
            detailed_candidate = self.podcast_index_client.get_podcast_by_feed_url(
                candidate.feed_url
            )

        verified_feed = self.feed_verifier.verify(
            detailed_candidate.feed_url,
            expected_title=detailed_candidate.title,
        )
        verified_candidate = replace(
            detailed_candidate,
            feed_url=verified_feed.feed_url,
            title=detailed_candidate.title or verified_feed.feed_title,
        )

        with connect(self.db_path) as connection:
            show_id = upsert_show(
                connection,
                podcast_index_id=verified_candidate.podcast_index_id,
                title=verified_candidate.title,
                feed_url=verified_candidate.feed_url,
                site_url=verified_candidate.site_url,
                language=verified_candidate.language,
                description=verified_candidate.description,
            )
            connection.commit()

        return SavedShow(
            show_id=show_id,
            title=verified_candidate.title,
            feed_url=verified_candidate.feed_url,
        )

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
                podcast_index_id=None,
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
