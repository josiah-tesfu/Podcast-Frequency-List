from __future__ import annotations

from podcast_frequency_list.asr import (
    AsrRunService,
    AudioChunker,
    AudioDownloader,
    OpenAITranscriber,
)
from podcast_frequency_list.asr.audio import DEFAULT_SAFE_UPLOAD_BYTES
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.discovery import DEFAULT_USER_AGENT, ShowDiscoveryService
from podcast_frequency_list.discovery.feed_verifier import FeedVerifier
from podcast_frequency_list.ingest import RssFeedClient, SyncFeedService
from podcast_frequency_list.normalize import TranscriptNormalizationService
from podcast_frequency_list.pilot import PilotSelectionService
from podcast_frequency_list.qc import SegmentQcService
from podcast_frequency_list.sentences import SentenceSplitService
from podcast_frequency_list.tokens import (
    CandidateInventoryService,
    CandidateMetricsService,
    SentenceTokenizationService,
)


def build_manual_discovery_service() -> ShowDiscoveryService:
    settings = load_settings()
    return ShowDiscoveryService(
        db_path=settings.db_path,
        feed_verifier=FeedVerifier(user_agent=DEFAULT_USER_AGENT),
    )


def build_sync_feed_service() -> SyncFeedService:
    settings = load_settings()
    return SyncFeedService(
        db_path=settings.db_path,
        rss_feed_client=RssFeedClient(user_agent=DEFAULT_USER_AGENT),
    )


def build_pilot_selection_service() -> PilotSelectionService:
    settings = load_settings()
    return PilotSelectionService(db_path=settings.db_path)


def build_asr_run_service() -> AsrRunService:
    settings = load_settings()
    transcriber = OpenAITranscriber(
        api_key=settings.openai_api_key,
        model=settings.asr_model,
    )
    return AsrRunService(
        db_path=settings.db_path,
        raw_data_dir=settings.raw_data_dir,
        audio_downloader=AudioDownloader(audio_dir=settings.raw_data_dir / "audio"),
        audio_chunker=AudioChunker(
            chunk_dir=settings.raw_data_dir / "audio_chunks",
            max_upload_bytes=DEFAULT_SAFE_UPLOAD_BYTES,
        ),
        transcriber=transcriber,
    )


def build_transcript_normalization_service() -> TranscriptNormalizationService:
    settings = load_settings()
    return TranscriptNormalizationService(db_path=settings.db_path)


def build_segment_qc_service() -> SegmentQcService:
    settings = load_settings()
    return SegmentQcService(db_path=settings.db_path)


def build_sentence_split_service() -> SentenceSplitService:
    settings = load_settings()
    return SentenceSplitService(db_path=settings.db_path)


def build_sentence_tokenization_service() -> SentenceTokenizationService:
    settings = load_settings()
    return SentenceTokenizationService(db_path=settings.db_path)


def build_candidate_inventory_service() -> CandidateInventoryService:
    settings = load_settings()
    return CandidateInventoryService(db_path=settings.db_path)


def build_candidate_metrics_service() -> CandidateMetricsService:
    settings = load_settings()
    return CandidateMetricsService(db_path=settings.db_path)
