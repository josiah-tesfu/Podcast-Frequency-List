from __future__ import annotations

import typer

from podcast_frequency_list.asr import (
    AsrRunError,
    AsrRunResult,
    AsrRunService,
    AudioChunker,
    AudioDownloader,
    OpenAITranscriber,
)
from podcast_frequency_list.asr.audio import DEFAULT_SAFE_UPLOAD_BYTES
from podcast_frequency_list.asr.client import OpenAITranscriptionError
from podcast_frequency_list.cli_output import (
    emit_asr_result,
    emit_fields,
    emit_normalization_result,
    emit_pilot_result,
    emit_qc_result,
    emit_saved_show,
    emit_sentence_split_result,
    emit_sync_result,
    emit_tokenization_result,
    run_service_command,
)
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.db import bootstrap_database
from podcast_frequency_list.discovery import (
    DEFAULT_USER_AGENT,
    FeedVerificationError,
    ShowDiscoveryService,
)
from podcast_frequency_list.discovery.feed_verifier import FeedVerifier
from podcast_frequency_list.ingest import (
    RssFeedClient,
    RssFeedError,
    SyncFeedError,
    SyncFeedService,
)
from podcast_frequency_list.normalize import (
    TranscriptNormalizationError,
    TranscriptNormalizationService,
)
from podcast_frequency_list.pilot import PilotSelectionError, PilotSelectionService
from podcast_frequency_list.qc import SegmentQcError, SegmentQcService
from podcast_frequency_list.sentences import (
    SentenceSplitError,
    SentenceSplitService,
)
from podcast_frequency_list.tokens import (
    SentenceTokenizationError,
    SentenceTokenizationService,
)

app = typer.Typer(
    add_completion=False,
    help="CLI for building the podcast-based French frequency deck.",
    no_args_is_help=True,
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


@app.command("info")
def info() -> None:
    settings = load_settings()

    emit_fields(
        (
            ("project_root", settings.project_root),
            ("db_path", settings.db_path),
            ("raw_data_dir", settings.raw_data_dir),
            ("processed_data_dir", settings.processed_data_dir),
        )
    )


@app.command("init-db")
def init_db() -> None:
    db_path = bootstrap_database()
    emit_fields((("initialized_db", db_path),))


@app.command("add-show")
def add_show(
    feed_url: str,
    title: str | None = typer.Option(None),
    language: str | None = typer.Option(None),
    bucket: str | None = typer.Option(None),
) -> None:
    bootstrap_database()
    run_service_command(
        build_manual_discovery_service,
        lambda service: emit_saved_show(
            service.save_manual_feed(
                feed_url=feed_url,
                title=title,
                language=language,
                bucket=bucket,
            )
        ),
        FeedVerificationError,
    )


@app.command("sync-feed")
def sync_feed(
    show_id: int = typer.Option(..., "--show-id", min=1),
    limit: int | None = typer.Option(None, min=1),
) -> None:
    bootstrap_database()
    run_service_command(
        build_sync_feed_service,
        lambda service: emit_sync_result(service.sync_show(show_id=show_id, limit=limit)),
        (RssFeedError, SyncFeedError),
    )


@app.command("create-pilot")
def create_pilot(
    show_id: int = typer.Option(..., "--show-id", min=1),
    name: str = typer.Option(..., "--name"),
    hours: float = typer.Option(10.0, "--hours", min=0.1),
    selection_order: str = typer.Option("newest", "--selection-order"),
    notes: str | None = typer.Option(None, "--notes"),
) -> None:
    bootstrap_database()
    run_service_command(
        build_pilot_selection_service,
        lambda service: emit_pilot_result(
            service.create_pilot(
                show_id=show_id,
                name=name,
                target_seconds=round(hours * 3600),
                selection_order=selection_order,
                notes=notes,
            )
        ),
        PilotSelectionError,
    )


@app.command("run-asr")
def run_asr(
    pilot: str = typer.Option(..., "--pilot"),
    limit: int | None = typer.Option(None, "--limit", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()
    result: AsrRunResult | None = None

    def _run(service: AsrRunService) -> None:
        nonlocal result
        result = service.run_pilot(pilot_name=pilot, limit=limit, force=force)
        emit_asr_result(result)

    run_service_command(
        build_asr_run_service,
        _run,
        (OpenAITranscriptionError, AsrRunError),
    )
    if result is not None and result.failed_count:
        raise typer.Exit(code=1)


@app.command("normalize-transcripts")
def normalize_transcripts(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()
    run_service_command(
        build_transcript_normalization_service,
        lambda service: emit_normalization_result(
            service.normalize(
                pilot_name=pilot,
                episode_id=episode_id,
                force=force,
            )
        ),
        TranscriptNormalizationError,
    )


@app.command("qc-segments")
def qc_segments(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()
    run_service_command(
        build_segment_qc_service,
        lambda service: emit_qc_result(
            service.run(
                pilot_name=pilot,
                episode_id=episode_id,
                force=force,
            )
        ),
        SegmentQcError,
    )


@app.command("split-sentences")
def split_sentences(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()
    run_service_command(
        build_sentence_split_service,
        lambda service: emit_sentence_split_result(
            service.split(
                pilot_name=pilot,
                episode_id=episode_id,
                force=force,
            )
        ),
        SentenceSplitError,
    )


@app.command("tokenize-sentences")
def tokenize_sentences(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()
    run_service_command(
        build_sentence_tokenization_service,
        lambda service: emit_tokenization_result(
            service.tokenize(
                pilot_name=pilot,
                episode_id=episode_id,
                force=force,
            )
        ),
        SentenceTokenizationError,
    )


def main() -> None:
    app()
