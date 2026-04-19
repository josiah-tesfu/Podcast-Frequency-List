from __future__ import annotations

import typer

from podcast_frequency_list.asr import (
    AsrRunError,
    AsrRunService,
    AudioChunker,
    AudioDownloader,
    OpenAITranscriber,
)
from podcast_frequency_list.asr.audio import DEFAULT_SAFE_UPLOAD_BYTES
from podcast_frequency_list.asr.client import OpenAITranscriptionError
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
from podcast_frequency_list.sentences import SentenceSplitError, SentenceSplitService

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


@app.command("info")
def info() -> None:
    settings = load_settings()

    typer.echo(f"project_root={settings.project_root}")
    typer.echo(f"db_path={settings.db_path}")
    typer.echo(f"raw_data_dir={settings.raw_data_dir}")
    typer.echo(f"processed_data_dir={settings.processed_data_dir}")


@app.command("init-db")
def init_db() -> None:
    db_path = bootstrap_database()
    typer.echo(f"initialized_db={db_path}")


@app.command("add-show")
def add_show(
    feed_url: str,
    title: str | None = typer.Option(None),
    language: str | None = typer.Option(None),
    bucket: str | None = typer.Option(None),
) -> None:
    bootstrap_database()
    service = build_manual_discovery_service()

    try:
        saved_show = service.save_manual_feed(
            feed_url=feed_url,
            title=title,
            language=language,
            bucket=bucket,
        )
        typer.echo(f"saved_show_id={saved_show.show_id}")
        typer.echo(f"title={saved_show.title}")
        typer.echo(f"feed_url={saved_show.feed_url}")
    except FeedVerificationError as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc
    finally:
        service.close()


@app.command("sync-feed")
def sync_feed(
    show_id: int = typer.Option(..., "--show-id", min=1),
    limit: int | None = typer.Option(None, min=1),
) -> None:
    bootstrap_database()
    service = build_sync_feed_service()

    try:
        result = service.sync_show(show_id=show_id, limit=limit)
        typer.echo(f"show_id={result.show_id}")
        typer.echo(f"title={result.title}")
        typer.echo(f"episodes_seen={result.episodes_seen}")
        typer.echo(f"episodes_inserted={result.episodes_inserted}")
        typer.echo(f"episodes_updated={result.episodes_updated}")
        typer.echo(f"episodes_skipped_no_audio={result.episodes_skipped_no_audio}")
        typer.echo(f"episodes_with_transcript_tag={result.episodes_with_transcript_tag}")
    except (RssFeedError, SyncFeedError) as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc
    finally:
        service.close()


@app.command("create-pilot")
def create_pilot(
    show_id: int = typer.Option(..., "--show-id", min=1),
    name: str = typer.Option(..., "--name"),
    hours: float = typer.Option(10.0, "--hours", min=0.1),
    selection_order: str = typer.Option("newest", "--selection-order"),
    notes: str | None = typer.Option(None, "--notes"),
) -> None:
    bootstrap_database()
    service = build_pilot_selection_service()

    try:
        result = service.create_pilot(
            show_id=show_id,
            name=name,
            target_seconds=round(hours * 3600),
            selection_order=selection_order,
            notes=notes,
        )
        typer.echo(f"pilot_run_id={result.pilot_run_id}")
        typer.echo(f"name={result.name}")
        typer.echo(f"show_id={result.show_id}")
        typer.echo(f"title={result.show_title}")
        typer.echo(f"target_hours={result.target_seconds / 3600:.2f}")
        typer.echo(f"selected_hours={result.total_seconds / 3600:.2f}")
        typer.echo(f"selected_episodes={result.selected_count}")
        typer.echo(f"skipped_ineligible_episodes={result.skipped_count}")
        typer.echo(f"selection_order={result.selection_order}")
        typer.echo(f"model={result.model}")
        typer.echo(f"estimated_asr_cost_usd={result.estimated_cost_usd:.2f}")
        typer.echo(f"first_selected_published_at={result.first_published_at or '-'}")
        typer.echo(f"last_selected_published_at={result.last_published_at or '-'}")
        typer.echo("status=needs_asr")
    except PilotSelectionError as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc


@app.command("run-asr")
def run_asr(
    pilot: str = typer.Option(..., "--pilot"),
    limit: int | None = typer.Option(None, "--limit", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()

    try:
        service = build_asr_run_service()
    except OpenAITranscriptionError as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc

    try:
        result = service.run_pilot(pilot_name=pilot, limit=limit, force=force)
        typer.echo(f"pilot={result.pilot_name}")
        typer.echo(f"model={result.model}")
        typer.echo(f"requested_limit={result.requested_limit or '-'}")
        typer.echo(f"selected_episodes={result.selected_count}")
        typer.echo(f"completed_episodes={result.completed_count}")
        typer.echo(f"skipped_episodes={result.skipped_count}")
        typer.echo(f"failed_episodes={result.failed_count}")
        typer.echo(f"chunks_transcribed={result.chunk_count}")
        for episode_result in result.episode_results:
            typer.echo(
                f"episode_id={episode_result.episode_id} status={episode_result.status} "
                f"chunks={episode_result.chunk_count} chars={episode_result.text_chars}"
            )
            if episode_result.transcript_path:
                typer.echo(f"transcript_path={episode_result.transcript_path}")
            if episode_result.preview:
                typer.echo(f"preview={episode_result.preview}")
            if episode_result.error:
                typer.echo(f"error={episode_result.error}")
        if result.failed_count:
            raise typer.Exit(code=1)
    except AsrRunError as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc
    finally:
        service.close()


@app.command("normalize-transcripts")
def normalize_transcripts(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()
    service = build_transcript_normalization_service()

    try:
        result = service.normalize(
            pilot_name=pilot,
            episode_id=episode_id,
            force=force,
        )
        typer.echo(f"scope={result.scope}")
        typer.echo(f"scope_value={result.scope_value}")
        typer.echo(f"normalization_version={result.normalization_version}")
        typer.echo(f"selected_segments={result.selected_segments}")
        typer.echo(f"normalized_segments={result.normalized_segments}")
        typer.echo(f"skipped_segments={result.skipped_segments}")
        typer.echo(f"episodes_touched={result.episode_count}")
    except TranscriptNormalizationError as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc


@app.command("qc-segments")
def qc_segments(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()
    service = build_segment_qc_service()

    try:
        result = service.run(
            pilot_name=pilot,
            episode_id=episode_id,
            force=force,
        )
        typer.echo(f"scope={result.scope}")
        typer.echo(f"scope_value={result.scope_value}")
        typer.echo(f"qc_version={result.qc_version}")
        typer.echo(f"selected_segments={result.selected_segments}")
        typer.echo(f"processed_segments={result.processed_segments}")
        typer.echo(f"skipped_segments={result.skipped_segments}")
        typer.echo(f"keep_segments={result.keep_segments}")
        typer.echo(f"review_segments={result.review_segments}")
        typer.echo(f"remove_segments={result.remove_segments}")
    except SegmentQcError as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc


@app.command("split-sentences")
def split_sentences(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    bootstrap_database()
    service = build_sentence_split_service()

    try:
        result = service.split(
            pilot_name=pilot,
            episode_id=episode_id,
            force=force,
        )
        typer.echo(f"scope={result.scope}")
        typer.echo(f"scope_value={result.scope_value}")
        typer.echo(f"split_version={result.split_version}")
        typer.echo(f"selected_segments={result.selected_segments}")
        typer.echo(f"created_sentences={result.created_sentences}")
        typer.echo(f"skipped_segments={result.skipped_segments}")
        typer.echo(f"episodes_touched={result.episode_count}")
    except SentenceSplitError as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc


def main() -> None:
    app()
