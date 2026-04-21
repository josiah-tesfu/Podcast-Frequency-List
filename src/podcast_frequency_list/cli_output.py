from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TypeVar

import typer

from podcast_frequency_list.asr import AsrEpisodeResult, AsrRunResult
from podcast_frequency_list.discovery.models import SavedShow
from podcast_frequency_list.ingest import SyncFeedResult
from podcast_frequency_list.normalize import NormalizationRunResult
from podcast_frequency_list.pilot import PilotSelectionResult
from podcast_frequency_list.qc import QcRunResult
from podcast_frequency_list.sentences import SentenceSplitResult
from podcast_frequency_list.tokens import TokenizationResult

S = TypeVar("S")


def emit_fields(fields: Iterable[tuple[str, object]]) -> None:
    for key, value in fields:
        typer.echo(f"{key}={value}")


def fail(exc: Exception) -> None:
    typer.echo(f"error={exc}")
    raise typer.Exit(code=1) from exc


def run_service_command(
    factory: Callable[[], S],
    action: Callable[[S], None],
    handled_errors: type[Exception] | tuple[type[Exception], ...],
) -> None:
    try:
        service = factory()
    except handled_errors as exc:
        fail(exc)

    try:
        action(service)
    except handled_errors as exc:
        fail(exc)
    finally:
        close = getattr(service, "close", None)
        if callable(close):
            close()


def emit_saved_show(saved_show: SavedShow) -> None:
    emit_fields(
        (
            ("saved_show_id", saved_show.show_id),
            ("title", saved_show.title),
            ("feed_url", saved_show.feed_url),
        )
    )


def emit_sync_result(result: SyncFeedResult) -> None:
    emit_fields(
        (
            ("show_id", result.show_id),
            ("title", result.title),
            ("episodes_seen", result.episodes_seen),
            ("episodes_inserted", result.episodes_inserted),
            ("episodes_updated", result.episodes_updated),
            ("episodes_skipped_no_audio", result.episodes_skipped_no_audio),
            ("episodes_with_transcript_tag", result.episodes_with_transcript_tag),
        )
    )


def emit_pilot_result(result: PilotSelectionResult) -> None:
    emit_fields(
        (
            ("pilot_run_id", result.pilot_run_id),
            ("name", result.name),
            ("show_id", result.show_id),
            ("title", result.show_title),
            ("target_hours", f"{result.target_seconds / 3600:.2f}"),
            ("selected_hours", f"{result.total_seconds / 3600:.2f}"),
            ("selected_episodes", result.selected_count),
            ("skipped_ineligible_episodes", result.skipped_count),
            ("selection_order", result.selection_order),
            ("model", result.model),
            ("estimated_asr_cost_usd", f"{result.estimated_cost_usd:.2f}"),
            ("first_selected_published_at", result.first_published_at or "-"),
            ("last_selected_published_at", result.last_published_at or "-"),
            ("status", "needs_asr"),
        )
    )


def emit_asr_episode_result(result: AsrEpisodeResult) -> None:
    typer.echo(
        f"episode_id={result.episode_id} status={result.status} "
        f"chunks={result.chunk_count} chars={result.text_chars}"
    )
    if result.transcript_path:
        typer.echo(f"transcript_path={result.transcript_path}")
    if result.preview:
        typer.echo(f"preview={result.preview}")
    if result.error:
        typer.echo(f"error={result.error}")


def emit_asr_result(result: AsrRunResult) -> None:
    emit_fields(
        (
            ("pilot", result.pilot_name),
            ("model", result.model),
            ("requested_limit", result.requested_limit or "-"),
            ("selected_episodes", result.selected_count),
            ("completed_episodes", result.completed_count),
            ("skipped_episodes", result.skipped_count),
            ("failed_episodes", result.failed_count),
            ("chunks_transcribed", result.chunk_count),
        )
    )
    for episode_result in result.episode_results:
        emit_asr_episode_result(episode_result)


def emit_normalization_result(result: NormalizationRunResult) -> None:
    emit_fields(
        (
            ("scope", result.scope),
            ("scope_value", result.scope_value),
            ("normalization_version", result.normalization_version),
            ("selected_segments", result.selected_segments),
            ("normalized_segments", result.normalized_segments),
            ("skipped_segments", result.skipped_segments),
            ("episodes_touched", result.episode_count),
        )
    )


def emit_qc_result(result: QcRunResult) -> None:
    emit_fields(
        (
            ("scope", result.scope),
            ("scope_value", result.scope_value),
            ("qc_version", result.qc_version),
            ("selected_segments", result.selected_segments),
            ("processed_segments", result.processed_segments),
            ("skipped_segments", result.skipped_segments),
            ("keep_segments", result.keep_segments),
            ("review_segments", result.review_segments),
            ("remove_segments", result.remove_segments),
        )
    )


def emit_sentence_split_result(result: SentenceSplitResult) -> None:
    emit_fields(
        (
            ("scope", result.scope),
            ("scope_value", result.scope_value),
            ("split_version", result.split_version),
            ("selected_segments", result.selected_segments),
            ("created_sentences", result.created_sentences),
            ("skipped_segments", result.skipped_segments),
            ("episodes_touched", result.episode_count),
        )
    )


def emit_tokenization_result(result: TokenizationResult) -> None:
    emit_fields(
        (
            ("scope", result.scope),
            ("scope_value", result.scope_value),
            ("tokenization_version", result.tokenization_version),
            ("selected_sentences", result.selected_sentences),
            ("tokenized_sentences", result.tokenized_sentences),
            ("created_tokens", result.created_tokens),
            ("skipped_sentences", result.skipped_sentences),
            ("episodes_touched", result.episode_count),
        )
    )
