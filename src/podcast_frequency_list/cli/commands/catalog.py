from __future__ import annotations

import typer

from podcast_frequency_list.cli import emitters
from podcast_frequency_list.cli.commands.execution import (
    CommandCallback,
    run_service_method,
)
from podcast_frequency_list.discovery import FeedVerificationError
from podcast_frequency_list.ingest import RssFeedError, SyncFeedError
from podcast_frequency_list.pilot import CorpusStatusError, PilotSelectionError


def add_show(
    feed_url: str,
    title: str | None = typer.Option(None),
    language: str | None = typer.Option(None),
    bucket: str | None = typer.Option(None),
) -> None:
    run_service_method(
        service_factory_name="build_manual_discovery_service",
        handled_errors=FeedVerificationError,
        emitter=emitters.emit_saved_show,
        method_name="save_manual_feed",
        method_kwargs={
            "feed_url": feed_url,
            "title": title,
            "language": language,
            "bucket": bucket,
        },
    )


def sync_feed(
    show_id: int = typer.Option(..., "--show-id", min=1),
    limit: int | None = typer.Option(None, min=1),
) -> None:
    run_service_method(
        service_factory_name="build_sync_feed_service",
        handled_errors=(RssFeedError, SyncFeedError),
        emitter=emitters.emit_sync_result,
        method_name="sync_show",
        method_kwargs={"show_id": show_id, "limit": limit},
    )


def create_pilot(
    show_id: int = typer.Option(..., "--show-id", min=1),
    name: str = typer.Option(..., "--name"),
    hours: float = typer.Option(10.0, "--hours", min=0.1),
    selection_order: str = typer.Option("newest", "--selection-order"),
    notes: str | None = typer.Option(None, "--notes"),
) -> None:
    run_service_method(
        service_factory_name="build_pilot_selection_service",
        handled_errors=PilotSelectionError,
        emitter=emitters.emit_pilot_result,
        method_name="create_pilot",
        method_kwargs={
            "show_id": show_id,
            "name": name,
            "target_seconds": round(hours * 3600),
            "selection_order": selection_order,
            "notes": notes,
        },
    )


def inspect_corpus() -> None:
    run_service_method(
        service_factory_name="build_corpus_status_service",
        handled_errors=CorpusStatusError,
        emitter=emitters.emit_corpus_status_result,
        method_name="inspect",
        method_kwargs={},
    )


COMMANDS: tuple[tuple[str, CommandCallback], ...] = (
    ("add-show", add_show),
    ("sync-feed", sync_feed),
    ("create-pilot", create_pilot),
    ("inspect-corpus", inspect_corpus),
)
