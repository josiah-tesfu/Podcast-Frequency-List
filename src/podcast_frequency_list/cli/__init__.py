from __future__ import annotations

import sys

import typer

from podcast_frequency_list.cli.commands import register_commands
from podcast_frequency_list.cli.service_factories import (
    build_asr_run_service,
    build_candidate_inventory_service,
    build_candidate_metrics_service,
    build_manual_discovery_service,
    build_pilot_selection_service,
    build_segment_qc_service,
    build_sentence_split_service,
    build_sentence_tokenization_service,
    build_sync_feed_service,
    build_transcript_normalization_service,
)

__all__ = [
    "app",
    "main",
    "build_asr_run_service",
    "build_candidate_inventory_service",
    "build_candidate_metrics_service",
    "build_manual_discovery_service",
    "build_pilot_selection_service",
    "build_segment_qc_service",
    "build_sentence_split_service",
    "build_sentence_tokenization_service",
    "build_sync_feed_service",
    "build_transcript_normalization_service",
]

app = typer.Typer(
    add_completion=False,
    help="CLI for building the podcast-based French frequency deck.",
    no_args_is_help=True,
)

register_commands(app, registry=sys.modules[__name__])


def main() -> None:
    app()
