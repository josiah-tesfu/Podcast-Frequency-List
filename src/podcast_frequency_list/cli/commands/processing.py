from __future__ import annotations

import typer

from podcast_frequency_list.cli import emitters
from podcast_frequency_list.cli.commands.execution import (
    CommandCallback,
    run_service_method,
)
from podcast_frequency_list.normalize import TranscriptNormalizationError
from podcast_frequency_list.qc import SegmentQcError
from podcast_frequency_list.sentences import SentenceSplitError
from podcast_frequency_list.tokens import CandidateInventoryError, SentenceTokenizationError


def normalize_transcripts(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    run_service_method(
        service_factory_name="build_transcript_normalization_service",
        handled_errors=TranscriptNormalizationError,
        emitter=emitters.emit_normalization_result,
        method_name="normalize",
        method_kwargs=_scope_kwargs(pilot=pilot, episode_id=episode_id, force=force),
    )


def qc_segments(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    run_service_method(
        service_factory_name="build_segment_qc_service",
        handled_errors=SegmentQcError,
        emitter=emitters.emit_qc_result,
        method_name="run",
        method_kwargs=_scope_kwargs(pilot=pilot, episode_id=episode_id, force=force),
    )


def split_sentences(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    run_service_method(
        service_factory_name="build_sentence_split_service",
        handled_errors=SentenceSplitError,
        emitter=emitters.emit_sentence_split_result,
        method_name="split",
        method_kwargs=_scope_kwargs(pilot=pilot, episode_id=episode_id, force=force),
    )


def tokenize_sentences(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    run_service_method(
        service_factory_name="build_sentence_tokenization_service",
        handled_errors=SentenceTokenizationError,
        emitter=emitters.emit_tokenization_result,
        method_name="tokenize",
        method_kwargs=_scope_kwargs(pilot=pilot, episode_id=episode_id, force=force),
    )


def generate_candidates(
    pilot: str | None = typer.Option(None, "--pilot"),
    episode_id: int | None = typer.Option(None, "--episode-id", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    run_service_method(
        service_factory_name="build_candidate_inventory_service",
        handled_errors=CandidateInventoryError,
        emitter=emitters.emit_candidate_inventory_result,
        method_name="generate",
        method_kwargs=_scope_kwargs(pilot=pilot, episode_id=episode_id, force=force),
    )


def _scope_kwargs(*, pilot: str | None, episode_id: int | None, force: bool) -> dict[str, object]:
    return {
        "pilot_name": pilot,
        "episode_id": episode_id,
        "force": force,
    }


COMMANDS: tuple[tuple[str, CommandCallback], ...] = (
    ("normalize-transcripts", normalize_transcripts),
    ("qc-segments", qc_segments),
    ("split-sentences", split_sentences),
    ("tokenize-sentences", tokenize_sentences),
    ("generate-candidates", generate_candidates),
)
