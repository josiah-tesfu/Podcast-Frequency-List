from __future__ import annotations

from functools import partial

import typer

from podcast_frequency_list.asr import AsrRunError
from podcast_frequency_list.asr.client import OpenAITranscriptionError
from podcast_frequency_list.cli import handlers
from podcast_frequency_list.cli.commands.execution import (
    CommandCallback,
    service_factory,
)
from podcast_frequency_list.cli.runtime import run_bootstrapped_service_command


def run_asr(
    pilot: str = typer.Option(..., "--pilot"),
    limit: int | None = typer.Option(None, "--limit", min=1),
    force: bool = typer.Option(False, "--force"),
) -> None:
    run_bootstrapped_service_command(
        service_factory("build_asr_run_service"),
        partial(_run_asr_service, pilot=pilot, limit=limit, force=force),
        (OpenAITranscriptionError, AsrRunError),
    )


def _run_asr_service(
    service: object,
    *,
    pilot: str,
    limit: int | None,
    force: bool,
) -> None:
    failed_count = handlers.run_asr_and_emit(
        service,
        pilot=pilot,
        limit=limit,
        force=force,
    )
    if failed_count:
        raise typer.Exit(code=1)


COMMANDS: tuple[tuple[str, CommandCallback], ...] = (("run-asr", run_asr),)
