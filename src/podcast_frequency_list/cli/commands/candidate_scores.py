from __future__ import annotations

from functools import partial

import typer

from podcast_frequency_list.cli import emitters, handlers
from podcast_frequency_list.cli.commands.execution import (
    CommandCallback,
    run_service_method,
    service_factory,
)
from podcast_frequency_list.cli.runtime import run_bootstrapped_service_command
from podcast_frequency_list.tokens import CandidateScoresError

_CANDIDATE_KEY_OPTION = typer.Option(None, "--candidate-key")


def refresh_candidate_scores() -> None:
    run_service_method(
        service_factory_name="build_candidate_scores_service",
        handled_errors=CandidateScoresError,
        emitter=emitters.emit_candidate_scores_result,
        method_name="refresh",
        method_kwargs={},
    )


def inspect_candidate_scores(
    limit: int = typer.Option(10, "--limit", min=1),
    offset: int = typer.Option(0, "--offset", min=0),
    candidate_key: list[str] | None = _CANDIDATE_KEY_OPTION,
) -> None:
    run_bootstrapped_service_command(
        service_factory("build_candidate_scores_service"),
        partial(
            _inspect_candidate_scores_service,
            limit=limit,
            offset=offset,
            candidate_keys=candidate_key,
        ),
        CandidateScoresError,
    )


def _inspect_candidate_scores_service(
    service: object,
    *,
    limit: int,
    offset: int,
    candidate_keys: list[str] | None,
) -> None:
    handlers.inspect_candidate_scores(
        service,
        limit=limit,
        offset=offset,
        candidate_keys=candidate_keys,
    )


COMMANDS: tuple[tuple[str, CommandCallback], ...] = (
    ("refresh-candidate-scores", refresh_candidate_scores),
    ("inspect-candidate-scores", inspect_candidate_scores),
)
