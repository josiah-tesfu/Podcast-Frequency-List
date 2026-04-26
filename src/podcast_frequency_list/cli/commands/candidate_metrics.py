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
from podcast_frequency_list.tokens import CandidateMetricsError

_CANDIDATE_KEY_OPTION = typer.Option(None, "--candidate-key")


def refresh_candidate_metrics() -> None:
    run_service_method(
        service_factory_name="build_candidate_metrics_service",
        handled_errors=CandidateMetricsError,
        emitter=emitters.emit_candidate_metrics_result,
        method_name="refresh",
        method_kwargs={},
    )


def inspect_candidate_metrics(
    limit: int = typer.Option(10, "--limit", min=1),
    candidate_key: list[str] | None = _CANDIDATE_KEY_OPTION,
) -> None:
    run_bootstrapped_service_command(
        service_factory("build_candidate_metrics_service"),
        partial(
            _inspect_candidate_metrics_service,
            limit=limit,
            candidate_keys=candidate_key,
        ),
        CandidateMetricsError,
    )


def _inspect_candidate_metrics_service(
    service: object,
    *,
    limit: int,
    candidate_keys: list[str] | None,
) -> None:
    handlers.inspect_candidate_metrics(
        service,
        limit=limit,
        candidate_keys=candidate_keys,
    )


COMMANDS: tuple[tuple[str, CommandCallback], ...] = (
    ("refresh-candidate-metrics", refresh_candidate_metrics),
    ("inspect-candidate-metrics", inspect_candidate_metrics),
)
