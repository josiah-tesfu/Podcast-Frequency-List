from __future__ import annotations

from podcast_frequency_list.cli import emitters
from podcast_frequency_list.cli.commands.execution import (
    CommandCallback,
    run_service_method,
)
from podcast_frequency_list.tokens import CandidateScoresError


def refresh_candidate_scores() -> None:
    run_service_method(
        service_factory_name="build_candidate_scores_service",
        handled_errors=CandidateScoresError,
        emitter=emitters.emit_candidate_scores_result,
        method_name="refresh",
        method_kwargs={},
    )


COMMANDS: tuple[tuple[str, CommandCallback], ...] = (
    ("refresh-candidate-scores", refresh_candidate_scores),
)
