from __future__ import annotations

from types import ModuleType

import typer

from podcast_frequency_list.cli.commands.candidate_metrics import (
    COMMANDS as CANDIDATE_METRICS_COMMANDS,
)
from podcast_frequency_list.cli.commands.candidate_scores import (
    COMMANDS as CANDIDATE_SCORES_COMMANDS,
)
from podcast_frequency_list.cli.commands.catalog import COMMANDS as CATALOG_COMMANDS
from podcast_frequency_list.cli.commands.execution import CommandCallback, set_registry
from podcast_frequency_list.cli.commands.processing import (
    COMMANDS as PROCESSING_COMMANDS,
)
from podcast_frequency_list.cli.commands.project import COMMANDS as PROJECT_COMMANDS
from podcast_frequency_list.cli.commands.transcription import (
    COMMANDS as TRANSCRIPTION_COMMANDS,
)

_COMMANDS: tuple[tuple[str, CommandCallback], ...] = (
    *PROJECT_COMMANDS,
    *CATALOG_COMMANDS,
    *TRANSCRIPTION_COMMANDS,
    *PROCESSING_COMMANDS,
    *CANDIDATE_METRICS_COMMANDS,
    *CANDIDATE_SCORES_COMMANDS,
)


def register_commands(app: typer.Typer, *, registry: ModuleType) -> None:
    set_registry(registry)
    for command_name, callback in _COMMANDS:
        app.command(command_name)(callback)
