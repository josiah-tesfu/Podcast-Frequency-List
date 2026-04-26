from __future__ import annotations

from podcast_frequency_list.cli import handlers
from podcast_frequency_list.cli.commands.execution import CommandCallback


def info() -> None:
    handlers.emit_info()


def init_db() -> None:
    handlers.emit_initialized_db()


COMMANDS: tuple[tuple[str, CommandCallback], ...] = (
    ("info", info),
    ("init-db", init_db),
)
