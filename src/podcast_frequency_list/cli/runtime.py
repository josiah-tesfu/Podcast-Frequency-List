from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

import typer

from podcast_frequency_list.db import bootstrap_database

S = TypeVar("S")


def fail(exc: Exception) -> None:
    typer.echo(f"error={exc}")
    raise typer.Exit(code=1) from exc


def run_service_command(
    factory: Callable[[], S],
    action: Callable[[S], None],
    handled_errors: type[Exception] | tuple[type[Exception], ...],
) -> None:
    service: S | None = None
    try:
        service = factory()
        action(service)
    except handled_errors as exc:
        fail(exc)
    finally:
        if service is not None:
            close = getattr(service, "close", None)
            if callable(close):
                close()


def run_bootstrapped_service_command(
    factory: Callable[[], S],
    action: Callable[[S], None],
    handled_errors: type[Exception] | tuple[type[Exception], ...],
) -> None:
    bootstrap_database()
    run_service_command(factory, action, handled_errors)
