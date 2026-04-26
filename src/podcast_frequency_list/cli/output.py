from __future__ import annotations

from collections.abc import Iterable

import typer


def emit_fields(fields: Iterable[tuple[str, object]]) -> None:
    for key, value in fields:
        typer.echo(f"{key}={value}")


def emit_inline_fields(fields: Iterable[tuple[str, object]]) -> None:
    typer.echo(_format_fields(fields, separator=" "))


def emit_record(fields: Iterable[tuple[str, object]]) -> None:
    typer.echo(_format_fields(fields, separator="\t"))


def _format_fields(fields: Iterable[tuple[str, object]], *, separator: str) -> str:
    return separator.join(f"{key}={value}" for key, value in fields)
