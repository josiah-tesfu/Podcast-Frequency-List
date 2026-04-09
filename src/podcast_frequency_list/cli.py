from __future__ import annotations

import typer

from podcast_frequency_list.config import load_settings
from podcast_frequency_list.db import bootstrap_database

app = typer.Typer(
    add_completion=False,
    help="CLI for building the podcast-based French frequency deck.",
    no_args_is_help=True,
)


@app.command("info")
def info() -> None:
    settings = load_settings()

    typer.echo(f"project_root={settings.project_root}")
    typer.echo(f"db_path={settings.db_path}")
    typer.echo(f"raw_data_dir={settings.raw_data_dir}")
    typer.echo(f"processed_data_dir={settings.processed_data_dir}")


@app.command("init-db")
def init_db() -> None:
    db_path = bootstrap_database()
    typer.echo(f"initialized_db={db_path}")


def main() -> None:
    app()
