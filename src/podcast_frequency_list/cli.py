from __future__ import annotations

import typer

from podcast_frequency_list.config import load_settings
from podcast_frequency_list.db import bootstrap_database
from podcast_frequency_list.discovery import (
    DEFAULT_USER_AGENT,
    FeedVerificationError,
    PodcastCandidate,
    PodcastIndexClient,
    PodcastIndexCredentialsError,
    PodcastIndexError,
    ShowDiscoveryService,
)
from podcast_frequency_list.discovery.feed_verifier import FeedVerifier
from podcast_frequency_list.ingest import (
    RssFeedClient,
    RssFeedError,
    SyncFeedError,
    SyncFeedService,
)

app = typer.Typer(
    add_completion=False,
    help="CLI for building the podcast-based French frequency deck.",
    no_args_is_help=True,
)


def build_discovery_service() -> ShowDiscoveryService:
    # Retained for later. Current discovery work uses direct feed URLs.
    settings = load_settings()
    return ShowDiscoveryService(
        db_path=settings.db_path,
        podcast_index_client=PodcastIndexClient(
            api_key=settings.podcast_index_api_key,
            api_secret=settings.podcast_index_api_secret,
            user_agent=DEFAULT_USER_AGENT,
        ),
        feed_verifier=FeedVerifier(user_agent=DEFAULT_USER_AGENT),
    )


def build_manual_discovery_service() -> ShowDiscoveryService:
    settings = load_settings()
    return ShowDiscoveryService(
        db_path=settings.db_path,
        podcast_index_client=None,
        feed_verifier=FeedVerifier(user_agent=DEFAULT_USER_AGENT),
    )


def build_sync_feed_service() -> SyncFeedService:
    settings = load_settings()
    return SyncFeedService(
        db_path=settings.db_path,
        rss_feed_client=RssFeedClient(user_agent=DEFAULT_USER_AGENT),
    )


def echo_candidate(index: int, candidate: PodcastCandidate) -> None:
    typer.echo(
        f"[{index}] {candidate.title} | author={candidate.author or '-'} "
        f"| lang={candidate.language or '-'} | dead={candidate.dead} "
        f"| score={candidate.score:.1f}"
    )
    typer.echo(f"    feed_url={candidate.feed_url}")
    if candidate.site_url:
        typer.echo(f"    site_url={candidate.site_url}")


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


@app.command("add-show")
def add_show(
    feed_url: str,
    title: str | None = typer.Option(None),
    language: str | None = typer.Option(None),
    bucket: str | None = typer.Option(None),
) -> None:
    bootstrap_database()
    service = build_manual_discovery_service()

    try:
        saved_show = service.save_manual_feed(
            feed_url=feed_url,
            title=title,
            language=language,
            bucket=bucket,
        )
        typer.echo(f"saved_show_id={saved_show.show_id}")
        typer.echo(f"title={saved_show.title}")
        typer.echo(f"feed_url={saved_show.feed_url}")
    except FeedVerificationError as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc
    finally:
        service.close()


@app.command("sync-feed")
def sync_feed(
    show_id: int = typer.Option(..., "--show-id", min=1),
    limit: int | None = typer.Option(None, min=1),
) -> None:
    bootstrap_database()
    service = build_sync_feed_service()

    try:
        result = service.sync_show(show_id=show_id, limit=limit)
        typer.echo(f"show_id={result.show_id}")
        typer.echo(f"title={result.title}")
        typer.echo(f"episodes_seen={result.episodes_seen}")
        typer.echo(f"episodes_inserted={result.episodes_inserted}")
        typer.echo(f"episodes_updated={result.episodes_updated}")
        typer.echo(f"episodes_skipped_no_audio={result.episodes_skipped_no_audio}")
        typer.echo(f"episodes_with_transcript_tag={result.episodes_with_transcript_tag}")
    except (RssFeedError, SyncFeedError) as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc
    finally:
        service.close()


@app.command("discover-show")
def discover_show(
    query: str,
    limit: int = typer.Option(5, min=1, max=10),
    select: int | None = typer.Option(None, min=1),
) -> None:
    bootstrap_database()
    service = build_discovery_service()

    try:
        candidates = service.search(query, limit=limit)
        if not candidates:
            typer.echo("no_candidates_found")
            raise typer.Exit(code=1)

        for index, candidate in enumerate(candidates, start=1):
            echo_candidate(index, candidate)

        selection = select or typer.prompt("select_candidate", type=int)
        if selection < 1 or selection > len(candidates):
            typer.echo("invalid_selection")
            raise typer.Exit(code=1)

        saved_show = service.save_selected_candidate(candidates[selection - 1])
        typer.echo(f"saved_show_id={saved_show.show_id}")
        typer.echo(f"title={saved_show.title}")
        typer.echo(f"feed_url={saved_show.feed_url}")
    except (PodcastIndexCredentialsError, PodcastIndexError, FeedVerificationError) as exc:
        typer.echo(f"error={exc}")
        raise typer.Exit(code=1) from exc
    finally:
        service.close()


def main() -> None:
    app()
