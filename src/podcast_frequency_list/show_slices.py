from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.pilot.service import PilotSelectionService
from podcast_frequency_list.show_manifest import ShowManifestRow, load_show_manifest


class ShowSliceError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShowSliceRow:
    slug: str
    show_id: int
    title: str
    feed_url: str
    slice_id: int
    slice_name: str
    target_hours: float
    selected_hours: float
    selected_episodes: int
    skipped_ineligible_episodes: int
    selection_order: str


@dataclass(frozen=True)
class ShowSliceResult:
    selected_shows: int
    created_slices: int
    selected_episodes: int
    selected_seconds: int
    skipped_ineligible_episodes: int
    rows: tuple[ShowSliceRow, ...]


class ShowSliceService:
    def __init__(
        self,
        *,
        db_path: Path,
        pilot_selection_service: PilotSelectionService,
    ) -> None:
        self.db_path = db_path
        self.pilot_selection_service = pilot_selection_service

    def bootstrap_manifest(self, *, manifest_path: Path | None = None) -> ShowSliceResult:
        manifest_rows = tuple(row for row in load_show_manifest(manifest_path) if row.enabled)
        if not manifest_rows:
            raise ShowSliceError("no enabled shows found in manifest")

        rows: list[ShowSliceRow] = []
        for manifest_row in manifest_rows:
            show = self._load_show(feed_url=manifest_row.feed_url, slug=manifest_row.slug)
            slice_name = _build_slice_name(manifest_row)
            selection = self.pilot_selection_service.create_pilot(
                show_id=int(show["show_id"]),
                name=slice_name,
                target_seconds=round(manifest_row.target_hours * 3600),
                selection_order=manifest_row.selection_order,
                notes=_build_slice_notes(manifest_row),
            )
            rows.append(
                ShowSliceRow(
                    slug=manifest_row.slug,
                    show_id=int(show["show_id"]),
                    title=str(show["title"]),
                    feed_url=str(show["feed_url"]),
                    slice_id=selection.pilot_run_id,
                    slice_name=selection.name,
                    target_hours=manifest_row.target_hours,
                    selected_hours=selection.total_seconds / 3600,
                    selected_episodes=selection.selected_count,
                    skipped_ineligible_episodes=selection.skipped_count,
                    selection_order=selection.selection_order,
                )
            )

        return ShowSliceResult(
            selected_shows=len(manifest_rows),
            created_slices=len(rows),
            selected_episodes=sum(row.selected_episodes for row in rows),
            selected_seconds=round(sum(row.selected_hours for row in rows) * 3600),
            skipped_ineligible_episodes=sum(
                row.skipped_ineligible_episodes for row in rows
            ),
            rows=tuple(rows),
        )

    def _load_show(self, *, feed_url: str, slug: str) -> object:
        with connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT show_id, title, feed_url
                FROM shows
                WHERE feed_url = ?
                """,
                (feed_url,),
            ).fetchone()
        if row is None:
            raise ShowSliceError(f"manifest show not found in DB for slug={slug}")
        return row


def _build_slice_name(row: ShowManifestRow) -> str:
    return f"{row.slug}-{_format_hours_label(row.target_hours)}h-slice"


def _format_hours_label(hours: float) -> str:
    if hours.is_integer():
        return str(int(hours))
    return str(hours).rstrip("0").rstrip(".").replace(".", "p")


def _build_slice_notes(row: ShowManifestRow) -> str:
    parts = [f"manifest_family={row.family}"]
    if row.notes:
        parts.append(f"manifest_notes={row.notes}")
    return "; ".join(parts)
