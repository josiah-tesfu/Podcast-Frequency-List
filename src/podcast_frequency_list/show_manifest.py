from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.config import PROJECT_ROOT

DEFAULT_SHOW_MANIFEST_PATH = PROJECT_ROOT / "data/show_manifest.csv"


class ShowManifestError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShowManifestRow:
    slug: str
    title: str
    feed_url: str
    language: str
    bucket: str
    family: str
    target_hours: float
    selection_order: str
    enabled: bool
    notes: str


_REQUIRED_COLUMNS = (
    "slug",
    "title",
    "feed_url",
    "language",
    "bucket",
    "family",
    "target_hours",
    "selection_order",
    "enabled",
    "notes",
)


def load_show_manifest(path: Path | None = None) -> tuple[ShowManifestRow, ...]:
    target_path = path or DEFAULT_SHOW_MANIFEST_PATH
    if not target_path.exists():
        raise ShowManifestError(f"show manifest not found: {target_path}")

    with target_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        _validate_columns(reader.fieldnames)

        rows: list[ShowManifestRow] = []
        seen_slugs: set[str] = set()
        seen_feed_urls: set[str] = set()

        for line_number, raw_row in enumerate(reader, start=2):
            row = _build_row(raw_row, line_number=line_number)
            if row.slug in seen_slugs:
                raise ShowManifestError(f"duplicate slug at line {line_number}: {row.slug}")
            if row.feed_url in seen_feed_urls:
                raise ShowManifestError(f"duplicate feed_url at line {line_number}: {row.feed_url}")

            seen_slugs.add(row.slug)
            seen_feed_urls.add(row.feed_url)
            rows.append(row)

    return tuple(rows)


def _validate_columns(fieldnames: list[str] | None) -> None:
    if fieldnames is None:
        raise ShowManifestError("show manifest is missing a header row")

    actual = tuple(fieldnames)
    if actual != _REQUIRED_COLUMNS:
        raise ShowManifestError(
            "show manifest header mismatch: "
            f"expected {_REQUIRED_COLUMNS} got {actual}"
        )


def _build_row(raw_row: dict[str, str | None], *, line_number: int) -> ShowManifestRow:
    values = {
        key: (raw_row.get(key) or "").strip()
        for key in _REQUIRED_COLUMNS
    }
    _require_non_empty(values, line_number=line_number)

    target_hours = _parse_target_hours(values["target_hours"], line_number=line_number)
    enabled = _parse_enabled(values["enabled"], line_number=line_number)

    if values["selection_order"] != "newest":
        raise ShowManifestError(
            f"line {line_number}: selection_order must be newest, got {values['selection_order']}"
        )
    if enabled and target_hours <= 0:
        raise ShowManifestError(
            f"line {line_number}: enabled rows must have target_hours > 0"
        )

    return ShowManifestRow(
        slug=values["slug"],
        title=values["title"],
        feed_url=values["feed_url"],
        language=values["language"],
        bucket=values["bucket"],
        family=values["family"],
        target_hours=target_hours,
        selection_order=values["selection_order"],
        enabled=enabled,
        notes=values["notes"],
    )


def _require_non_empty(values: dict[str, str], *, line_number: int) -> None:
    for key, value in values.items():
        if key == "notes":
            continue
        if not value:
            raise ShowManifestError(f"line {line_number}: {key} is required")


def _parse_target_hours(raw_value: str, *, line_number: int) -> float:
    try:
        return float(raw_value)
    except ValueError as exc:
        raise ShowManifestError(
            f"line {line_number}: target_hours must be numeric, got {raw_value!r}"
        ) from exc


def _parse_enabled(raw_value: str, *, line_number: int) -> bool:
    if raw_value == "1":
        return True
    if raw_value == "0":
        return False
    raise ShowManifestError(f"line {line_number}: enabled must be 1 or 0, got {raw_value!r}")
