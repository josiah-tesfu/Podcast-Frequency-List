from __future__ import annotations

from podcast_frequency_list.cli.output import emit_fields, emit_record
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.pilot.service import PilotSelectionService
from podcast_frequency_list.show_slices import ShowSliceService


def main() -> None:
    settings = load_settings()
    service = ShowSliceService(
        db_path=settings.db_path,
        pilot_selection_service=PilotSelectionService(db_path=settings.db_path),
    )
    result = service.bootstrap_manifest()

    emit_fields(
        (
            ("selected_shows", result.selected_shows),
            ("created_slices", result.created_slices),
            ("selected_episodes", result.selected_episodes),
            ("selected_hours", f"{result.selected_seconds / 3600:.2f}"),
            ("skipped_ineligible_episodes", result.skipped_ineligible_episodes),
        )
    )
    for row in result.rows:
        emit_record(
            (
                ("record", "show_slice"),
                ("slug", row.slug),
                ("show_id", row.show_id),
                ("title", row.title),
                ("feed_url", row.feed_url),
                ("slice_id", row.slice_id),
                ("slice_name", row.slice_name),
                ("target_hours", row.target_hours),
                ("selected_hours", f"{row.selected_hours:.2f}"),
                ("selected_episodes", row.selected_episodes),
                ("skipped_ineligible_episodes", row.skipped_ineligible_episodes),
                ("selection_order", row.selection_order),
            )
        )


if __name__ == "__main__":
    main()
