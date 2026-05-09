from pathlib import Path

import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.pilot.service import PilotSelectionService
from podcast_frequency_list.show_slices import ShowSliceError, ShowSliceService


def _build_manifest(tmp_path: Path) -> Path:
    manifest_path = tmp_path / "show_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "slug,title,feed_url,language,bucket,family,target_hours,selection_order,enabled,notes",
                "zack,Zack en Roue Libre,https://example.com/zack.xml,fr,native,test,10,newest,1,one",
                "flood,FloodCast,https://example.com/flood.xml,fr,native,test,2.5,newest,1,two",
            ]
        ),
        encoding="utf-8",
    )
    return manifest_path


def test_show_slice_service_creates_manifest_driven_slices_and_is_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    manifest_path = _build_manifest(tmp_path)

    with connect(db_path) as connection:
        zack_id = upsert_show(
            connection,
            title="Zack en Roue Libre by Zack Nani",
            feed_url="https://example.com/zack.xml",
        )
        flood_id = upsert_show(
            connection,
            title="FloodCast",
            feed_url="https://example.com/flood.xml",
        )
        upsert_episode(
            connection,
            show_id=zack_id,
            guid="z-1",
            title="Episode Zack 1",
            published_at="2025-01-03T00:00:00+00:00",
            audio_url="https://cdn.example.com/zack-1.mp3",
            duration_seconds=3_600,
        )
        upsert_episode(
            connection,
            show_id=zack_id,
            guid="z-2",
            title="Episode Zack 2",
            published_at="2025-01-02T00:00:00+00:00",
            audio_url="https://cdn.example.com/zack-2.mp3",
            duration_seconds=3_600,
        )
        upsert_episode(
            connection,
            show_id=flood_id,
            guid="f-1",
            title="Episode Flood 1",
            published_at="2025-01-03T00:00:00+00:00",
            audio_url="https://cdn.example.com/flood-1.mp3",
            duration_seconds=1_800,
        )
        upsert_episode(
            connection,
            show_id=flood_id,
            guid="f-2",
            title="Episode Flood 2",
            published_at="2025-01-02T00:00:00+00:00",
            audio_url="https://cdn.example.com/flood-2.mp3",
            duration_seconds=1_800,
        )
        connection.commit()

    service = ShowSliceService(
        db_path=db_path,
        pilot_selection_service=PilotSelectionService(db_path=db_path),
    )

    first_result = service.bootstrap_manifest(manifest_path=manifest_path)
    second_result = service.bootstrap_manifest(manifest_path=manifest_path)

    with connect(db_path) as connection:
        pilot_count = connection.execute("SELECT COUNT(*) FROM pilot_runs").fetchone()[0]
        zack_row = connection.execute(
            """
            SELECT pilot_run_id, show_id, name, selection_order, notes
            FROM pilot_runs
            WHERE name = 'zack-10h-slice'
            """
        ).fetchone()
        flood_row = connection.execute(
            """
            SELECT pilot_run_id, show_id, name, selection_order, notes
            FROM pilot_runs
            WHERE name = 'flood-2p5h-slice'
            """
        ).fetchone()

    assert first_result.selected_shows == 2
    assert first_result.created_slices == 2
    assert first_result.selected_episodes == 4
    assert first_result.skipped_ineligible_episodes == 0
    assert [row.slice_name for row in first_result.rows] == [
        "zack-10h-slice",
        "flood-2p5h-slice",
    ]

    assert second_result.selected_shows == 2
    assert second_result.created_slices == 2
    assert pilot_count == 2
    assert int(zack_row["show_id"]) == zack_id
    assert int(flood_row["show_id"]) == flood_id
    assert str(zack_row["selection_order"]) == "newest"
    assert str(flood_row["selection_order"]) == "newest"
    assert "manifest_family=test" in str(zack_row["notes"])
    assert "manifest_notes=one" in str(zack_row["notes"])
    assert "manifest_family=test" in str(flood_row["notes"])
    assert "manifest_notes=two" in str(flood_row["notes"])


def test_show_slice_service_errors_when_manifest_show_missing_from_db(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    manifest_path = _build_manifest(tmp_path)

    with connect(db_path) as connection:
        zack_id = upsert_show(
            connection,
            title="Zack en Roue Libre by Zack Nani",
            feed_url="https://example.com/zack.xml",
        )
        upsert_episode(
            connection,
            show_id=zack_id,
            guid="z-1",
            title="Episode Zack 1",
            published_at="2025-01-03T00:00:00+00:00",
            audio_url="https://cdn.example.com/zack-1.mp3",
            duration_seconds=3_600,
        )
        connection.commit()

    service = ShowSliceService(
        db_path=db_path,
        pilot_selection_service=PilotSelectionService(db_path=db_path),
    )

    with pytest.raises(ShowSliceError, match="slug=flood"):
        service.bootstrap_manifest(manifest_path=manifest_path)
