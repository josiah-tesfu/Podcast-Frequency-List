from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.pilot import (
    CorpusStatusService,
    PilotSelectionError,
    PilotSelectionService,
)


def _seed_show(connection) -> int:
    return upsert_show(
        connection,
        title="Zack en Roue Libre by Zack Nani",
        feed_url="https://example.com/zack.xml",
    )


def test_pilot_selection_stops_after_target_and_marks_needs_asr(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = _seed_show(connection)
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-1",
            title="Episode 1",
            published_at="2025-01-03T00:00:00+00:00",
            audio_url="https://cdn.example.com/ep1.mp3",
            duration_seconds=3_600,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-2",
            title="Episode 2",
            published_at="2025-01-02T00:00:00+00:00",
            audio_url="https://cdn.example.com/ep2.mp3",
            duration_seconds=3_600,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-3",
            title="Episode 3",
            published_at="2025-01-01T00:00:00+00:00",
            audio_url="https://cdn.example.com/ep3.mp3",
            duration_seconds=3_600,
        )
        connection.commit()

    service = PilotSelectionService(db_path=db_path)

    result = service.create_pilot(
        show_id=show_id,
        name="zack-test-pilot",
        target_seconds=7_200,
    )

    with connect(db_path) as connection:
        pilot_rows = connection.execute(
            """
            SELECT episode_id, position, cumulative_seconds
            FROM pilot_run_episodes
            WHERE pilot_run_id = ?
            ORDER BY position
            """,
            (result.pilot_run_id,),
        ).fetchall()
        source_rows = connection.execute(
            """
            SELECT status, source_type, model
            FROM transcript_sources
            ORDER BY source_id
            """
        ).fetchall()

    assert result.selected_count == 2
    assert result.total_seconds == 7_200
    assert result.estimated_cost_usd == 0.36
    assert [row["position"] for row in pilot_rows] == [1, 2]
    assert [row["cumulative_seconds"] for row in pilot_rows] == [3_600, 7_200]
    assert len(source_rows) == 2
    assert {row["status"] for row in source_rows} == {"needs_asr"}
    assert {row["source_type"] for row in source_rows} == {"asr"}
    assert {row["model"] for row in source_rows} == {"gpt-4o-mini-transcribe"}


def test_pilot_selection_skips_missing_audio_or_duration(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = _seed_show(connection)
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-no-audio",
            title="No Audio",
            audio_url=None,
            duration_seconds=3_600,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-no-duration",
            title="No Duration",
            audio_url="https://cdn.example.com/no-duration.mp3",
            duration_seconds=None,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-valid",
            title="Valid",
            audio_url="https://cdn.example.com/valid.mp3",
            duration_seconds=3_600,
        )
        connection.commit()

    service = PilotSelectionService(db_path=db_path)

    result = service.create_pilot(
        show_id=show_id,
        name="zack-skip-test-pilot",
        target_seconds=3_600,
    )

    assert result.selected_count == 1
    assert result.skipped_count == 2
    assert result.episodes[0].title == "Valid"


def test_pilot_selection_skips_teaser_and_best_of_titles(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = _seed_show(connection)
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-teaser",
            title="Teaser - bientôt",
            audio_url="https://cdn.example.com/teaser.mp3",
            duration_seconds=3_600,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-best-of",
            title="Best of 2024",
            audio_url="https://cdn.example.com/best-of.mp3",
            duration_seconds=3_600,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-main",
            title="Conversation principale",
            audio_url="https://cdn.example.com/main.mp3",
            duration_seconds=3_600,
        )
        connection.commit()

    service = PilotSelectionService(db_path=db_path)

    result = service.create_pilot(
        show_id=show_id,
        name="zack-title-filter-test",
        target_seconds=3_600,
    )

    assert result.selected_count == 1
    assert result.skipped_count == 2
    assert result.episodes[0].title == "Conversation principale"


def test_pilot_selection_applies_optional_min_duration_floor(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = _seed_show(connection)
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-short",
            title="Episode court",
            audio_url="https://cdn.example.com/short.mp3",
            duration_seconds=1_200,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-long",
            title="Episode long",
            audio_url="https://cdn.example.com/long.mp3",
            duration_seconds=2_400,
        )
        connection.commit()

    service = PilotSelectionService(db_path=db_path)

    result = service.create_pilot(
        show_id=show_id,
        name="zack-duration-floor-test",
        target_seconds=2_400,
        min_duration_seconds=1_800,
    )

    assert result.selected_count == 1
    assert result.skipped_count == 1
    assert result.episodes[0].title == "Episode long"


def test_pilot_selection_replaces_existing_run_without_clobbering_ready_source(
    tmp_path,
) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = _seed_show(connection)
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-1",
            title="Episode 1",
            audio_url="https://cdn.example.com/ep1.mp3",
            duration_seconds=3_600,
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-2",
            title="Episode 2",
            audio_url="https://cdn.example.com/ep2.mp3",
            duration_seconds=3_600,
        )
        episode_id = connection.execute(
            "SELECT episode_id FROM episodes WHERE guid = ?",
            ("ep-1",),
        ).fetchone()["episode_id"]
        connection.execute(
            """
            INSERT INTO transcript_sources (episode_id, source_type, status, model)
            VALUES (?, ?, ?, ?)
            """,
            (episode_id, "asr", "ready", "gpt-4o-mini-transcribe"),
        )
        connection.commit()

    service = PilotSelectionService(db_path=db_path)

    first_result = service.create_pilot(
        show_id=show_id,
        name="zack-replace-test-pilot",
        target_seconds=3_600,
    )
    second_result = service.create_pilot(
        show_id=show_id,
        name="zack-replace-test-pilot",
        target_seconds=7_200,
    )

    with connect(db_path) as connection:
        pilot_episode_count = connection.execute(
            "SELECT COUNT(*) FROM pilot_run_episodes WHERE pilot_run_id = ?",
            (second_result.pilot_run_id,),
        ).fetchone()[0]
        ready_status = connection.execute(
            """
            SELECT status
            FROM transcript_sources
            WHERE episode_id = ?
            AND source_type = 'asr'
            AND model = 'gpt-4o-mini-transcribe'
            """,
            (episode_id,),
        ).fetchone()["status"]

    assert second_result.pilot_run_id == first_result.pilot_run_id
    assert pilot_episode_count == 2
    assert ready_status == "ready"


def test_pilot_selection_errors_when_show_missing(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    service = PilotSelectionService(db_path=db_path)

    try:
        service.create_pilot(show_id=999, name="missing", target_seconds=3_600)
    except PilotSelectionError as exc:
        assert "show_id 999 not found" in str(exc)
    else:
        raise AssertionError("expected PilotSelectionError")


def test_corpus_status_reports_show_and_slice_progress(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_one_id = _seed_show(connection)
        show_two_id = upsert_show(
            connection,
            title="FloodCast",
            feed_url="https://example.com/floodcast.xml",
        )
        upsert_episode(
            connection,
            show_id=show_one_id,
            guid="ep-1",
            title="Episode 1",
            published_at="2025-01-03T00:00:00+00:00",
            audio_url="https://cdn.example.com/ep1.mp3",
            duration_seconds=3_600,
            has_transcript_tag=True,
        )
        upsert_episode(
            connection,
            show_id=show_one_id,
            guid="ep-2",
            title="Episode 2",
            published_at="2025-01-02T00:00:00+00:00",
            audio_url="https://cdn.example.com/ep2.mp3",
            duration_seconds=1_800,
        )
        upsert_episode(
            connection,
            show_id=show_two_id,
            guid="ep-3",
            title="Episode 3",
            published_at="2025-01-01T00:00:00+00:00",
            audio_url="https://cdn.example.com/ep3.mp3",
            duration_seconds=2_700,
        )
        connection.commit()

    selection_service = PilotSelectionService(db_path=db_path)
    result = selection_service.create_pilot(
        show_id=show_one_id,
        name="zack-10h-slice",
        target_seconds=5_400,
    )

    with connect(db_path) as connection:
        episode_rows = connection.execute(
            """
            SELECT episode_id, guid
            FROM episodes
            WHERE show_id = ?
            ORDER BY guid
            """,
            (show_one_id,),
        ).fetchall()
        episode_ids = {row["guid"]: int(row["episode_id"]) for row in episode_rows}
        connection.execute(
            """
            UPDATE transcript_sources
            SET status = 'ready'
            WHERE episode_id = ?
            AND source_type = 'asr'
            AND model = 'gpt-4o-mini-transcribe'
            """,
            (episode_ids["ep-1"],),
        )
        connection.execute(
            """
            UPDATE transcript_sources
            SET status = 'failed'
            WHERE episode_id = ?
            AND source_type = 'asr'
            AND model = 'gpt-4o-mini-transcribe'
            """,
            (episode_ids["ep-2"],),
        )
        connection.commit()

    status_service = CorpusStatusService(db_path=db_path)
    status = status_service.inspect()

    assert status.show_count == 2
    assert status.slice_count == 1
    assert status.episode_count == 3
    assert status.total_seconds == 8_100
    assert status.episodes_with_transcript_tag == 1
    assert status.selected_slice_episodes == 2
    assert status.selected_slice_seconds == 5_400
    assert status.needs_asr_episodes == 0
    assert status.in_progress_asr_episodes == 0
    assert status.ready_asr_episodes == 1
    assert status.failed_asr_episodes == 1

    slice_row = next(row for row in status.rows if row.slice_id == result.pilot_run_id)
    assert slice_row.title == "Zack en Roue Libre by Zack Nani"
    assert slice_row.slice_name == "zack-10h-slice"
    assert slice_row.selected_episodes == 2
    assert slice_row.selected_seconds == 5_400
    assert slice_row.ready_asr_episodes == 1
    assert slice_row.failed_asr_episodes == 1

    no_slice_row = next(row for row in status.rows if row.show_id == show_two_id)
    assert no_slice_row.slice_id is None
    assert no_slice_row.slice_name is None
    assert no_slice_row.selected_episodes == 0
    assert no_slice_row.ready_asr_episodes == 0
