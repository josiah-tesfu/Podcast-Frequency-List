from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.pilot import PilotSelectionService
from podcast_frequency_list.qc import QC_VERSION, SegmentQcError, SegmentQcService


def _seed_qc_data(db_path):
    bootstrap_database(db_path)
    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Zack en Roue Libre by Zack Nani",
            feed_url="https://example.com/zack.xml",
        )

        episode_ids: list[int] = []
        topic_words = {
            1: "football",
            2: "cinema",
            3: "internet",
        }
        for index in range(1, 4):
            upsert_episode(
                connection,
                show_id=show_id,
                guid=f"ep-{index}",
                title=f"Episode {index}",
                audio_url=f"https://cdn.example.com/ep{index}.mp3",
                duration_seconds=180,
            )
            episode_id = connection.execute(
                "SELECT episode_id FROM episodes WHERE guid = ?",
                (f"ep-{index}",),
            ).fetchone()["episode_id"]
            episode_ids.append(episode_id)

            cursor = connection.execute(
                """
                INSERT INTO transcript_sources (
                    episode_id,
                    source_type,
                    status,
                    model,
                    raw_path
                )
                VALUES (?, 'asr', 'ready', 'gpt-4o-mini-transcribe', ?)
                """,
                (episode_id, f"/tmp/episode-{index}.txt"),
            )
            source_id = int(cursor.lastrowid)

            intro_text = (
                f"Bonjour tout le monde, j'espère que vous allez bien. "
                f"On se retrouve aujourd'hui pour un {23 - index}e épisode de la saison."
            )
            middle_text = f"Contenu utile sur {topic_words[index]} et la création."
            review_text = (
                "oui vraiment oui clairement oui franchement oui honnêtement "
                "oui exactement oui totalement bon"
            )
            outro_text = "Merci à tous d'avoir suivi l'émission, replay demain et à bientôt, ciao."
            tail_text = "Et là la discussion continue normalement jusqu'à la fin."

            chunk_texts = [
                intro_text,
                middle_text,
                review_text if index == 3 else middle_text + " Suite naturelle.",
                outro_text if index in (1, 2) else tail_text,
            ]

            for chunk_index, chunk_text in enumerate(chunk_texts):
                segment_cursor = connection.execute(
                    """
                    INSERT INTO transcript_segments (
                        source_id,
                        episode_id,
                        chunk_index,
                        start_ms,
                        end_ms,
                        raw_text
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source_id,
                        episode_id,
                        chunk_index,
                        chunk_index * 10_000,
                        (chunk_index + 1) * 10_000,
                        chunk_text,
                    ),
                )
                segment_id = int(segment_cursor.lastrowid)
                connection.execute(
                    """
                    INSERT INTO normalized_segments (
                        segment_id,
                        episode_id,
                        normalization_version,
                        normalized_text
                    )
                    VALUES (?, ?, '1', ?)
                    """,
                    (segment_id, episode_id, chunk_text),
                )

        connection.commit()

    PilotSelectionService(db_path=db_path).create_pilot(
        show_id=show_id,
        name="zack-qc-pilot",
        target_seconds=540,
    )

    return episode_ids


def test_segment_qc_service_flags_intro_outro_and_artifact(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_ids = _seed_qc_data(db_path)
    service = SegmentQcService(db_path=db_path)

    result = service.run(pilot_name="zack-qc-pilot")

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT sq.episode_id, ts.chunk_index, sq.status, sq.reason_summary
            FROM segment_qc sq
            JOIN transcript_segments ts
                ON ts.segment_id = sq.segment_id
            WHERE sq.qc_version = ?
            ORDER BY sq.episode_id, ts.chunk_index
            """,
            (QC_VERSION,),
        ).fetchall()
        flag_rows = connection.execute(
            """
            SELECT sq.episode_id, ts.chunk_index, qf.flag
            FROM segment_qc_flags qf
            JOIN transcript_segments ts
                ON ts.segment_id = qf.segment_id
            JOIN segment_qc sq
                ON sq.segment_id = qf.segment_id
                AND sq.qc_version = qf.qc_version
            WHERE qf.qc_version = ?
            ORDER BY sq.episode_id, ts.chunk_index, qf.flag
            """,
            (QC_VERSION,),
        ).fetchall()

    assert result.scope == "pilot"
    assert result.scope_value == "zack-qc-pilot"
    assert result.selected_segments == 12
    assert result.processed_segments == 12
    assert result.skipped_segments == 0
    assert result.keep_segments == 6
    assert result.review_segments == 1
    assert result.remove_segments == 5

    statuses = {
        (row["episode_id"], row["chunk_index"]): row["status"]
        for row in rows
    }
    assert statuses[(episode_ids[0], 0)] == "remove"
    assert statuses[(episode_ids[1], 0)] == "remove"
    assert statuses[(episode_ids[2], 0)] == "remove"
    assert statuses[(episode_ids[0], 3)] == "remove"
    assert statuses[(episode_ids[1], 3)] == "remove"
    assert statuses[(episode_ids[2], 2)] == "review"
    assert statuses[(episode_ids[2], 3)] == "keep"

    flags = {
        (row["episode_id"], row["chunk_index"], row["flag"])
        for row in flag_rows
    }
    assert (episode_ids[0], 0, "intro_boilerplate") in flags
    assert (episode_ids[1], 3, "outro_boilerplate") in flags
    assert (episode_ids[2], 2, "asr_artifact") in flags


def test_segment_qc_service_skips_current_version_without_force(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    episode_ids = _seed_qc_data(db_path)
    service = SegmentQcService(db_path=db_path)

    first_result = service.run(episode_id=episode_ids[0])
    second_result = service.run(episode_id=episode_ids[0])

    with connect(db_path) as connection:
        row_count = connection.execute(
            "SELECT COUNT(*) FROM segment_qc WHERE episode_id = ? AND qc_version = ?",
            (episode_ids[0], QC_VERSION),
        ).fetchone()[0]

    assert first_result.processed_segments == 4
    assert second_result.processed_segments == 0
    assert second_result.skipped_segments == 4
    assert row_count == 4


def test_segment_qc_service_requires_exactly_one_scope(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    service = SegmentQcService(db_path=db_path)

    try:
        service.run()
    except SegmentQcError as exc:
        assert "exactly one" in str(exc)
    else:
        raise AssertionError("expected SegmentQcError")
