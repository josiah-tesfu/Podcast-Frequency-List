import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.tokens import (
    INVENTORY_VERSION,
    CandidateMetricsError,
    CandidateMetricsService,
)
from podcast_frequency_list.tokens.models import CandidateMetricsValidationResult


def _insert_episode_context(connection, *, show_id: int, guid: str) -> tuple[int, int, int]:
    upsert_episode(
        connection,
        show_id=show_id,
        guid=guid,
        title=f"Episode {guid}",
        audio_url=f"https://cdn.example.com/{guid}.mp3",
    )
    episode_id = int(
        connection.execute(
            """
            SELECT episode_id
            FROM episodes
            WHERE show_id = ?
            AND guid = ?
            """,
            (show_id, guid),
        ).fetchone()["episode_id"]
    )
    source_id = int(
        connection.execute(
            """
            INSERT INTO transcript_sources (
                episode_id,
                source_type,
                status,
                model
            )
            VALUES (?, 'asr', 'ready', 'test-model')
            """,
            (episode_id,),
        ).lastrowid
    )
    segment_id = int(
        connection.execute(
            """
            INSERT INTO transcript_segments (
                source_id,
                episode_id,
                chunk_index,
                raw_text
            )
            VALUES (?, ?, 0, ?)
            """,
            (source_id, episode_id, "en fait"),
        ).lastrowid
    )
    sentence_id = int(
        connection.execute(
            """
            INSERT INTO segment_sentences (
                segment_id,
                episode_id,
                split_version,
                sentence_index,
                char_start,
                char_end,
                sentence_text
            )
            VALUES (?, ?, '1', 0, 0, 7, ?)
            """,
            (segment_id, episode_id, "en fait"),
        ).lastrowid
    )
    return episode_id, segment_id, sentence_id


def _insert_candidate(
    connection,
    *,
    candidate_key: str,
    display_text: str,
    ngram_size: int = 2,
    inventory_version: str = INVENTORY_VERSION,
    raw_frequency: int = 99,
    episode_dispersion: int = 99,
    show_dispersion: int = 99,
) -> int:
    return int(
        connection.execute(
            """
            INSERT INTO token_candidates (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                inventory_version,
                candidate_key,
                display_text,
                ngram_size,
                raw_frequency,
                episode_dispersion,
                show_dispersion,
            ),
        ).lastrowid
    )


def _insert_occurrence(
    connection,
    *,
    candidate_id: int,
    sentence_id: int,
    episode_id: int,
    segment_id: int,
    surface_text: str,
    inventory_version: str = INVENTORY_VERSION,
) -> None:
    connection.execute(
        """
        INSERT INTO token_occurrences (
            candidate_id,
            sentence_id,
            episode_id,
            segment_id,
            inventory_version,
            token_start_index,
            token_end_index,
            char_start,
            char_end,
            surface_text
        )
        VALUES (?, ?, ?, ?, ?, 0, 2, 0, 7, ?)
        """,
        (
            candidate_id,
            sentence_id,
            episode_id,
            segment_id,
            inventory_version,
            surface_text,
        ),
    )


def test_candidate_metrics_service_refreshes_metrics_and_display_text(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        other_show_id = upsert_show(
            connection,
            title="Other Show",
            feed_url="https://example.com/other.xml",
        )
        contexts = (
            _insert_episode_context(connection, show_id=show_id, guid="ep-1"),
            _insert_episode_context(connection, show_id=show_id, guid="ep-2"),
            _insert_episode_context(connection, show_id=other_show_id, guid="ep-3"),
            _insert_episode_context(connection, show_id=show_id, guid="ep-4"),
            _insert_episode_context(connection, show_id=show_id, guid="ep-5"),
            _insert_episode_context(connection, show_id=show_id, guid="ep-6"),
        )
        en_fait_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="En fait",
        )
        du_coup_id = _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="Du coup",
        )
        orphan_id = _insert_candidate(
            connection,
            candidate_key="orphan",
            display_text="orphan",
        )
        other_version_orphan_id = _insert_candidate(
            connection,
            candidate_key="other version orphan",
            display_text="other version orphan",
            inventory_version="2",
        )

        for context, surface_text in zip(
            contexts[:4],
            ("En fait", "en fait", "En fait", "en fait"),
            strict=True,
        ):
            episode_id, segment_id, sentence_id = context
            _insert_occurrence(
                connection,
                candidate_id=en_fait_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                surface_text=surface_text,
            )

        for context in contexts[4:]:
            episode_id, segment_id, sentence_id = context
            _insert_occurrence(
                connection,
                candidate_id=du_coup_id,
                sentence_id=sentence_id,
                episode_id=episode_id,
                segment_id=segment_id,
                surface_text="du coup",
            )

        connection.commit()

    result = CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        en_fait = connection.execute(
            """
            SELECT raw_frequency, episode_dispersion, show_dispersion, display_text
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (en_fait_id,),
        ).fetchone()
        du_coup = connection.execute(
            """
            SELECT raw_frequency, episode_dispersion, show_dispersion, display_text
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (du_coup_id,),
        ).fetchone()
        orphan_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (orphan_id,),
        ).fetchone()[0]
        other_version_orphan_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM token_candidates
            WHERE candidate_id = ?
            """,
            (other_version_orphan_id,),
        ).fetchone()[0]

    assert result.inventory_version == INVENTORY_VERSION
    assert result.selected_candidates == 3
    assert result.refreshed_candidates == 2
    assert result.deleted_orphan_candidates == 1
    assert result.occurrence_count == 6
    assert result.raw_frequency_total == 6
    assert result.episode_dispersion_total == 6
    assert result.show_dispersion_total == 3
    assert result.display_text_updates == 2

    assert en_fait["raw_frequency"] == 4
    assert en_fait["episode_dispersion"] == 4
    assert en_fait["show_dispersion"] == 2
    assert en_fait["display_text"] == "en fait"
    assert du_coup["raw_frequency"] == 2
    assert du_coup["episode_dispersion"] == 2
    assert du_coup["show_dispersion"] == 1
    assert du_coup["display_text"] == "du coup"
    assert orphan_count == 0
    assert other_version_orphan_count == 1


def test_candidate_metrics_service_refresh_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        episode_id, segment_id, sentence_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-1",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="tu vois",
            display_text="Tu vois",
        )
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            surface_text="tu vois",
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    first_result = service.refresh()
    second_result = service.refresh()

    assert first_result.selected_candidates == 1
    assert first_result.display_text_updates == 1
    assert second_result.selected_candidates == 1
    assert second_result.refreshed_candidates == 1
    assert second_result.deleted_orphan_candidates == 0
    assert second_result.occurrence_count == 1
    assert second_result.raw_frequency_total == 1
    assert second_result.display_text_updates == 0


def test_candidate_metrics_service_errors_without_candidates(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    service = CandidateMetricsService(db_path=db_path)

    with pytest.raises(CandidateMetricsError, match="no token candidates"):
        service.refresh()


def test_candidate_metrics_service_lists_top_candidates_by_ngram(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="alpha",
            display_text="alpha",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=1,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_key="beta",
            display_text="beta",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_key="gamma",
            display_text="gamma",
            ngram_size=2,
            raw_frequency=10,
            episode_dispersion=3,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_key="delta",
            display_text="delta",
            ngram_size=1,
            raw_frequency=99,
            episode_dispersion=1,
            show_dispersion=1,
        )
        _insert_candidate(
            connection,
            candidate_key="epsilon",
            display_text="epsilon",
            ngram_size=2,
            inventory_version="2",
            raw_frequency=100,
            episode_dispersion=100,
            show_dispersion=1,
        )
        connection.commit()

    rows = CandidateMetricsService(db_path=db_path).list_top_candidates(
        ngram_size=2,
        limit=2,
    )

    assert [row.candidate_key for row in rows] == ["beta", "gamma"]
    assert rows[0].display_text == "beta"
    assert rows[0].ngram_size == 2
    assert rows[0].raw_frequency == 10
    assert rows[0].episode_dispersion == 3
    assert rows[0].show_dispersion == 1


def test_candidate_metrics_service_summary_empty_bucket_returns_empty_tuple(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="alpha",
            display_text="alpha",
            ngram_size=1,
        )
        connection.commit()

    rows = CandidateMetricsService(db_path=db_path).list_top_candidates(
        ngram_size=3,
        limit=20,
    )

    assert rows == ()


def test_candidate_metrics_service_summary_validates_arguments(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    service = CandidateMetricsService(db_path=db_path)

    with pytest.raises(CandidateMetricsError, match="ngram_size"):
        service.list_top_candidates(ngram_size=0)

    with pytest.raises(CandidateMetricsError, match="limit"):
        service.list_top_candidates(ngram_size=1, limit=0)


def test_candidate_metrics_service_validate_detects_stale_metrics(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        other_show_id = upsert_show(
            connection,
            title="Other Show",
            feed_url="https://example.com/other.xml",
        )
        episode_id, segment_id, sentence_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-1",
        )
        other_episode_id, other_segment_id, other_sentence_id = _insert_episode_context(
            connection,
            show_id=other_show_id,
            guid="ep-2",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="En fait",
            raw_frequency=99,
            episode_dispersion=99,
            show_dispersion=99,
        )
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            surface_text="En fait",
        )
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=other_sentence_id,
            episode_id=other_episode_id,
            segment_id=other_segment_id,
            surface_text="en fait",
        )
        connection.commit()

    result = CandidateMetricsService(db_path=db_path).validate()

    assert result == CandidateMetricsValidationResult(
        inventory_version=INVENTORY_VERSION,
        candidate_count=1,
        occurrence_count=2,
        raw_frequency_mismatch_count=1,
        episode_dispersion_mismatch_count=1,
        show_dispersion_mismatch_count=1,
        display_text_mismatch_count=1,
        foreign_key_issue_count=0,
    )


def test_candidate_metrics_service_validate_reports_clean_state_after_refresh(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        episode_id, segment_id, sentence_id = _insert_episode_context(
            connection,
            show_id=show_id,
            guid="ep-1",
        )
        candidate_id = _insert_candidate(
            connection,
            candidate_key="tu vois",
            display_text="Tu vois",
        )
        _insert_occurrence(
            connection,
            candidate_id=candidate_id,
            sentence_id=sentence_id,
            episode_id=episode_id,
            segment_id=segment_id,
            surface_text="tu vois",
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()
    result = service.validate()

    assert result == CandidateMetricsValidationResult(
        inventory_version=INVENTORY_VERSION,
        candidate_count=1,
        occurrence_count=1,
        raw_frequency_mismatch_count=0,
        episode_dispersion_mismatch_count=0,
        show_dispersion_mismatch_count=0,
        display_text_mismatch_count=0,
        foreign_key_issue_count=0,
    )


def test_candidate_metrics_service_lists_candidates_by_key_in_requested_order(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        _insert_candidate(
            connection,
            candidate_key="en fait",
            display_text="en fait",
            ngram_size=2,
            raw_frequency=12,
            episode_dispersion=4,
            show_dispersion=2,
        )
        _insert_candidate(
            connection,
            candidate_key="du coup",
            display_text="du coup",
            ngram_size=2,
            raw_frequency=9,
            episode_dispersion=5,
            show_dispersion=2,
        )
        _insert_candidate(
            connection,
            candidate_key="il y a",
            display_text="il y a",
            ngram_size=3,
            raw_frequency=8,
            episode_dispersion=6,
            show_dispersion=3,
        )
        connection.commit()

    rows = CandidateMetricsService(db_path=db_path).list_candidates_by_key(
        candidate_keys=("il y a", "missing", "du coup", "il y a", "  "),
    )

    assert [row.candidate_key for row in rows] == ["il y a", "du coup"]
    assert rows[0].display_text == "il y a"
    assert rows[1].raw_frequency == 9
