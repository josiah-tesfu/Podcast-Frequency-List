import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_show
from podcast_frequency_list.tokens import CandidateMetricsError, CandidateMetricsService

from ._helpers import _insert_candidate, _insert_episode_context, _insert_occurrence


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

    assert result.inventory_version == "1"
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
