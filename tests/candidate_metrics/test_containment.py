import pytest

from podcast_frequency_list.db import bootstrap_database, connect, upsert_show
from podcast_frequency_list.tokens import INVENTORY_VERSION, CandidateMetricsService

from ._helpers import _insert_candidate, _insert_occurrence_bundle, _load_containment_rows


def test_candidate_metrics_service_refreshes_direct_parent_containment_rows(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        envie_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        ai_envie_id = _insert_candidate(
            connection,
            candidate_key="ai envie",
            display_text="ai envie",
            ngram_size=2,
        )
        envie_de_id = _insert_candidate(
            connection,
            candidate_key="envie de",
            display_text="envie de",
            ngram_size=2,
        )
        j_ai_envie_id = _insert_candidate(
            connection,
            candidate_key="j ai envie",
            display_text="j ai envie",
            ngram_size=3,
        )
        ai_envie_de_id = _insert_candidate(
            connection,
            candidate_key="ai envie de",
            display_text="ai envie de",
            ngram_size=3,
        )

        for index in range(2):
            _insert_occurrence_bundle(
                connection,
                show_id=show_id,
                guid="ep-shared",
                sentence_text="j ai envie",
                source_model=f"test-model-{index}",
                occurrences=(
                    (envie_id, 2, 3, 5, 10, "envie"),
                    (ai_envie_id, 1, 3, 2, 10, "ai envie"),
                    (j_ai_envie_id, 0, 3, 0, 10, "j ai envie"),
                ),
            )

        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-other",
            sentence_text="ai envie de",
            occurrences=(
                (envie_id, 1, 2, 3, 8, "envie"),
                (ai_envie_id, 0, 2, 0, 8, "ai envie"),
                (envie_de_id, 1, 3, 3, 11, "envie de"),
                (ai_envie_de_id, 0, 3, 0, 11, "ai envie de"),
            ),
        )
        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        rows = _load_containment_rows(connection)
        skipped_pair_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM candidate_containment cc
            JOIN token_candidates smaller
                ON smaller.candidate_id = cc.smaller_candidate_id
                AND smaller.inventory_version = cc.inventory_version
            JOIN token_candidates larger
                ON larger.candidate_id = cc.larger_candidate_id
                AND larger.inventory_version = cc.inventory_version
            WHERE cc.inventory_version = ?
            AND smaller.candidate_key = 'envie'
            AND larger.candidate_key = 'j ai envie'
            """,
            (INVENTORY_VERSION,),
        ).fetchone()[0]

    assert rows == (
        {
            "smaller_key": "ai envie",
            "larger_key": "ai envie de",
            "extension_side": "right",
            "shared_occurrence_count": 1,
            "shared_episode_count": 1,
        },
        {
            "smaller_key": "ai envie",
            "larger_key": "j ai envie",
            "extension_side": "left",
            "shared_occurrence_count": 2,
            "shared_episode_count": 1,
        },
        {
            "smaller_key": "envie",
            "larger_key": "ai envie",
            "extension_side": "left",
            "shared_occurrence_count": 3,
            "shared_episode_count": 2,
        },
        {
            "smaller_key": "envie",
            "larger_key": "envie de",
            "extension_side": "right",
            "shared_occurrence_count": 1,
            "shared_episode_count": 1,
        },
        {
            "smaller_key": "envie de",
            "larger_key": "ai envie de",
            "extension_side": "left",
            "shared_occurrence_count": 1,
            "shared_episode_count": 1,
        },
    )
    assert skipped_pair_count == 0


def test_candidate_metrics_service_containment_refresh_replaces_stale_rows(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        envie_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        ai_envie_id = _insert_candidate(
            connection,
            candidate_key="ai envie",
            display_text="ai envie",
            ngram_size=2,
        )
        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-1",
            sentence_text="ai envie",
            occurrences=(
                (envie_id, 1, 2, 3, 8, "envie"),
                (ai_envie_id, 0, 2, 0, 8, "ai envie"),
            ),
        )
        connection.execute(
            """
            INSERT INTO candidate_containment (
                inventory_version,
                smaller_candidate_id,
                larger_candidate_id,
                extension_side,
                shared_occurrence_count,
                shared_episode_count
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (INVENTORY_VERSION, envie_id, ai_envie_id, "left", 99, 99),
        )
        connection.commit()

    CandidateMetricsService(db_path=db_path).refresh()

    with connect(db_path) as connection:
        rows = _load_containment_rows(connection)

    assert rows == (
        {
            "smaller_key": "envie",
            "larger_key": "ai envie",
            "extension_side": "left",
            "shared_occurrence_count": 1,
            "shared_episode_count": 1,
        },
    )


def test_candidate_metrics_service_containment_refresh_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        envie_id = _insert_candidate(
            connection,
            candidate_key="envie",
            display_text="envie",
            ngram_size=1,
        )
        ai_envie_id = _insert_candidate(
            connection,
            candidate_key="ai envie",
            display_text="ai envie",
            ngram_size=2,
        )
        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-1",
            sentence_text="ai envie",
            occurrences=(
                (envie_id, 1, 2, 3, 8, "envie"),
                (ai_envie_id, 0, 2, 0, 8, "ai envie"),
            ),
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()

    with connect(db_path) as connection:
        first_rows = _load_containment_rows(connection)

    service.refresh()

    with connect(db_path) as connection:
        second_rows = _load_containment_rows(connection)

    assert second_rows == first_rows


def test_candidate_metrics_service_containment_refresh_aggregates_both_sides(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)

    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Main Show",
            feed_url="https://example.com/main.xml",
        )
        de_id = _insert_candidate(
            connection,
            candidate_key="de",
            display_text="de",
            ngram_size=1,
        )
        de_de_id = _insert_candidate(
            connection,
            candidate_key="de de",
            display_text="de de",
            ngram_size=2,
        )
        _insert_occurrence_bundle(
            connection,
            show_id=show_id,
            guid="ep-1",
            sentence_text="de de",
            occurrences=(
                (de_id, 0, 1, 0, 2, "de"),
                (de_id, 1, 2, 3, 5, "de"),
                (de_de_id, 0, 2, 0, 5, "de de"),
            ),
        )
        connection.commit()

    service = CandidateMetricsService(db_path=db_path)
    service.refresh()

    with connect(db_path) as connection:
        rows = _load_containment_rows(connection)

    summary = service.list_candidates_by_key(candidate_keys=("de",))[0]

    assert rows == (
        {
            "smaller_key": "de",
            "larger_key": "de de",
            "extension_side": "both",
            "shared_occurrence_count": 2,
            "shared_episode_count": 1,
        },
    )
    assert summary.covered_by_any_count == 2
    assert summary.covered_by_any_ratio == pytest.approx(1.0)
    assert summary.independent_occurrence_count == 0
    assert summary.direct_parent_count == 1
    assert summary.dominant_parent_key == "de de"
    assert summary.dominant_parent_shared_count == 2
    assert summary.dominant_parent_share == pytest.approx(1.0)
    assert summary.dominant_parent_side == "both"
