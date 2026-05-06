import sqlite3

from typer.testing import CliRunner

from podcast_frequency_list.asr.models import AsrEpisodeResult, AsrRunResult
from podcast_frequency_list.cli import app
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.discovery.models import SavedShow
from podcast_frequency_list.ingest.models import SyncFeedResult
from podcast_frequency_list.normalize.models import NormalizationRunResult
from podcast_frequency_list.pilot.models import PilotEpisode, PilotSelectionResult
from podcast_frequency_list.qc.models import QcRunResult
from podcast_frequency_list.sentences.models import SentenceSplitResult
from podcast_frequency_list.tokens import CandidateInventoryError
from podcast_frequency_list.tokens.models import (
    CandidateInventoryResult,
    CandidateMetricsResult,
    CandidateMetricsValidationResult,
    CandidateScoresResult,
    CandidateSummaryRow,
    TokenizationResult,
)

runner = CliRunner()


class FakeDiscoveryService:
    def __init__(self) -> None:
        self.manual_feed_url: str | None = None
        self.manual_title: str | None = None
        self.manual_language: str | None = None
        self.manual_bucket: str | None = None

    def save_manual_feed(
        self,
        *,
        feed_url: str,
        title: str | None = None,
        language: str | None = None,
        bucket: str | None = None,
    ) -> SavedShow:
        self.manual_feed_url = feed_url
        self.manual_title = title
        self.manual_language = language
        self.manual_bucket = bucket
        return SavedShow(show_id=8, title=title or "Detected Title", feed_url=feed_url)

    def close(self) -> None:
        return None


class FakeSyncFeedService:
    def __init__(self) -> None:
        self.show_id: int | None = None
        self.limit: int | None = None

    def sync_show(self, *, show_id: int, limit: int | None = None) -> SyncFeedResult:
        self.show_id = show_id
        self.limit = limit
        return SyncFeedResult(
            show_id=show_id,
            title="Zack en Roue Libre by Zack Nani",
            episodes_seen=10,
            episodes_inserted=8,
            episodes_updated=2,
            episodes_skipped_no_audio=1,
            episodes_with_transcript_tag=0,
        )

    def close(self) -> None:
        return None


class FakePilotSelectionService:
    def __init__(self) -> None:
        self.show_id: int | None = None
        self.name: str | None = None
        self.target_seconds: int | None = None
        self.selection_order: str | None = None
        self.notes: str | None = None

    def create_pilot(
        self,
        *,
        show_id: int,
        name: str,
        target_seconds: int,
        selection_order: str = "newest",
        notes: str | None = None,
    ) -> PilotSelectionResult:
        self.show_id = show_id
        self.name = name
        self.target_seconds = target_seconds
        self.selection_order = selection_order
        self.notes = notes
        return PilotSelectionResult(
            pilot_run_id=3,
            name=name,
            show_id=show_id,
            show_title="Zack en Roue Libre by Zack Nani",
            target_seconds=target_seconds,
            total_seconds=36_600,
            selected_count=4,
            skipped_count=1,
            estimated_cost_usd=1.83,
            model="gpt-4o-mini-transcribe",
            selection_order=selection_order,
            first_published_at="2025-02-01T00:00:00+00:00",
            last_published_at="2025-01-01T00:00:00+00:00",
            episodes=(
                PilotEpisode(
                    episode_id=1,
                    title="Episode 1",
                    published_at="2025-02-01T00:00:00+00:00",
                    duration_seconds=9_000,
                    cumulative_seconds=9_000,
                ),
            ),
        )


class FakeAsrRunService:
    def __init__(self) -> None:
        self.pilot_name: str | None = None
        self.limit: int | None = None
        self.force: bool | None = None

    def run_pilot(
        self,
        *,
        pilot_name: str,
        limit: int | None = None,
        force: bool = False,
    ) -> AsrRunResult:
        self.pilot_name = pilot_name
        self.limit = limit
        self.force = force
        return AsrRunResult(
            pilot_name=pilot_name,
            model="gpt-4o-mini-transcribe",
            requested_limit=limit,
            selected_count=1,
            completed_count=1,
            skipped_count=0,
            failed_count=0,
            chunk_count=2,
            episode_results=(
                AsrEpisodeResult(
                    episode_id=1,
                    title="Episode 1",
                    status="ready",
                    audio_path=None,
                    transcript_path=None,
                    chunk_count=2,
                    text_chars=42,
                    preview="bonjour tout le monde",
                ),
            ),
        )

    def close(self) -> None:
        return None


class FakeTranscriptNormalizationService:
    def __init__(self) -> None:
        self.pilot_name: str | None = None
        self.episode_id: int | None = None
        self.force: bool | None = None

    def normalize(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> NormalizationRunResult:
        self.pilot_name = pilot_name
        self.episode_id = episode_id
        self.force = force
        return NormalizationRunResult(
            scope="pilot",
            scope_value=pilot_name or "",
            normalization_version="1",
            selected_segments=6,
            normalized_segments=6,
            skipped_segments=0,
            episode_count=2,
        )


class FakeSegmentQcService:
    def __init__(self) -> None:
        self.pilot_name: str | None = None
        self.episode_id: int | None = None
        self.force: bool | None = None

    def run(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> QcRunResult:
        self.pilot_name = pilot_name
        self.episode_id = episode_id
        self.force = force
        return QcRunResult(
            scope="pilot",
            scope_value=pilot_name or "",
            qc_version="1",
            selected_segments=6,
            processed_segments=6,
            skipped_segments=0,
            keep_segments=4,
            review_segments=1,
            remove_segments=1,
        )


class FakeSentenceSplitService:
    def __init__(self) -> None:
        self.pilot_name: str | None = None
        self.episode_id: int | None = None
        self.force: bool | None = None

    def split(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> SentenceSplitResult:
        self.pilot_name = pilot_name
        self.episode_id = episode_id
        self.force = force
        return SentenceSplitResult(
            scope="pilot",
            scope_value=pilot_name or "",
            split_version="1",
            selected_segments=5,
            created_sentences=14,
            skipped_segments=0,
            episode_count=2,
        )


class FakeSentenceTokenizationService:
    def __init__(self) -> None:
        self.pilot_name: str | None = None
        self.episode_id: int | None = None
        self.force: bool | None = None

    def tokenize(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> TokenizationResult:
        self.pilot_name = pilot_name
        self.episode_id = episode_id
        self.force = force
        return TokenizationResult(
            scope="pilot",
            scope_value=pilot_name or "",
            tokenization_version="1",
            selected_sentences=12,
            tokenized_sentences=12,
            created_tokens=92,
            skipped_sentences=0,
            episode_count=2,
        )


class FakeCandidateInventoryService:
    def __init__(self) -> None:
        self.pilot_name: str | None = None
        self.episode_id: int | None = None
        self.force: bool | None = None

    def generate(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> CandidateInventoryResult:
        self.pilot_name = pilot_name
        self.episode_id = episode_id
        self.force = force
        if (pilot_name is None) == (episode_id is None):
            raise CandidateInventoryError("provide exactly one of pilot_name or episode_id")
        return CandidateInventoryResult(
            scope="pilot" if pilot_name is not None else "episode",
            scope_value=pilot_name or str(episode_id),
            inventory_version="1",
            selected_sentences=12,
            processed_sentences=12,
            skipped_sentences=0,
            created_candidates=23,
            created_occurrences=92,
            episode_count=2,
        )


class FakeCandidateMetricsService:
    def __init__(self) -> None:
        self.refresh_calls = 0
        self.validation_calls = 0
        self.top_candidate_requests: list[tuple[int, int]] = []
        self.focus_candidate_requests: list[tuple[str, ...]] = []

    def refresh(self) -> CandidateMetricsResult:
        self.refresh_calls += 1
        return CandidateMetricsResult(
            inventory_version="1",
            selected_candidates=23,
            refreshed_candidates=22,
            deleted_orphan_candidates=1,
            occurrence_count=92,
            raw_frequency_total=92,
            episode_dispersion_total=40,
            show_dispersion_total=22,
            display_text_updates=5,
        )

    def validate(self) -> CandidateMetricsValidationResult:
        self.validation_calls += 1
        return CandidateMetricsValidationResult(
            inventory_version="1",
            candidate_count=22,
            occurrence_count=92,
            raw_frequency_mismatch_count=0,
            episode_dispersion_mismatch_count=0,
            show_dispersion_mismatch_count=0,
            display_text_mismatch_count=0,
            foreign_key_issue_count=0,
        )

    def list_top_candidates(
        self,
        *,
        ngram_size: int,
        limit: int = 20,
        inventory_version: str = "1",
    ) -> tuple[CandidateSummaryRow, ...]:
        self.top_candidate_requests.append((ngram_size, limit))
        return (
            CandidateSummaryRow(
                candidate_key=f"candidate-{ngram_size}",
                display_text=f"candidate {ngram_size}",
                ngram_size=ngram_size,
                raw_frequency=10 * ngram_size,
                episode_dispersion=ngram_size + 1,
                show_dispersion=1,
                t_score=None if ngram_size == 1 else 2.5 * ngram_size,
                npmi=None if ngram_size == 1 else 0.5 + (0.1 * ngram_size),
                left_context_type_count=None if ngram_size == 1 else ngram_size,
                right_context_type_count=None if ngram_size == 1 else ngram_size + 1,
                left_entropy=None if ngram_size == 1 else 0.2 * ngram_size,
                right_entropy=None if ngram_size == 1 else 0.3 * ngram_size,
                covered_by_any_count=None if ngram_size == 3 else ngram_size,
                covered_by_any_ratio=None if ngram_size == 3 else 0.25 * ngram_size,
                independent_occurrence_count=None if ngram_size == 3 else 9 * ngram_size,
                direct_parent_count=None if ngram_size == 3 else ngram_size + 1,
                dominant_parent_key=None if ngram_size == 3 else f"parent-{ngram_size}",
                dominant_parent_shared_count=None if ngram_size == 3 else ngram_size + 2,
                dominant_parent_share=None if ngram_size == 3 else 0.15 * ngram_size,
                dominant_parent_side=None if ngram_size == 3 else "left",
            ),
        )

    def list_candidates_by_key(
        self,
        *,
        candidate_keys: tuple[str, ...] | list[str],
        inventory_version: str = "1",
    ) -> tuple[CandidateSummaryRow, ...]:
        requested_keys = tuple(candidate_keys)
        self.focus_candidate_requests.append(requested_keys)

        rows: list[CandidateSummaryRow] = []
        for key in requested_keys:
            if key == "missing":
                continue
            rows.append(
                CandidateSummaryRow(
                    candidate_key=key,
                    display_text=key,
                    ngram_size=max(1, len(key.split())),
                    raw_frequency=7,
                    episode_dispersion=3,
                    show_dispersion=1,
                    t_score=4.2 if len(key.split()) >= 2 else None,
                    npmi=0.77 if len(key.split()) >= 2 else None,
                    left_context_type_count=2 if len(key.split()) >= 2 else None,
                    right_context_type_count=3 if len(key.split()) >= 2 else None,
                    left_entropy=0.41 if len(key.split()) >= 2 else None,
                    right_entropy=0.92 if len(key.split()) >= 2 else None,
                    covered_by_any_count=4 if len(key.split()) < 3 else None,
                    covered_by_any_ratio=0.57 if len(key.split()) < 3 else None,
                    independent_occurrence_count=3 if len(key.split()) < 3 else None,
                    direct_parent_count=2 if len(key.split()) < 3 else None,
                    dominant_parent_key="je en fait" if len(key.split()) < 3 else None,
                    dominant_parent_shared_count=3 if len(key.split()) < 3 else None,
                    dominant_parent_share=0.43 if len(key.split()) < 3 else None,
                    dominant_parent_side="left" if len(key.split()) < 3 else None,
                )
            )
        return tuple(rows)

    def close(self) -> None:
        return None


class FakeCandidateScoresService:
    def __init__(self) -> None:
        self.refresh_calls = 0
        self.summary_calls = 0
        self.top_candidate_requests: list[tuple[int, int]] = []
        self.global_candidate_requests: list[int] = []
        self.focus_candidate_requests: list[tuple[str, ...]] = []

    def refresh(self) -> CandidateScoresResult:
        self.refresh_calls += 1
        return CandidateScoresResult(
            inventory_version="1",
            score_version="pilot-v1",
            selected_candidates=49_542,
            stored_candidates=49_542,
            eligible_candidates=804,
            eligible_1gram_candidates=205,
            eligible_2gram_candidates=453,
            eligible_3gram_candidates=146,
        )

    def summarize(self) -> CandidateScoresResult:
        self.summary_calls += 1
        return CandidateScoresResult(
            inventory_version="1",
            score_version="pilot-v1",
            selected_candidates=49_542,
            stored_candidates=49_542,
            eligible_candidates=804,
            eligible_1gram_candidates=205,
            eligible_2gram_candidates=453,
            eligible_3gram_candidates=146,
        )

    def list_top_candidates(
        self,
        *,
        ngram_size: int,
        limit: int = 20,
        inventory_version: str = "1",
        score_version: str = "pilot-v1",
    ) -> tuple[CandidateSummaryRow, ...]:
        self.top_candidate_requests.append((ngram_size, limit))
        return (
            CandidateSummaryRow(
                candidate_key=f"score-candidate-{ngram_size}",
                display_text=f"score candidate {ngram_size}",
                ngram_size=ngram_size,
                raw_frequency=10 * ngram_size,
                episode_dispersion=ngram_size + 1,
                show_dispersion=1,
                t_score=None if ngram_size == 1 else 2.5 * ngram_size,
                npmi=None if ngram_size == 1 else 0.5 + (0.1 * ngram_size),
                left_context_type_count=None if ngram_size == 1 else ngram_size,
                right_context_type_count=None if ngram_size == 1 else ngram_size + 1,
                left_entropy=None if ngram_size == 1 else 0.2 * ngram_size,
                right_entropy=None if ngram_size == 1 else 0.3 * ngram_size,
                covered_by_any_count=None if ngram_size == 3 else ngram_size,
                covered_by_any_ratio=None if ngram_size == 3 else 0.25 * ngram_size,
                independent_occurrence_count=None if ngram_size == 3 else 9 * ngram_size,
                direct_parent_count=None if ngram_size == 3 else ngram_size + 1,
                dominant_parent_key=None if ngram_size == 3 else f"parent-{ngram_size}",
                dominant_parent_shared_count=None if ngram_size == 3 else ngram_size + 2,
                dominant_parent_share=None if ngram_size == 3 else 0.15 * ngram_size,
                dominant_parent_side=None if ngram_size == 3 else "left",
                score_version=score_version,
                ranking_lane=f"{ngram_size}gram",
                is_eligible=1,
                frequency_score=0.3 * ngram_size,
                dispersion_score=0.2 * ngram_size,
                association_score=None if ngram_size == 1 else 0.4 * ngram_size,
                boundary_score=None if ngram_size == 1 else 0.1 * ngram_size,
                redundancy_penalty=0.0,
                final_score=0.5 * ngram_size,
                lane_rank=1,
            ),
        )

    def list_global_candidates(
        self,
        *,
        limit: int = 20,
        inventory_version: str = "1",
        score_version: str = "pilot-v1",
    ) -> tuple[CandidateSummaryRow, ...]:
        self.global_candidate_requests.append(limit)
        return (
            CandidateSummaryRow(
                candidate_key="global-candidate-1",
                display_text="global candidate 1",
                ngram_size=1,
                raw_frequency=50,
                episode_dispersion=6,
                show_dispersion=1,
                covered_by_any_count=50,
                covered_by_any_ratio=1.0,
                independent_occurrence_count=0,
                direct_parent_count=10,
                dominant_parent_key="de la",
                dominant_parent_shared_count=5,
                dominant_parent_share=0.1,
                dominant_parent_side="right",
                score_version=score_version,
                ranking_lane="1gram",
                is_eligible=1,
                frequency_score=0.9,
                dispersion_score=1.0,
                association_score=None,
                boundary_score=None,
                redundancy_penalty=0.0,
                final_score=0.95,
                lane_rank=2,
            ),
            CandidateSummaryRow(
                candidate_key="global-candidate-2",
                display_text="global candidate 2",
                ngram_size=2,
                raw_frequency=40,
                episode_dispersion=5,
                show_dispersion=1,
                t_score=6.0,
                npmi=0.8,
                left_context_type_count=3,
                right_context_type_count=4,
                left_entropy=0.7,
                right_entropy=0.8,
                covered_by_any_count=40,
                covered_by_any_ratio=1.0,
                independent_occurrence_count=0,
                direct_parent_count=6,
                dominant_parent_key="c est un",
                dominant_parent_shared_count=8,
                dominant_parent_share=0.2,
                dominant_parent_side="right",
                score_version=score_version,
                ranking_lane="2gram",
                is_eligible=1,
                frequency_score=0.8,
                dispersion_score=0.9,
                association_score=0.85,
                boundary_score=0.75,
                redundancy_penalty=0.0,
                final_score=0.9,
                lane_rank=1,
            ),
        )[:limit]

    def list_candidates_by_key(
        self,
        *,
        candidate_keys: tuple[str, ...] | list[str],
        inventory_version: str = "1",
        score_version: str = "pilot-v1",
    ) -> tuple[CandidateSummaryRow, ...]:
        requested_keys = tuple(candidate_keys)
        self.focus_candidate_requests.append(requested_keys)

        rows: list[CandidateSummaryRow] = []
        for key in requested_keys:
            if key == "missing":
                continue
            ngram_size = max(1, len(key.split()))
            rows.append(
                CandidateSummaryRow(
                    candidate_key=key,
                    display_text=key,
                    ngram_size=ngram_size,
                    raw_frequency=7,
                    episode_dispersion=3,
                    show_dispersion=1,
                    t_score=4.2 if ngram_size >= 2 else None,
                    npmi=0.77 if ngram_size >= 2 else None,
                    left_context_type_count=2 if ngram_size >= 2 else None,
                    right_context_type_count=3 if ngram_size >= 2 else None,
                    left_entropy=0.41 if ngram_size >= 2 else None,
                    right_entropy=0.92 if ngram_size >= 2 else None,
                    covered_by_any_count=4 if ngram_size < 3 else None,
                    covered_by_any_ratio=0.57 if ngram_size < 3 else None,
                    independent_occurrence_count=3 if ngram_size < 3 else None,
                    direct_parent_count=2 if ngram_size < 3 else None,
                    dominant_parent_key="je en fait" if ngram_size < 3 else None,
                    dominant_parent_shared_count=3 if ngram_size < 3 else None,
                    dominant_parent_share=0.43 if ngram_size < 3 else None,
                    dominant_parent_side="left" if ngram_size < 3 else None,
                    score_version=score_version,
                    ranking_lane=f"{ngram_size}gram",
                    is_eligible=0 if key == "de" else 1,
                    frequency_score=0.44,
                    dispersion_score=0.33,
                    association_score=0.77 if ngram_size >= 2 else None,
                    boundary_score=0.22 if ngram_size >= 2 else None,
                    redundancy_penalty=0.11 if ngram_size == 2 else 0.0,
                    final_score=0.66,
                    lane_rank=None if key == "de" else 5,
                )
            )
        return tuple(rows)

    def close(self) -> None:
        return None


def test_cli_help() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "CLI for building the podcast-based French frequency deck." in result.stdout
    assert "generate-candidates" in result.stdout
    assert "refresh-candidate-metrics" in result.stdout
    assert "inspect-candidate-metrics" in result.stdout
    assert "inspect-candidate-scores" in result.stdout


def test_init_db_creates_database(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 0
    db_path = tmp_path / "test.db"
    assert db_path.exists()

    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            AND name IN ('app_meta', 'shows', 'episodes')
            ORDER BY name
            """
        ).fetchall()

    assert rows == [("app_meta",), ("episodes",), ("shows",)]

    load_settings.cache_clear()


def test_add_show_saves_manual_feed(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeDiscoveryService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_manual_discovery_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "add-show",
            "https://example.com/feed.xml",
            "--title",
            "Zack en roue libre",
            "--language",
            "fr",
        ],
    )

    assert result.exit_code == 0
    assert "saved_show_id=8" in result.stdout
    assert fake_service.manual_feed_url == "https://example.com/feed.xml"
    assert fake_service.manual_title == "Zack en roue libre"
    assert fake_service.manual_language == "fr"

    load_settings.cache_clear()


def test_sync_feed_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeSyncFeedService()
    monkeypatch.setattr("podcast_frequency_list.cli.build_sync_feed_service", lambda: fake_service)

    result = runner.invoke(app, ["sync-feed", "--show-id", "1", "--limit", "10"])

    assert result.exit_code == 0
    assert "show_id=1" in result.stdout
    assert "episodes_inserted=8" in result.stdout
    assert fake_service.show_id == 1
    assert fake_service.limit == 10

    load_settings.cache_clear()


def test_create_pilot_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakePilotSelectionService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_pilot_selection_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "create-pilot",
            "--show-id",
            "1",
            "--name",
            "zack-10h-pilot",
            "--hours",
            "10",
        ],
    )

    assert result.exit_code == 0
    assert "pilot_run_id=3" in result.stdout
    assert "selected_episodes=4" in result.stdout
    assert "estimated_asr_cost_usd=1.83" in result.stdout
    assert "status=needs_asr" in result.stdout
    assert fake_service.show_id == 1
    assert fake_service.name == "zack-10h-pilot"
    assert fake_service.target_seconds == 36_000
    assert fake_service.selection_order == "newest"

    load_settings.cache_clear()


def test_run_asr_prints_smoke_test_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeAsrRunService()
    monkeypatch.setattr("podcast_frequency_list.cli.build_asr_run_service", lambda: fake_service)

    result = runner.invoke(
        app,
        [
            "run-asr",
            "--pilot",
            "zack-10h-pilot",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert "pilot=zack-10h-pilot" in result.stdout
    assert "completed_episodes=1" in result.stdout
    assert "chunks_transcribed=2" in result.stdout
    assert "preview=bonjour tout le monde" in result.stdout
    assert fake_service.pilot_name == "zack-10h-pilot"
    assert fake_service.limit == 1
    assert fake_service.force is False

    load_settings.cache_clear()


def test_normalize_transcripts_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeTranscriptNormalizationService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_transcript_normalization_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "normalize-transcripts",
            "--pilot",
            "zack-10h-pilot",
        ],
    )

    assert result.exit_code == 0
    assert "scope=pilot" in result.stdout
    assert "scope_value=zack-10h-pilot" in result.stdout
    assert "normalized_segments=6" in result.stdout
    assert fake_service.pilot_name == "zack-10h-pilot"
    assert fake_service.episode_id is None
    assert fake_service.force is False

    load_settings.cache_clear()


def test_qc_segments_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeSegmentQcService()
    monkeypatch.setattr("podcast_frequency_list.cli.build_segment_qc_service", lambda: fake_service)

    result = runner.invoke(
        app,
        [
            "qc-segments",
            "--pilot",
            "zack-10h-pilot",
        ],
    )

    assert result.exit_code == 0
    assert "scope=pilot" in result.stdout
    assert "scope_value=zack-10h-pilot" in result.stdout
    assert "processed_segments=6" in result.stdout
    assert "review_segments=1" in result.stdout
    assert "remove_segments=1" in result.stdout
    assert fake_service.pilot_name == "zack-10h-pilot"
    assert fake_service.episode_id is None
    assert fake_service.force is False

    load_settings.cache_clear()


def test_split_sentences_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeSentenceSplitService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_sentence_split_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "split-sentences",
            "--pilot",
            "zack-10h-pilot",
        ],
    )

    assert result.exit_code == 0
    assert "scope=pilot" in result.stdout
    assert "scope_value=zack-10h-pilot" in result.stdout
    assert "split_version=1" in result.stdout
    assert "created_sentences=14" in result.stdout
    assert fake_service.pilot_name == "zack-10h-pilot"
    assert fake_service.episode_id is None
    assert fake_service.force is False

    load_settings.cache_clear()


def test_tokenize_sentences_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeSentenceTokenizationService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_sentence_tokenization_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "tokenize-sentences",
            "--pilot",
            "zack-10h-pilot",
        ],
    )

    assert result.exit_code == 0
    assert "scope=pilot" in result.stdout
    assert "scope_value=zack-10h-pilot" in result.stdout
    assert "tokenization_version=1" in result.stdout
    assert "created_tokens=92" in result.stdout
    assert fake_service.pilot_name == "zack-10h-pilot"
    assert fake_service.episode_id is None
    assert fake_service.force is False

    load_settings.cache_clear()


def test_generate_candidates_pilot_scope_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateInventoryService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_inventory_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "generate-candidates",
            "--pilot",
            "zack-10h-pilot",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout.splitlines() == [
        "scope=pilot",
        "scope_value=zack-10h-pilot",
        "inventory_version=1",
        "selected_sentences=12",
        "processed_sentences=12",
        "skipped_sentences=0",
        "created_candidates=23",
        "created_occurrences=92",
        "episodes_touched=2",
    ]
    assert fake_service.pilot_name == "zack-10h-pilot"
    assert fake_service.episode_id is None
    assert fake_service.force is False

    load_settings.cache_clear()


def test_generate_candidates_episode_scope_calls_service(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateInventoryService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_inventory_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "generate-candidates",
            "--episode-id",
            "7",
        ],
    )

    assert result.exit_code == 0
    assert "scope=episode" in result.stdout
    assert "scope_value=7" in result.stdout
    assert fake_service.pilot_name is None
    assert fake_service.episode_id == 7
    assert fake_service.force is False

    load_settings.cache_clear()


def test_generate_candidates_force_passthrough(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateInventoryService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_inventory_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "generate-candidates",
            "--pilot",
            "zack-10h-pilot",
            "--force",
        ],
    )

    assert result.exit_code == 0
    assert fake_service.force is True

    load_settings.cache_clear()


def test_generate_candidates_without_scope_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateInventoryService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_inventory_service",
        lambda: fake_service,
    )

    result = runner.invoke(app, ["generate-candidates"])

    assert result.exit_code == 1
    assert "error=provide exactly one of pilot_name or episode_id" in result.stdout

    load_settings.cache_clear()


def test_generate_candidates_with_both_scopes_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateInventoryService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_inventory_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "generate-candidates",
            "--pilot",
            "zack-10h-pilot",
            "--episode-id",
            "7",
        ],
    )

    assert result.exit_code == 1
    assert "error=provide exactly one of pilot_name or episode_id" in result.stdout

    load_settings.cache_clear()


def test_refresh_candidate_metrics_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateMetricsService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_metrics_service",
        lambda: fake_service,
    )

    result = runner.invoke(app, ["refresh-candidate-metrics"])

    assert result.exit_code == 0
    assert result.stdout.splitlines() == [
        "inventory_version=1",
        "selected_candidates=23",
        "refreshed_candidates=22",
        "deleted_orphan_candidates=1",
        "occurrence_count=92",
        "raw_frequency_total=92",
        "episode_dispersion_total=40",
        "show_dispersion_total=22",
        "display_text_updates=5",
    ]
    assert fake_service.refresh_calls == 1

    load_settings.cache_clear()


def test_refresh_candidate_scores_prints_stats(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateScoresService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_scores_service",
        lambda: fake_service,
    )

    result = runner.invoke(app, ["refresh-candidate-scores"])

    assert result.exit_code == 0
    assert result.stdout.splitlines() == [
        "inventory_version=1",
        "score_version=pilot-v1",
        "selected_candidates=49542",
        "stored_candidates=49542",
        "eligible_candidates=804",
        "eligible_1gram_candidates=205",
        "eligible_2gram_candidates=453",
        "eligible_3gram_candidates=146",
    ]
    assert fake_service.refresh_calls == 1

    load_settings.cache_clear()


def test_inspect_candidate_scores_prints_summary_and_rows(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateScoresService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_scores_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "inspect-candidate-scores",
            "--limit",
            "2",
            "--candidate-key",
            "en fait",
            "--candidate-key",
            "de",
            "--candidate-key",
            "missing",
        ],
    )

    assert result.exit_code == 0
    assert "score_version=pilot-v1" in result.stdout
    assert "stored_candidates=49542" in result.stdout
    assert "eligible_candidates=804" in result.stdout
    assert "top_candidate_count_1gram=1" in result.stdout
    assert "top_candidate_count_2gram=1" in result.stdout
    assert "top_candidate_count_3gram=1" in result.stdout
    assert "top_candidate_count_global=2" in result.stdout
    assert (
        "record=top_1gram\trank=1\tcandidate_key=score-candidate-1"
        "\tdisplay_text=score candidate 1\tngram_size=1\traw_frequency=10"
        "\tepisode_dispersion=2\tshow_dispersion=1\tcovered_by_any_count=1"
        "\tcovered_by_any_ratio=0.25\tindependent_occurrence_count=9"
        "\tdirect_parent_count=2\tdominant_parent_key=parent-1"
        "\tdominant_parent_shared_count=3\tdominant_parent_share=0.15"
        "\tdominant_parent_side=left\tscore_version=pilot-v1\tranking_lane=1gram"
        "\tis_eligible=1\tfrequency_score=0.3\tdispersion_score=0.2"
        "\tassociation_score=-\tboundary_score=-\tredundancy_penalty=0.0"
        "\tfinal_score=0.5\tlane_rank=1"
        in result.stdout
    )
    assert (
        "record=top_2gram\trank=1\tcandidate_key=score-candidate-2"
        "\tdisplay_text=score candidate 2\tngram_size=2\traw_frequency=20"
        "\tepisode_dispersion=3\tshow_dispersion=1\tt_score=5.0\tnpmi=0.7"
        "\tleft_context_type_count=2\tright_context_type_count=3"
        "\tleft_entropy=0.4\tright_entropy=0.6\tcovered_by_any_count=2"
        "\tcovered_by_any_ratio=0.5\tindependent_occurrence_count=18"
        "\tdirect_parent_count=3\tdominant_parent_key=parent-2"
        "\tdominant_parent_shared_count=4\tdominant_parent_share=0.3"
        "\tdominant_parent_side=left\tscore_version=pilot-v1\tranking_lane=2gram"
        "\tis_eligible=1\tfrequency_score=0.6\tdispersion_score=0.4"
        "\tassociation_score=0.8\tboundary_score=0.2\tredundancy_penalty=0.0"
        "\tfinal_score=1.0\tlane_rank=1"
        in result.stdout
    )
    assert (
        "record=top_global\trank=1\tcandidate_key=global-candidate-1"
        "\tdisplay_text=global candidate 1\tngram_size=1\traw_frequency=50"
        "\tepisode_dispersion=6\tshow_dispersion=1\tt_score=-\tnpmi=-"
        "\tleft_context_type_count=-\tright_context_type_count=-"
        "\tleft_entropy=-\tright_entropy=-\tcovered_by_any_count=50"
        "\tcovered_by_any_ratio=1.0\tindependent_occurrence_count=0"
        "\tdirect_parent_count=10\tdominant_parent_key=de la"
        "\tdominant_parent_shared_count=5\tdominant_parent_share=0.1"
        "\tdominant_parent_side=right\tscore_version=pilot-v1"
        "\tranking_lane=1gram\tis_eligible=1\tfrequency_score=0.9"
        "\tdispersion_score=1.0\tassociation_score=-\tboundary_score=-"
        "\tredundancy_penalty=0.0\tfinal_score=0.95\tlane_rank=2"
        in result.stdout
    )
    assert (
        "record=top_global\trank=2\tcandidate_key=global-candidate-2"
        "\tdisplay_text=global candidate 2\tngram_size=2\traw_frequency=40"
        "\tepisode_dispersion=5\tshow_dispersion=1\tt_score=6.0\tnpmi=0.8"
        "\tleft_context_type_count=3\tright_context_type_count=4"
        "\tleft_entropy=0.7\tright_entropy=0.8\tcovered_by_any_count=40"
        "\tcovered_by_any_ratio=1.0\tindependent_occurrence_count=0"
        "\tdirect_parent_count=6\tdominant_parent_key=c est un"
        "\tdominant_parent_shared_count=8\tdominant_parent_share=0.2"
        "\tdominant_parent_side=right\tscore_version=pilot-v1"
        "\tranking_lane=2gram\tis_eligible=1\tfrequency_score=0.8"
        "\tdispersion_score=0.9\tassociation_score=0.85\tboundary_score=0.75"
        "\tredundancy_penalty=0.0\tfinal_score=0.9\tlane_rank=1"
        in result.stdout
    )
    assert (
        "record=focus_candidate\trank=2\tcandidate_key=de\tdisplay_text=de"
        "\tngram_size=1\traw_frequency=7\tepisode_dispersion=3\tshow_dispersion=1"
        "\tt_score=-\tnpmi=-\tleft_context_type_count=-\tright_context_type_count=-"
        "\tleft_entropy=-\tright_entropy=-\tcovered_by_any_count=4"
        "\tcovered_by_any_ratio=0.57\tindependent_occurrence_count=3"
        "\tdirect_parent_count=2\tdominant_parent_key=je en fait"
        "\tdominant_parent_shared_count=3\tdominant_parent_share=0.43"
        "\tdominant_parent_side=left\tscore_version=pilot-v1\tranking_lane=1gram"
        "\tis_eligible=0\tfrequency_score=0.44\tdispersion_score=0.33"
        "\tassociation_score=-\tboundary_score=-\tredundancy_penalty=0.0"
        "\tfinal_score=0.66\tlane_rank=-"
        in result.stdout
    )
    assert "focus_missing_count=1" in result.stdout
    assert "focus_missing=missing" in result.stdout
    assert fake_service.summary_calls == 1
    assert fake_service.top_candidate_requests == [(1, 2), (2, 2), (3, 2)]
    assert fake_service.global_candidate_requests == [2]
    assert fake_service.focus_candidate_requests == [("en fait", "de", "missing")]

    load_settings.cache_clear()


def test_inspect_candidate_scores_fails_without_scores(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateScoresService()
    fake_service.summarize = lambda: CandidateScoresResult(
        inventory_version="1",
        score_version="pilot-v1",
        selected_candidates=23,
        stored_candidates=0,
        eligible_candidates=0,
        eligible_1gram_candidates=0,
        eligible_2gram_candidates=0,
        eligible_3gram_candidates=0,
    )
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_scores_service",
        lambda: fake_service,
    )

    result = runner.invoke(app, ["inspect-candidate-scores"])

    assert result.exit_code == 1
    assert "error=no candidate scores found for score inspection" in result.stdout

    load_settings.cache_clear()


def test_inspect_candidate_metrics_prints_validation_and_rows(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateMetricsService()
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_metrics_service",
        lambda: fake_service,
    )

    result = runner.invoke(
        app,
        [
            "inspect-candidate-metrics",
            "--limit",
            "2",
            "--candidate-key",
            "en fait",
            "--candidate-key",
            "missing",
        ],
    )

    assert result.exit_code == 0
    assert "candidate_count=22" in result.stdout
    assert "raw_frequency_mismatch_count=0" in result.stdout
    assert "top_candidate_count_1gram=1" in result.stdout
    assert "top_candidate_count_2gram=1" in result.stdout
    assert "top_candidate_count_3gram=1" in result.stdout
    assert (
        "record=top_1gram\trank=1\tcandidate_key=candidate-1\tdisplay_text=candidate 1"
        "\tngram_size=1\traw_frequency=10\tepisode_dispersion=2\tshow_dispersion=1"
        "\tcovered_by_any_count=1\tcovered_by_any_ratio=0.25"
        "\tindependent_occurrence_count=9\tdirect_parent_count=2"
        "\tdominant_parent_key=parent-1\tdominant_parent_shared_count=3"
        "\tdominant_parent_share=0.15\tdominant_parent_side=left"
        in result.stdout
    )
    assert (
        "record=top_2gram\trank=1\tcandidate_key=candidate-2\tdisplay_text=candidate 2"
        "\tngram_size=2\traw_frequency=20\tepisode_dispersion=3\tshow_dispersion=1"
        "\tt_score=5.0\tnpmi=0.7\tleft_context_type_count=2\tright_context_type_count=3"
        "\tleft_entropy=0.4\tright_entropy=0.6\tcovered_by_any_count=2"
        "\tcovered_by_any_ratio=0.5\tindependent_occurrence_count=18"
        "\tdirect_parent_count=3\tdominant_parent_key=parent-2"
        "\tdominant_parent_shared_count=4\tdominant_parent_share=0.3"
        "\tdominant_parent_side=left"
        in result.stdout
    )
    assert (
        "record=focus_candidate\trank=1\tcandidate_key=en fait\tdisplay_text=en fait"
        "\tngram_size=2\traw_frequency=7\tepisode_dispersion=3\tshow_dispersion=1"
        "\tt_score=4.2\tnpmi=0.77\tleft_context_type_count=2\tright_context_type_count=3"
        "\tleft_entropy=0.41\tright_entropy=0.92\tcovered_by_any_count=4"
        "\tcovered_by_any_ratio=0.57\tindependent_occurrence_count=3"
        "\tdirect_parent_count=2\tdominant_parent_key=je en fait"
        "\tdominant_parent_shared_count=3\tdominant_parent_share=0.43"
        "\tdominant_parent_side=left"
        in result.stdout
    )
    assert "focus_missing_count=1" in result.stdout
    assert "focus_missing=missing" in result.stdout
    assert fake_service.validation_calls == 1
    assert fake_service.top_candidate_requests == [(1, 2), (2, 2), (3, 2)]
    assert fake_service.focus_candidate_requests == [("en fait", "missing")]

    load_settings.cache_clear()


def test_inspect_candidate_metrics_fails_without_candidates(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeCandidateMetricsService()
    fake_service.validate = lambda: CandidateMetricsValidationResult(
        inventory_version="1",
        candidate_count=0,
        occurrence_count=0,
        raw_frequency_mismatch_count=0,
        episode_dispersion_mismatch_count=0,
        show_dispersion_mismatch_count=0,
        display_text_mismatch_count=0,
        foreign_key_issue_count=0,
    )
    monkeypatch.setattr(
        "podcast_frequency_list.cli.build_candidate_metrics_service",
        lambda: fake_service,
    )

    result = runner.invoke(app, ["inspect-candidate-metrics"])

    assert result.exit_code == 1
    assert "error=no token candidates found for inventory inspection" in result.stdout

    load_settings.cache_clear()
