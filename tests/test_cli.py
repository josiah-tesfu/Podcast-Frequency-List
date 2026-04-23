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
from podcast_frequency_list.tokens.models import CandidateInventoryResult, TokenizationResult

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


def test_cli_help() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "CLI for building the podcast-based French frequency deck." in result.stdout
    assert "generate-candidates" in result.stdout


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
