import sqlite3

from typer.testing import CliRunner

from podcast_frequency_list.cli import app
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.discovery.models import PodcastCandidate, SavedShow
from podcast_frequency_list.ingest.models import SyncFeedResult

runner = CliRunner()


class FakeDiscoveryService:
    def __init__(self) -> None:
        self.saved_candidate: PodcastCandidate | None = None
        self.manual_feed_url: str | None = None
        self.manual_title: str | None = None
        self.manual_language: str | None = None
        self.manual_bucket: str | None = None

    def search(self, query: str, *, limit: int = 5) -> list[PodcastCandidate]:
        assert query == "InnerFrench"
        assert limit == 2
        return [
            PodcastCandidate(
                podcast_index_id=1,
                title="InnerFrench",
                feed_url="https://example.com/feed.xml",
                site_url="https://example.com",
                author="Hugo",
                language="fr",
                score=175.0,
            ),
            PodcastCandidate(
                podcast_index_id=2,
                title="Another Show",
                feed_url="https://example.com/other.xml",
                language="en",
                score=90.0,
            ),
        ]

    def save_selected_candidate(self, candidate: PodcastCandidate) -> SavedShow:
        self.saved_candidate = candidate
        return SavedShow(show_id=7, title=candidate.title, feed_url=candidate.feed_url)

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


def test_cli_help() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "CLI for building the podcast-based French frequency deck." in result.stdout


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


def test_discover_show_selects_and_saves_candidate(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PROCESSED_DATA_DIR", str(tmp_path / "processed"))
    load_settings.cache_clear()

    fake_service = FakeDiscoveryService()
    monkeypatch.setattr("podcast_frequency_list.cli.build_discovery_service", lambda: fake_service)

    result = runner.invoke(app, ["discover-show", "InnerFrench", "--limit", "2", "--select", "1"])

    assert result.exit_code == 0
    assert "saved_show_id=7" in result.stdout
    assert fake_service.saved_candidate is not None
    assert fake_service.saved_candidate.title == "InnerFrench"

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
