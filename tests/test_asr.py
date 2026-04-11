from pathlib import Path

import httpx
import pytest

from podcast_frequency_list.asr.audio import AudioChunker, AudioDownloader
from podcast_frequency_list.asr.client import OpenAITranscriber, OpenAITranscriptionError
from podcast_frequency_list.asr.models import AudioChunk
from podcast_frequency_list.asr.service import AsrRunService
from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.pilot import PilotSelectionService


class FakeDownloader:
    def __init__(self, audio_path: Path) -> None:
        self.audio_path = audio_path
        self.downloaded_episode_id: int | None = None

    def download(self, *, show_id: int, episode_id: int, audio_url: str) -> Path:
        self.downloaded_episode_id = episode_id
        return self.audio_path

    def close(self) -> None:
        return None


class FakeChunker:
    def chunk(
        self,
        *,
        audio_path: Path,
        duration_seconds: int,
        episode_id: int,
    ) -> tuple[AudioChunk, ...]:
        return (
            AudioChunk(audio_path, chunk_index=0, start_seconds=0, end_seconds=60),
            AudioChunk(audio_path, chunk_index=1, start_seconds=60, end_seconds=120),
        )


class FakeTranscriber:
    model = "gpt-4o-mini-transcribe"

    def __init__(self) -> None:
        self.calls: list[Path] = []

    def transcribe(self, audio_path: Path) -> str:
        self.calls.append(audio_path)
        return f"bonjour chunk {len(self.calls)}"

    def close(self) -> None:
        return None


def _seed_pilot(db_path: Path) -> int:
    bootstrap_database(db_path)
    with connect(db_path) as connection:
        show_id = upsert_show(
            connection,
            title="Zack en Roue Libre by Zack Nani",
            feed_url="https://example.com/zack.xml",
        )
        upsert_episode(
            connection,
            show_id=show_id,
            guid="ep-1",
            title="Episode 1",
            audio_url="https://cdn.example.com/ep1.mp3",
            duration_seconds=120,
        )
        connection.commit()

    PilotSelectionService(db_path=db_path).create_pilot(
        show_id=show_id,
        name="zack-test-pilot",
        target_seconds=120,
    )
    return show_id


def test_audio_downloader_caches_file(tmp_path) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"audio bytes",
            headers={"content-type": "audio/mpeg"},
        )

    downloader = AudioDownloader(
        audio_dir=tmp_path / "audio",
        transport=httpx.MockTransport(handler),
    )

    try:
        first_path = downloader.download(
            show_id=1,
            episode_id=2,
            audio_url="https://cdn.example.com/audio.mp3",
        )
        second_path = downloader.download(
            show_id=1,
            episode_id=2,
            audio_url="https://cdn.example.com/audio.mp3",
        )
    finally:
        downloader.close()

    assert first_path == second_path
    assert first_path.read_bytes() == b"audio bytes"


def test_audio_chunker_returns_original_when_under_upload_limit(tmp_path) -> None:
    audio_path = tmp_path / "episode.mp3"
    audio_path.write_bytes(b"small audio")
    chunker = AudioChunker(chunk_dir=tmp_path / "chunks", max_upload_bytes=100)

    chunks = chunker.chunk(audio_path=audio_path, duration_seconds=120, episode_id=1)

    assert chunks == (
        AudioChunk(audio_path, chunk_index=0, start_seconds=0, end_seconds=120),
    )


def test_openai_transcriber_requires_api_key() -> None:
    with pytest.raises(OpenAITranscriptionError, match="OPENAI_API_KEY"):
        OpenAITranscriber(api_key="", model="gpt-4o-mini-transcribe")


def test_openai_transcriber_posts_json_response_format(tmp_path) -> None:
    audio_path = tmp_path / "chunk.mp3"
    audio_path.write_bytes(b"audio")
    seen_request_body = b""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_request_body
        seen_request_body = request.read()
        return httpx.Response(200, json={"text": "bonjour"})

    transcriber = OpenAITranscriber(
        api_key="test-key",
        model="gpt-4o-mini-transcribe",
        transport=httpx.MockTransport(handler),
    )

    try:
        text = transcriber.transcribe(audio_path)
    finally:
        transcriber.close()

    assert text == "bonjour"
    assert b'name="response_format"' in seen_request_body
    assert b"json" in seen_request_body


def test_asr_run_service_writes_transcript_and_segments(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    audio_path = tmp_path / "episode.mp3"
    audio_path.write_bytes(b"audio")
    _seed_pilot(db_path)

    transcriber = FakeTranscriber()
    service = AsrRunService(
        db_path=db_path,
        raw_data_dir=tmp_path / "raw",
        audio_downloader=FakeDownloader(audio_path),  # type: ignore[arg-type]
        audio_chunker=FakeChunker(),  # type: ignore[arg-type]
        transcriber=transcriber,
    )

    result = service.run_pilot(pilot_name="zack-test-pilot", limit=1)

    with connect(db_path) as connection:
        source = connection.execute(
            "SELECT source_id, status, raw_path FROM transcript_sources"
        ).fetchone()
        segments = connection.execute(
            """
            SELECT chunk_index, start_ms, end_ms, raw_text
            FROM transcript_segments
            ORDER BY chunk_index
            """
        ).fetchall()

    assert result.completed_count == 1
    assert result.chunk_count == 2
    assert result.episode_results[0].preview == "bonjour chunk 1 bonjour chunk 2"
    assert len(transcriber.calls) == 2
    assert source["status"] == "ready"
    assert (
        Path(source["raw_path"]).read_text(encoding="utf-8")
        == "bonjour chunk 1\n\nbonjour chunk 2"
    )
    assert [row["chunk_index"] for row in segments] == [0, 1]
    assert [row["start_ms"] for row in segments] == [0, 60_000]
    assert [row["end_ms"] for row in segments] == [60_000, 120_000]
