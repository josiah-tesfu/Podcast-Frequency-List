from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from podcast_frequency_list.asr.audio import AudioChunker, AudioDownloader
from podcast_frequency_list.asr.client import OpenAITranscriber
from podcast_frequency_list.asr.models import AsrEpisodeResult, AsrRunResult, AudioChunk
from podcast_frequency_list.db import connect, upsert_transcript_source


class Transcriber(Protocol):
    model: str

    def transcribe(self, audio_path: Path) -> str: ...


class AsrRunError(RuntimeError):
    pass


@dataclass(frozen=True)
class _AsrEpisodeJob:
    episode_id: int
    show_id: int
    title: str
    audio_url: str
    duration_seconds: int
    status: str | None


class AsrRunService:
    def __init__(
        self,
        *,
        db_path: Path,
        raw_data_dir: Path,
        audio_downloader: AudioDownloader,
        audio_chunker: AudioChunker,
        transcriber: OpenAITranscriber | Transcriber,
    ) -> None:
        self.db_path = db_path
        self.raw_data_dir = raw_data_dir
        self.audio_downloader = audio_downloader
        self.audio_chunker = audio_chunker
        self.transcriber = transcriber

    def close(self) -> None:
        self.audio_downloader.close()
        close = getattr(self.transcriber, "close", None)
        if close is not None:
            close()

    def run_pilot(
        self,
        *,
        pilot_name: str,
        limit: int | None = None,
        force: bool = False,
    ) -> AsrRunResult:
        if limit is not None and limit <= 0:
            raise AsrRunError("limit must be positive")

        episodes = self._load_episodes(pilot_name=pilot_name, limit=limit, force=force)
        if not episodes:
            raise AsrRunError("no ASR-ready pilot episodes found")

        results: list[AsrEpisodeResult] = []
        for episode in episodes:
            results.append(self._run_episode(episode=episode, force=force))

        return AsrRunResult(
            pilot_name=pilot_name,
            model=self.transcriber.model,
            requested_limit=limit,
            selected_count=len(results),
            completed_count=sum(result.status == "ready" for result in results),
            skipped_count=sum(result.status == "skipped" for result in results),
            failed_count=sum(result.status == "failed" for result in results),
            chunk_count=sum(result.chunk_count for result in results),
            episode_results=tuple(results),
        )

    def _load_episodes(
        self,
        *,
        pilot_name: str,
        limit: int | None,
        force: bool,
    ) -> list[_AsrEpisodeJob]:
        limit_sql = "" if limit is None else "LIMIT ?"
        parameters: list[object] = [self.transcriber.model, pilot_name]
        if limit is not None:
            parameters.append(limit)

        status_filter = "" if force else "AND COALESCE(ts.status, 'needs_asr') != 'ready'"
        with connect(self.db_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    e.episode_id,
                    e.show_id,
                    e.title,
                    e.audio_url,
                    e.duration_seconds,
                    ts.status
                FROM pilot_runs pr
                JOIN pilot_run_episodes pre
                    ON pre.pilot_run_id = pr.pilot_run_id
                JOIN episodes e
                    ON e.episode_id = pre.episode_id
                LEFT JOIN transcript_sources ts
                    ON ts.episode_id = e.episode_id
                    AND ts.source_type = 'asr'
                    AND ts.model = ?
                WHERE pr.name = ?
                {status_filter}
                ORDER BY pre.position
                {limit_sql}
                """,
                parameters,
            ).fetchall()

        return [
            _AsrEpisodeJob(
                episode_id=int(row["episode_id"]),
                show_id=int(row["show_id"]),
                title=str(row["title"]),
                audio_url=str(row["audio_url"]),
                duration_seconds=int(row["duration_seconds"]),
                status=row["status"],
            )
            for row in rows
        ]

    def _run_episode(self, *, episode: _AsrEpisodeJob, force: bool) -> AsrEpisodeResult:
        episode_id = episode.episode_id
        title = episode.title
        audio_path: Path | None = None
        transcript_path: Path | None = None

        if episode.status == "ready" and not force:
            return AsrEpisodeResult(
                episode_id=episode_id,
                title=title,
                status="skipped",
                audio_path=None,
                transcript_path=None,
                chunk_count=0,
                text_chars=0,
                preview="",
            )

        try:
            audio_path = self.audio_downloader.download(
                show_id=episode.show_id,
                episode_id=episode_id,
                audio_url=episode.audio_url,
            )
            chunks = self.audio_chunker.chunk(
                audio_path=audio_path,
                duration_seconds=episode.duration_seconds,
                episode_id=episode_id,
            )
            source_id = self._set_source_status(episode_id=episode_id, status="in_progress")

            chunk_texts = []
            for chunk in chunks:
                chunk_texts.append((chunk, self.transcriber.transcribe(chunk.path)))

            transcript_path = self._write_transcript(
                episode_id=episode_id,
                chunk_texts=tuple(chunk_texts),
            )
            self._save_segments(source_id=source_id, episode_id=episode_id, chunk_texts=chunk_texts)
            text = "\n\n".join(text for _, text in chunk_texts)
            self._set_source_status(
                episode_id=episode_id,
                status="ready",
                raw_path=str(transcript_path),
            )

            return AsrEpisodeResult(
                episode_id=episode_id,
                title=title,
                status="ready",
                audio_path=audio_path,
                transcript_path=transcript_path,
                chunk_count=len(chunks),
                text_chars=len(text),
                preview=_preview(text),
            )
        except Exception as exc:
            self._set_source_status(episode_id=episode_id, status="failed")
            return AsrEpisodeResult(
                episode_id=episode_id,
                title=title,
                status="failed",
                audio_path=audio_path,
                transcript_path=transcript_path,
                chunk_count=0,
                text_chars=0,
                preview="",
                error=str(exc),
            )

    def _set_source_status(
        self,
        *,
        episode_id: int,
        status: str,
        raw_path: str | None = None,
    ) -> int:
        with connect(self.db_path) as connection:
            source_id = upsert_transcript_source(
                connection,
                episode_id=episode_id,
                source_type="asr",
                status=status,
                model=self.transcriber.model,
                raw_path=raw_path,
            )
            connection.commit()
            return source_id

    def _write_transcript(
        self,
        *,
        episode_id: int,
        chunk_texts: tuple[tuple[AudioChunk, str], ...],
    ) -> Path:
        transcript_dir = self.raw_data_dir / "transcripts" / "asr"
        transcript_dir.mkdir(parents=True, exist_ok=True)
        transcript_path = transcript_dir / f"episode-{episode_id}.txt"
        transcript_text = "\n\n".join(text for _, text in chunk_texts)
        transcript_path.write_text(transcript_text, encoding="utf-8")
        return transcript_path

    def _save_segments(
        self,
        *,
        source_id: int,
        episode_id: int,
        chunk_texts: list[tuple[AudioChunk, str]],
    ) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                "DELETE FROM transcript_segments WHERE source_id = ?",
                (source_id,),
            )
            for chunk, text in chunk_texts:
                connection.execute(
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
                        chunk.chunk_index,
                        chunk.start_seconds * 1000,
                        chunk.end_seconds * 1000,
                        text,
                    ),
                )
            connection.commit()


def _preview(text: str, max_chars: int = 240) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3] + "..."
