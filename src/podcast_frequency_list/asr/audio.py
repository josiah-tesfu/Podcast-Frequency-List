from __future__ import annotations

import mimetypes
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import httpx
import imageio_ffmpeg

from podcast_frequency_list.asr.models import AudioChunk
from podcast_frequency_list.discovery import DEFAULT_USER_AGENT

OPENAI_MAX_UPLOAD_BYTES = 25 * 1024 * 1024
DEFAULT_SAFE_UPLOAD_BYTES = 24 * 1024 * 1024
DEFAULT_CHUNK_SECONDS = 20 * 60


class AudioDownloadError(RuntimeError):
    pass


class AudioChunkError(RuntimeError):
    pass


class AudioDownloader:
    def __init__(
        self,
        *,
        audio_dir: Path,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout_seconds: float = 120.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.audio_dir = audio_dir
        self.client = httpx.Client(
            follow_redirects=True,
            timeout=timeout_seconds,
            headers={"User-Agent": user_agent},
            transport=transport,
        )

    def close(self) -> None:
        self.client.close()

    def download(
        self,
        *,
        show_id: int,
        episode_id: int,
        audio_url: str,
    ) -> Path:
        target_dir = self.audio_dir / str(show_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{episode_id}{self._extension_from_url(audio_url)}"

        if target_path.exists() and target_path.stat().st_size > 0:
            return target_path

        temporary_path = target_path.with_suffix(f"{target_path.suffix}.part")
        try:
            with self.client.stream("GET", audio_url) as response:
                response.raise_for_status()
                suffix = self._extension_from_content_type(
                    response.headers.get("content-type")
                )
                if suffix and target_path.suffix == ".audio":
                    target_path = target_path.with_suffix(suffix)
                    temporary_path = target_path.with_suffix(f"{target_path.suffix}.part")

                with temporary_path.open("wb") as output_file:
                    for chunk in response.iter_bytes():
                        output_file.write(chunk)

            if temporary_path.stat().st_size <= 0:
                raise AudioDownloadError("downloaded audio file is empty")

            temporary_path.replace(target_path)
            return target_path
        except (httpx.HTTPError, OSError) as exc:
            if temporary_path.exists():
                temporary_path.unlink()
            raise AudioDownloadError(f"failed to download episode {episode_id}") from exc

    def _extension_from_url(self, audio_url: str) -> str:
        path = urlparse(audio_url).path
        suffix = Path(path).suffix.lower()
        if suffix in {".mp3", ".m4a", ".mp4", ".mpeg", ".mpga", ".wav", ".webm"}:
            return suffix
        return ".audio"

    def _extension_from_content_type(self, content_type: str | None) -> str | None:
        if not content_type:
            return None
        suffix = mimetypes.guess_extension(content_type.split(";", maxsplit=1)[0])
        if suffix == ".mpga":
            return ".mp3"
        return suffix


class AudioChunker:
    def __init__(
        self,
        *,
        chunk_dir: Path,
        max_upload_bytes: int = DEFAULT_SAFE_UPLOAD_BYTES,
        chunk_seconds: int = DEFAULT_CHUNK_SECONDS,
        ffmpeg_path: str | None = None,
    ) -> None:
        if max_upload_bytes > OPENAI_MAX_UPLOAD_BYTES:
            raise AudioChunkError("max_upload_bytes must stay under OpenAI's 25 MB limit")
        self.chunk_dir = chunk_dir
        self.max_upload_bytes = max_upload_bytes
        self.chunk_seconds = chunk_seconds
        self.ffmpeg_path = ffmpeg_path

    def chunk(
        self,
        *,
        audio_path: Path,
        duration_seconds: int,
        episode_id: int,
    ) -> tuple[AudioChunk, ...]:
        if duration_seconds <= 0:
            raise AudioChunkError("duration_seconds must be positive")

        if audio_path.stat().st_size <= self.max_upload_bytes:
            return (
                AudioChunk(
                    path=audio_path,
                    chunk_index=0,
                    start_seconds=0,
                    end_seconds=duration_seconds,
                ),
            )

        target_dir = self.chunk_dir / str(episode_id)
        if target_dir.exists():
            shutil.rmtree(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        ffmpeg_path = self._resolve_ffmpeg_path()
        chunks: list[AudioChunk] = []
        start_seconds = 0

        while start_seconds < duration_seconds:
            end_seconds = min(start_seconds + self.chunk_seconds, duration_seconds)
            chunk_index = len(chunks)
            chunk_path = target_dir / f"chunk-{chunk_index:04}.mp3"
            command = [
                ffmpeg_path,
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-ss",
                str(start_seconds),
                "-t",
                str(end_seconds - start_seconds),
                "-i",
                str(audio_path),
                "-vn",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-b:a",
                "64k",
                str(chunk_path),
            ]

            completed_process = subprocess.run(command, capture_output=True, text=True, check=False)
            if completed_process.returncode != 0:
                raise AudioChunkError(completed_process.stderr.strip() or "ffmpeg chunking failed")
            if chunk_path.stat().st_size > self.max_upload_bytes:
                raise AudioChunkError(f"chunk exceeded upload limit: {chunk_path}")

            chunks.append(
                AudioChunk(
                    path=chunk_path,
                    chunk_index=chunk_index,
                    start_seconds=start_seconds,
                    end_seconds=end_seconds,
                )
            )
            start_seconds = end_seconds

        return tuple(chunks)

    def _resolve_ffmpeg_path(self) -> str:
        if self.ffmpeg_path:
            return self.ffmpeg_path
        return imageio_ffmpeg.get_ffmpeg_exe()
