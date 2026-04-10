from __future__ import annotations

from pathlib import Path

import httpx


class OpenAITranscriptionError(RuntimeError):
    pass


class OpenAITranscriber:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        language: str = "fr",
        timeout_seconds: float = 600.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise OpenAITranscriptionError("OPENAI_API_KEY is required")

        self.model = model
        self.language = language
        self.client = httpx.Client(
            timeout=timeout_seconds,
            headers={"Authorization": f"Bearer {api_key}"},
            transport=transport,
        )

    def close(self) -> None:
        self.client.close()

    def transcribe(self, audio_path: Path) -> str:
        try:
            with audio_path.open("rb") as audio_file:
                response = self.client.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    data={
                        "model": self.model,
                        "language": self.language,
                        "response_format": "json",
                    },
                    files={"file": (audio_path.name, audio_file, "audio/mpeg")},
                )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, OSError) as exc:
            message = f"failed to transcribe {audio_path}"
            if isinstance(exc, httpx.HTTPStatusError):
                message = f"{message}: {exc.response.status_code} {exc.response.text}"
            raise OpenAITranscriptionError(message) from exc

        text = str(payload.get("text", "")).strip()
        if not text:
            raise OpenAITranscriptionError(f"empty transcription response for {audio_path}")
        return text
