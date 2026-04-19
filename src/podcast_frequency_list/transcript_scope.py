from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class TranscriptScope:
    kind: Literal["pilot", "episode"]
    pilot_name: str | None = None
    episode_id: int | None = None

    @property
    def scope_value(self) -> str:
        if self.kind == "pilot":
            return str(self.pilot_name)
        return str(self.episode_id)


def resolve_transcript_scope(
    *,
    pilot_name: str | None,
    episode_id: int | None,
    error_type: type[Exception],
) -> TranscriptScope:
    if (pilot_name is None and episode_id is None) or (
        pilot_name is not None and episode_id is not None
    ):
        raise error_type("provide exactly one of pilot_name or episode_id")

    if pilot_name is not None:
        return TranscriptScope(kind="pilot", pilot_name=pilot_name)

    return TranscriptScope(kind="episode", episode_id=episode_id)
