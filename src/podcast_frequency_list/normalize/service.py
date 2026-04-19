from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.normalize.models import NormalizationRunResult
from podcast_frequency_list.normalize.text import normalize_transcript_text
from podcast_frequency_list.transcript_scope import resolve_transcript_scope

NORMALIZATION_VERSION = "1"


class TranscriptNormalizationError(RuntimeError):
    pass


@dataclass(frozen=True)
class _NormalizationTarget:
    segment_id: int
    episode_id: int
    raw_text: str
    normalization_version: str | None


class TranscriptNormalizationService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def normalize(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> NormalizationRunResult:
        scope = resolve_transcript_scope(
            pilot_name=pilot_name,
            episode_id=episode_id,
            error_type=TranscriptNormalizationError,
        )

        targets = self._load_targets(
            pilot_name=scope.pilot_name,
            episode_id=scope.episode_id,
        )
        if not targets:
            raise TranscriptNormalizationError("no transcript segments found for normalization")

        normalized_segments = 0
        skipped_segments = 0
        episode_ids: set[int] = set()

        with connect(self.db_path) as connection:
            for target in targets:
                episode_ids.add(target.episode_id)
                if target.normalization_version == NORMALIZATION_VERSION and not force:
                    skipped_segments += 1
                    continue

                connection.execute(
                    """
                    INSERT INTO normalized_segments (
                        segment_id,
                        episode_id,
                        normalization_version,
                        normalized_text
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(segment_id) DO UPDATE SET
                        episode_id = excluded.episode_id,
                        normalization_version = excluded.normalization_version,
                        normalized_text = excluded.normalized_text,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        target.segment_id,
                        target.episode_id,
                        NORMALIZATION_VERSION,
                        normalize_transcript_text(target.raw_text),
                    ),
                )
                normalized_segments += 1

            connection.commit()

        return NormalizationRunResult(
            scope=scope.kind,
            scope_value=scope.scope_value,
            normalization_version=NORMALIZATION_VERSION,
            selected_segments=len(targets),
            normalized_segments=normalized_segments,
            skipped_segments=skipped_segments,
            episode_count=len(episode_ids),
        )

    def _load_targets(
        self,
        *,
        pilot_name: str | None,
        episode_id: int | None,
    ) -> list[_NormalizationTarget]:
        with connect(self.db_path) as connection:
            if pilot_name is not None:
                rows = connection.execute(
                    """
                    SELECT
                        ts.segment_id,
                        ts.episode_id,
                        ts.raw_text,
                        ns.normalization_version
                    FROM transcript_segments ts
                    JOIN transcript_sources src
                        ON src.source_id = ts.source_id
                    JOIN pilot_run_episodes pre
                        ON pre.episode_id = ts.episode_id
                    JOIN pilot_runs pr
                        ON pr.pilot_run_id = pre.pilot_run_id
                    LEFT JOIN normalized_segments ns
                        ON ns.segment_id = ts.segment_id
                    WHERE pr.name = ?
                    AND src.status = 'ready'
                    ORDER BY pre.position, ts.chunk_index
                    """,
                    (pilot_name,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT
                        ts.segment_id,
                        ts.episode_id,
                        ts.raw_text,
                        ns.normalization_version
                    FROM transcript_segments ts
                    JOIN transcript_sources src
                        ON src.source_id = ts.source_id
                    LEFT JOIN normalized_segments ns
                        ON ns.segment_id = ts.segment_id
                    WHERE ts.episode_id = ?
                    AND src.status = 'ready'
                    ORDER BY ts.chunk_index
                    """,
                    (episode_id,),
                ).fetchall()

        return [
            _NormalizationTarget(
                segment_id=int(row["segment_id"]),
                episode_id=int(row["episode_id"]),
                raw_text=str(row["raw_text"]),
                normalization_version=row["normalization_version"],
            )
            for row in rows
        ]
