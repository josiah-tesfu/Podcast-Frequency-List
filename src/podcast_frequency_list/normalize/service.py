from __future__ import annotations

import sqlite3
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.normalize.models import NormalizationRunResult
from podcast_frequency_list.normalize.text import normalize_transcript_text

NORMALIZATION_VERSION = "1"


class TranscriptNormalizationError(RuntimeError):
    pass


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
        if (pilot_name is None and episode_id is None) or (
            pilot_name is not None and episode_id is not None
        ):
            raise TranscriptNormalizationError(
                "provide exactly one of pilot_name or episode_id"
            )

        rows = self._load_segments(pilot_name=pilot_name, episode_id=episode_id)
        if not rows:
            raise TranscriptNormalizationError("no transcript segments found for normalization")

        normalized_segments = 0
        skipped_segments = 0
        episode_ids: set[int] = set()

        with connect(self.db_path) as connection:
            for row in rows:
                episode_ids.add(int(row["episode_id"]))
                if row["normalization_version"] == NORMALIZATION_VERSION and not force:
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
                        row["segment_id"],
                        row["episode_id"],
                        NORMALIZATION_VERSION,
                        normalize_transcript_text(row["raw_text"]),
                    ),
                )
                normalized_segments += 1

            connection.commit()

        if pilot_name is not None:
            scope = "pilot"
            scope_value = pilot_name
        else:
            scope = "episode"
            scope_value = str(episode_id)

        return NormalizationRunResult(
            scope=scope,
            scope_value=scope_value,
            normalization_version=NORMALIZATION_VERSION,
            selected_segments=len(rows),
            normalized_segments=normalized_segments,
            skipped_segments=skipped_segments,
            episode_count=len(episode_ids),
        )

    def _load_segments(
        self,
        *,
        pilot_name: str | None,
        episode_id: int | None,
    ) -> list[sqlite3.Row]:
        with connect(self.db_path) as connection:
            if pilot_name is not None:
                rows = connection.execute(
                    """
                    SELECT
                        ts.segment_id,
                        ts.episode_id,
                        ts.chunk_index,
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
                        ts.chunk_index,
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

        return list(rows)
