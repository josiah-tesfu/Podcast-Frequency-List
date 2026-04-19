from __future__ import annotations

import sqlite3
from pathlib import Path

from podcast_frequency_list.db import connect, get_show_by_id, upsert_transcript_source
from podcast_frequency_list.pilot.models import PilotEpisode, PilotSelectionResult

DEFAULT_ASR_MODEL = "gpt-4o-mini-transcribe"
DEFAULT_ASR_COST_PER_MINUTE_USD = 0.003


class PilotSelectionError(RuntimeError):
    pass


class PilotSelectionService:
    def __init__(
        self,
        *,
        db_path: Path,
        asr_model: str = DEFAULT_ASR_MODEL,
        asr_cost_per_minute_usd: float = DEFAULT_ASR_COST_PER_MINUTE_USD,
    ) -> None:
        self.db_path = db_path
        self.asr_model = asr_model
        self.asr_cost_per_minute_usd = asr_cost_per_minute_usd

    def create_pilot(
        self,
        *,
        show_id: int,
        name: str,
        target_seconds: int,
        selection_order: str = "newest",
        notes: str | None = None,
    ) -> PilotSelectionResult:
        if target_seconds <= 0:
            raise PilotSelectionError("target_seconds must be positive")
        if selection_order not in {"newest", "oldest"}:
            raise PilotSelectionError("selection_order must be newest or oldest")

        order_sql = "DESC" if selection_order == "newest" else "ASC"

        with connect(self.db_path) as connection:
            show = get_show_by_id(connection, show_id)
            if show is None:
                raise PilotSelectionError(f"show_id {show_id} not found")

            skipped_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM episodes
                WHERE show_id = ?
                AND (
                    audio_url IS NULL
                    OR audio_url = ''
                    OR duration_seconds IS NULL
                    OR duration_seconds <= 0
                )
                """,
                (show_id,),
            ).fetchone()[0]

            rows = connection.execute(
                f"""
                SELECT episode_id, title, published_at, duration_seconds
                FROM episodes
                WHERE show_id = ?
                AND audio_url IS NOT NULL
                AND audio_url != ''
                AND duration_seconds IS NOT NULL
                AND duration_seconds > 0
                ORDER BY published_at {order_sql}, episode_id {order_sql}
                """,
                (show_id,),
            ).fetchall()

            selected: list[PilotEpisode] = []
            cumulative_seconds = 0

            for row in rows:
                cumulative_seconds += int(row["duration_seconds"])
                selected.append(
                    PilotEpisode(
                        episode_id=int(row["episode_id"]),
                        title=row["title"],
                        published_at=row["published_at"],
                        duration_seconds=int(row["duration_seconds"]),
                        cumulative_seconds=cumulative_seconds,
                    )
                )
                if cumulative_seconds >= target_seconds:
                    break

            if not selected:
                raise PilotSelectionError("no eligible episodes found")

            pilot_run_id = self._upsert_pilot_run(
                connection,
                show_id=show_id,
                name=name,
                target_seconds=target_seconds,
                selection_order=selection_order,
                notes=notes,
            )

            connection.execute(
                "DELETE FROM pilot_run_episodes WHERE pilot_run_id = ?",
                (pilot_run_id,),
            )

            for position, episode in enumerate(selected, start=1):
                connection.execute(
                    """
                    INSERT INTO pilot_run_episodes (
                        pilot_run_id,
                        episode_id,
                        position,
                        cumulative_seconds
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        pilot_run_id,
                        episode.episode_id,
                        position,
                        episode.cumulative_seconds,
                    ),
                )
                self._mark_episode_needs_asr(
                    connection,
                    episode_id=episode.episode_id,
                    duration_seconds=episode.duration_seconds,
                )

            connection.commit()

        selected_tuple = tuple(selected)
        total_seconds = selected_tuple[-1].cumulative_seconds
        return PilotSelectionResult(
            pilot_run_id=pilot_run_id,
            name=name,
            show_id=show_id,
            show_title=show["title"],
            target_seconds=target_seconds,
            total_seconds=total_seconds,
            selected_count=len(selected_tuple),
            skipped_count=int(skipped_count),
            estimated_cost_usd=self._estimate_cost(total_seconds),
            model=self.asr_model,
            selection_order=selection_order,
            first_published_at=selected_tuple[0].published_at,
            last_published_at=selected_tuple[-1].published_at,
            episodes=selected_tuple,
        )

    def _upsert_pilot_run(
        self,
        connection: sqlite3.Connection,
        *,
        show_id: int,
        name: str,
        target_seconds: int,
        selection_order: str,
        notes: str | None,
    ) -> int:
        row = connection.execute(
            "SELECT pilot_run_id FROM pilot_runs WHERE name = ?",
            (name,),
        ).fetchone()

        if row is not None:
            pilot_run_id = int(row["pilot_run_id"])
            connection.execute(
                """
                UPDATE pilot_runs
                SET show_id = ?,
                    target_seconds = ?,
                    selection_order = ?,
                    notes = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pilot_run_id = ?
                """,
                (
                    show_id,
                    target_seconds,
                    selection_order,
                    notes,
                    pilot_run_id,
                ),
            )
            return pilot_run_id

        cursor = connection.execute(
            """
            INSERT INTO pilot_runs (show_id, name, target_seconds, selection_order, notes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (show_id, name, target_seconds, selection_order, notes),
        )
        return int(cursor.lastrowid)

    def _mark_episode_needs_asr(
        self,
        connection: sqlite3.Connection,
        *,
        episode_id: int,
        duration_seconds: int,
    ) -> None:
        upsert_transcript_source(
            connection,
            episode_id=episode_id,
            source_type="asr",
            status="needs_asr",
            model=self.asr_model,
            estimated_cost_usd=self._estimate_cost(duration_seconds),
            preserve_ready=True,
        )

    def _estimate_cost(self, duration_seconds: int) -> float:
        return (duration_seconds / 60) * self.asr_cost_per_minute_usd
