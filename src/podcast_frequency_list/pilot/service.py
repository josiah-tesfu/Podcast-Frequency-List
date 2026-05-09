from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path

from podcast_frequency_list.db import connect, get_show_by_id, upsert_transcript_source
from podcast_frequency_list.discovery.common import normalize_text
from podcast_frequency_list.pilot.models import (
    CorpusStatusResult,
    CorpusStatusRow,
    PilotEpisode,
    PilotSelectionResult,
)

DEFAULT_ASR_MODEL = "gpt-4o-mini-transcribe"
DEFAULT_ASR_COST_PER_MINUTE_USD = 0.003
_EXCLUDED_TITLE_PATTERNS = (
    "teaser",
    "trailer",
    "bande annonce",
    "extrait",
    "best of",
    "rediff",
    "replay",
)


class PilotSelectionError(RuntimeError):
    pass


class CorpusStatusError(RuntimeError):
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
        min_duration_seconds: int | None = None,
        notes: str | None = None,
    ) -> PilotSelectionResult:
        if target_seconds <= 0:
            raise PilotSelectionError("target_seconds must be positive")
        if selection_order not in {"newest", "oldest"}:
            raise PilotSelectionError("selection_order must be newest or oldest")
        if min_duration_seconds is not None and min_duration_seconds <= 0:
            raise PilotSelectionError("min_duration_seconds must be positive when provided")

        order_sql = "DESC" if selection_order == "newest" else "ASC"

        with connect(self.db_path) as connection:
            show = get_show_by_id(connection, show_id)
            if show is None:
                raise PilotSelectionError(f"show_id {show_id} not found")

            rows = connection.execute(
                f"""
                SELECT episode_id, title, published_at, audio_url, duration_seconds
                FROM episodes
                WHERE show_id = ?
                ORDER BY published_at {order_sql}, episode_id {order_sql}
                """,
                (show_id,),
            ).fetchall()

            eligible_rows: list[sqlite3.Row] = []
            skipped_count = 0
            for row in rows:
                if not _is_episode_eligible(
                    title=str(row["title"]),
                    audio_url=row["audio_url"],
                    duration_seconds=row["duration_seconds"],
                    min_duration_seconds=min_duration_seconds,
                ):
                    skipped_count += 1
                    continue
                eligible_rows.append(row)

            selected: list[PilotEpisode] = []
            cumulative_seconds = 0

            for row in eligible_rows:
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


class CorpusStatusService:
    def __init__(self, *, db_path: Path, asr_model: str = DEFAULT_ASR_MODEL) -> None:
        self.db_path = db_path
        self.asr_model = asr_model

    def inspect(self) -> CorpusStatusResult:
        with connect(self.db_path) as connection:
            show_rows = connection.execute(
                """
                SELECT
                    s.show_id,
                    s.title,
                    s.feed_url,
                    COUNT(e.episode_id) AS episode_count,
                    COALESCE(SUM(COALESCE(e.duration_seconds, 0)), 0) AS total_seconds,
                    COALESCE(
                        SUM(COALESCE(e.has_transcript_tag, 0)),
                        0
                    ) AS episodes_with_transcript_tag
                FROM shows s
                LEFT JOIN episodes e
                    ON e.show_id = s.show_id
                GROUP BY s.show_id, s.title, s.feed_url
                ORDER BY s.title, s.show_id
                """
            ).fetchall()
            if not show_rows:
                raise CorpusStatusError("no saved shows found for corpus inspection")

            slice_rows = connection.execute(
                """
                SELECT
                    pr.pilot_run_id,
                    pr.show_id,
                    pr.name,
                    pr.selection_order,
                    COUNT(pre.episode_id) AS selected_episodes,
                    COALESCE(MAX(pre.cumulative_seconds), 0) AS selected_seconds,
                    COALESCE(SUM(CASE WHEN ts.status = 'needs_asr' THEN 1 ELSE 0 END), 0)
                        AS needs_asr_episodes,
                    COALESCE(SUM(CASE WHEN ts.status = 'in_progress' THEN 1 ELSE 0 END), 0)
                        AS in_progress_asr_episodes,
                    COALESCE(SUM(CASE WHEN ts.status = 'ready' THEN 1 ELSE 0 END), 0)
                        AS ready_asr_episodes,
                    COALESCE(SUM(CASE WHEN ts.status = 'failed' THEN 1 ELSE 0 END), 0)
                        AS failed_asr_episodes
                FROM pilot_runs pr
                LEFT JOIN pilot_run_episodes pre
                    ON pre.pilot_run_id = pr.pilot_run_id
                LEFT JOIN transcript_sources ts
                    ON ts.episode_id = pre.episode_id
                    AND ts.source_type = 'asr'
                    AND ts.model = ?
                GROUP BY pr.pilot_run_id, pr.show_id, pr.name, pr.selection_order
                ORDER BY pr.name, pr.pilot_run_id
                """,
                (self.asr_model,),
            ).fetchall()

        slice_rows_by_show: dict[int, list[sqlite3.Row]] = defaultdict(list)
        for row in slice_rows:
            slice_rows_by_show[int(row["show_id"])].append(row)

        rows: list[CorpusStatusRow] = []
        for show_row in show_rows:
            show_id = int(show_row["show_id"])
            matched_slice_rows = slice_rows_by_show.get(show_id)
            if not matched_slice_rows:
                rows.append(
                    CorpusStatusRow(
                        show_id=show_id,
                        title=str(show_row["title"]),
                        feed_url=str(show_row["feed_url"]),
                        episode_count=int(show_row["episode_count"]),
                        total_seconds=int(show_row["total_seconds"]),
                        episodes_with_transcript_tag=int(show_row["episodes_with_transcript_tag"]),
                        slice_id=None,
                        slice_name=None,
                        slice_selection_order=None,
                        selected_episodes=0,
                        selected_seconds=0,
                        needs_asr_episodes=0,
                        in_progress_asr_episodes=0,
                        ready_asr_episodes=0,
                        failed_asr_episodes=0,
                    )
                )
                continue

            for slice_row in matched_slice_rows:
                rows.append(
                    CorpusStatusRow(
                        show_id=show_id,
                        title=str(show_row["title"]),
                        feed_url=str(show_row["feed_url"]),
                        episode_count=int(show_row["episode_count"]),
                        total_seconds=int(show_row["total_seconds"]),
                        episodes_with_transcript_tag=int(show_row["episodes_with_transcript_tag"]),
                        slice_id=int(slice_row["pilot_run_id"]),
                        slice_name=str(slice_row["name"]),
                        slice_selection_order=str(slice_row["selection_order"]),
                        selected_episodes=int(slice_row["selected_episodes"]),
                        selected_seconds=int(slice_row["selected_seconds"]),
                        needs_asr_episodes=int(slice_row["needs_asr_episodes"]),
                        in_progress_asr_episodes=int(slice_row["in_progress_asr_episodes"]),
                        ready_asr_episodes=int(slice_row["ready_asr_episodes"]),
                        failed_asr_episodes=int(slice_row["failed_asr_episodes"]),
                    )
                )

        return CorpusStatusResult(
            show_count=len(show_rows),
            slice_count=len(slice_rows),
            episode_count=sum(int(row["episode_count"]) for row in show_rows),
            total_seconds=sum(int(row["total_seconds"]) for row in show_rows),
            episodes_with_transcript_tag=sum(
                int(row["episodes_with_transcript_tag"]) for row in show_rows
            ),
            selected_slice_episodes=sum(int(row["selected_episodes"]) for row in slice_rows),
            selected_slice_seconds=sum(int(row["selected_seconds"]) for row in slice_rows),
            needs_asr_episodes=sum(int(row["needs_asr_episodes"]) for row in slice_rows),
            in_progress_asr_episodes=sum(
                int(row["in_progress_asr_episodes"]) for row in slice_rows
            ),
            ready_asr_episodes=sum(int(row["ready_asr_episodes"]) for row in slice_rows),
            failed_asr_episodes=sum(int(row["failed_asr_episodes"]) for row in slice_rows),
            rows=tuple(rows),
        )


def _is_episode_eligible(
    *,
    title: str,
    audio_url: str | None,
    duration_seconds: int | None,
    min_duration_seconds: int | None,
) -> bool:
    if audio_url is None or audio_url == "":
        return False
    if duration_seconds is None or duration_seconds <= 0:
        return False
    if min_duration_seconds is not None and duration_seconds < min_duration_seconds:
        return False

    normalized_title = normalize_text(title)
    return not any(pattern in normalized_title for pattern in _EXCLUDED_TITLE_PATTERNS)
