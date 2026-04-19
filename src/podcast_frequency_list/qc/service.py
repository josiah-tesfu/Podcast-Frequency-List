from __future__ import annotations

import sqlite3
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.qc.models import QcRunResult
from podcast_frequency_list.qc.rules import QcInputSegment, evaluate_segment_qc

QC_VERSION = "1"


class SegmentQcError(RuntimeError):
    pass


class SegmentQcService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def run(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> QcRunResult:
        if (pilot_name is None and episode_id is None) or (
            pilot_name is not None and episode_id is not None
        ):
            raise SegmentQcError("provide exactly one of pilot_name or episode_id")

        target_rows = self._load_target_segments(pilot_name=pilot_name, episode_id=episode_id)
        if not target_rows:
            raise SegmentQcError("no normalized segments found for qc")

        show_ids = {int(row["show_id"]) for row in target_rows}
        if len(show_ids) != 1:
            raise SegmentQcError("qc scope must resolve to exactly one show")

        show_id = show_ids.pop()
        reference_rows = self._load_reference_segments(show_id=show_id)
        evaluations = evaluate_segment_qc(
            target_segments=[self._to_input_segment(row) for row in target_rows],
            reference_segments=[self._to_input_segment(row) for row in reference_rows],
        )

        processed_segments = 0
        skipped_segments = 0
        keep_segments = 0
        review_segments = 0
        remove_segments = 0

        with connect(self.db_path) as connection:
            for row in target_rows:
                if row["qc_version"] == QC_VERSION and not force:
                    skipped_segments += 1
                    continue

                evaluation = evaluations[int(row["segment_id"])]
                connection.execute(
                    """
                    DELETE FROM segment_qc_flags
                    WHERE segment_id = ?
                    AND qc_version = ?
                    """,
                    (row["segment_id"], QC_VERSION),
                )
                connection.execute(
                    """
                    INSERT INTO segment_qc (
                        segment_id,
                        episode_id,
                        qc_version,
                        status,
                        reason_summary
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(segment_id, qc_version) DO UPDATE SET
                        episode_id = excluded.episode_id,
                        status = excluded.status,
                        reason_summary = excluded.reason_summary,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        evaluation.segment_id,
                        evaluation.episode_id,
                        QC_VERSION,
                        evaluation.status,
                        evaluation.reason_summary,
                    ),
                )

                for flag in evaluation.flags:
                    connection.execute(
                        """
                        INSERT INTO segment_qc_flags (
                            segment_id,
                            qc_version,
                            flag,
                            rule_name,
                            details
                        )
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(segment_id, qc_version, flag, rule_name) DO UPDATE SET
                            details = excluded.details
                        """,
                        (
                            evaluation.segment_id,
                            QC_VERSION,
                            flag.flag,
                            flag.rule_name,
                            flag.details,
                        ),
                    )

                processed_segments += 1
                if evaluation.status == "keep":
                    keep_segments += 1
                elif evaluation.status == "review":
                    review_segments += 1
                else:
                    remove_segments += 1

            connection.commit()

        if pilot_name is not None:
            scope = "pilot"
            scope_value = pilot_name
        else:
            scope = "episode"
            scope_value = str(episode_id)

        return QcRunResult(
            scope=scope,
            scope_value=scope_value,
            qc_version=QC_VERSION,
            selected_segments=len(target_rows),
            processed_segments=processed_segments,
            skipped_segments=skipped_segments,
            keep_segments=keep_segments,
            review_segments=review_segments,
            remove_segments=remove_segments,
        )

    def _load_target_segments(
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
                        ns.segment_id,
                        ns.episode_id,
                        e.show_id,
                        ts.chunk_index,
                        ns.normalized_text,
                        sq.qc_version
                    FROM normalized_segments ns
                    JOIN transcript_segments ts
                        ON ts.segment_id = ns.segment_id
                    JOIN episodes e
                        ON e.episode_id = ns.episode_id
                    JOIN transcript_sources src
                        ON src.source_id = ts.source_id
                    JOIN pilot_run_episodes pre
                        ON pre.episode_id = ns.episode_id
                    JOIN pilot_runs pr
                        ON pr.pilot_run_id = pre.pilot_run_id
                    LEFT JOIN segment_qc sq
                        ON sq.segment_id = ns.segment_id
                        AND sq.qc_version = ?
                    WHERE pr.name = ?
                    AND src.status = 'ready'
                    ORDER BY pre.position, ts.chunk_index
                    """,
                    (QC_VERSION, pilot_name),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT
                        ns.segment_id,
                        ns.episode_id,
                        e.show_id,
                        ts.chunk_index,
                        ns.normalized_text,
                        sq.qc_version
                    FROM normalized_segments ns
                    JOIN transcript_segments ts
                        ON ts.segment_id = ns.segment_id
                    JOIN episodes e
                        ON e.episode_id = ns.episode_id
                    JOIN transcript_sources src
                        ON src.source_id = ts.source_id
                    LEFT JOIN segment_qc sq
                        ON sq.segment_id = ns.segment_id
                        AND sq.qc_version = ?
                    WHERE ns.episode_id = ?
                    AND src.status = 'ready'
                    ORDER BY ts.chunk_index
                    """,
                    (QC_VERSION, episode_id),
                ).fetchall()
        return list(rows)

    def _load_reference_segments(self, *, show_id: int) -> list[sqlite3.Row]:
        with connect(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    ns.segment_id,
                    ns.episode_id,
                    e.show_id,
                    ts.chunk_index,
                    ns.normalized_text
                FROM normalized_segments ns
                JOIN transcript_segments ts
                    ON ts.segment_id = ns.segment_id
                JOIN transcript_sources src
                    ON src.source_id = ts.source_id
                JOIN episodes e
                    ON e.episode_id = ns.episode_id
                WHERE e.show_id = ?
                AND src.status = 'ready'
                ORDER BY ns.episode_id, ts.chunk_index
                """,
                (show_id,),
            ).fetchall()
        return list(rows)

    def _to_input_segment(self, row: sqlite3.Row) -> QcInputSegment:
        return QcInputSegment(
            segment_id=int(row["segment_id"]),
            episode_id=int(row["episode_id"]),
            chunk_index=int(row["chunk_index"]),
            normalized_text=str(row["normalized_text"]),
        )
