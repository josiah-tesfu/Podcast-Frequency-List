from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.qc.service import QC_VERSION
from podcast_frequency_list.sentences.models import SentenceSplitResult
from podcast_frequency_list.sentences.splitter import split_segment_text
from podcast_frequency_list.transcript_scope import resolve_transcript_scope

SPLIT_VERSION = "1"


class SentenceSplitError(RuntimeError):
    pass


@dataclass(frozen=True)
class _SentenceSplitTarget:
    segment_id: int
    episode_id: int
    normalized_text: str
    existing_sentence_count: int


class SentenceSplitService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def split(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> SentenceSplitResult:
        scope = resolve_transcript_scope(
            pilot_name=pilot_name,
            episode_id=episode_id,
            error_type=SentenceSplitError,
        )

        targets = self._load_targets(
            pilot_name=scope.pilot_name,
            episode_id=scope.episode_id,
        )
        if not targets:
            raise SentenceSplitError("no keep segments found for sentence splitting")

        created_sentences = 0
        skipped_segments = 0
        episode_ids: set[int] = set()

        with connect(self.db_path) as connection:
            for target in targets:
                episode_ids.add(target.episode_id)
                if target.existing_sentence_count > 0 and not force:
                    skipped_segments += 1
                    continue

                connection.execute(
                    """
                    DELETE FROM segment_sentences
                    WHERE segment_id = ?
                    AND split_version = ?
                    """,
                    (target.segment_id, SPLIT_VERSION),
                )

                sentences = split_segment_text(target.normalized_text)
                for sentence in sentences:
                    connection.execute(
                        """
                        INSERT INTO segment_sentences (
                            segment_id,
                            episode_id,
                            split_version,
                            sentence_index,
                            char_start,
                            char_end,
                            sentence_text
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            target.segment_id,
                            target.episode_id,
                            SPLIT_VERSION,
                            sentence.sentence_index,
                            sentence.char_start,
                            sentence.char_end,
                            sentence.sentence_text,
                        ),
                    )
                created_sentences += len(sentences)

            connection.commit()

        return SentenceSplitResult(
            scope=scope.kind,
            scope_value=scope.scope_value,
            split_version=SPLIT_VERSION,
            selected_segments=len(targets),
            created_sentences=created_sentences,
            skipped_segments=skipped_segments,
            episode_count=len(episode_ids),
        )

    def _load_targets(
        self,
        *,
        pilot_name: str | None,
        episode_id: int | None,
    ) -> list[_SentenceSplitTarget]:
        with connect(self.db_path) as connection:
            if pilot_name is not None:
                rows = connection.execute(
                    """
                    SELECT
                        ns.segment_id,
                        ns.episode_id,
                        ns.normalized_text,
                        COUNT(ss.sentence_id) AS existing_sentence_count
                    FROM normalized_segments ns
                    JOIN transcript_segments ts
                        ON ts.segment_id = ns.segment_id
                    JOIN transcript_sources src
                        ON src.source_id = ts.source_id
                    JOIN segment_qc sq
                        ON sq.segment_id = ns.segment_id
                        AND sq.qc_version = ?
                        AND sq.status = 'keep'
                    JOIN pilot_run_episodes pre
                        ON pre.episode_id = ns.episode_id
                    JOIN pilot_runs pr
                        ON pr.pilot_run_id = pre.pilot_run_id
                    LEFT JOIN segment_sentences ss
                        ON ss.segment_id = ns.segment_id
                        AND ss.split_version = ?
                    WHERE pr.name = ?
                    AND src.status = 'ready'
                    GROUP BY
                        ns.segment_id,
                        ns.episode_id,
                        ts.chunk_index,
                        ns.normalized_text
                    ORDER BY pre.position, ts.chunk_index
                    """,
                    (QC_VERSION, SPLIT_VERSION, pilot_name),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT
                        ns.segment_id,
                        ns.episode_id,
                        ns.normalized_text,
                        COUNT(ss.sentence_id) AS existing_sentence_count
                    FROM normalized_segments ns
                    JOIN transcript_segments ts
                        ON ts.segment_id = ns.segment_id
                    JOIN transcript_sources src
                        ON src.source_id = ts.source_id
                    JOIN segment_qc sq
                        ON sq.segment_id = ns.segment_id
                        AND sq.qc_version = ?
                        AND sq.status = 'keep'
                    LEFT JOIN segment_sentences ss
                        ON ss.segment_id = ns.segment_id
                        AND ss.split_version = ?
                    WHERE ns.episode_id = ?
                    AND src.status = 'ready'
                    GROUP BY
                        ns.segment_id,
                        ns.episode_id,
                        ts.chunk_index,
                        ns.normalized_text
                    ORDER BY ts.chunk_index
                    """,
                    (QC_VERSION, SPLIT_VERSION, episode_id),
                ).fetchall()

        return [
            _SentenceSplitTarget(
                segment_id=int(row["segment_id"]),
                episode_id=int(row["episode_id"]),
                normalized_text=str(row["normalized_text"]),
                existing_sentence_count=int(row["existing_sentence_count"]),
            )
            for row in rows
        ]
