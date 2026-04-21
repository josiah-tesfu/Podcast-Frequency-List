from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.db import connect
from podcast_frequency_list.tokens.models import TokenizationResult
from podcast_frequency_list.tokens.tokenizer import tokenize_sentence_text
from podcast_frequency_list.transcript_scope import resolve_transcript_scope

TOKENIZATION_VERSION = "1"


class SentenceTokenizationError(RuntimeError):
    pass


@dataclass(frozen=True)
class _TokenizationTarget:
    sentence_id: int
    episode_id: int
    segment_id: int
    sentence_text: str
    existing_token_count: int


class SentenceTokenizationService:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path

    def tokenize(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> TokenizationResult:
        scope = resolve_transcript_scope(
            pilot_name=pilot_name,
            episode_id=episode_id,
            error_type=SentenceTokenizationError,
        )

        targets = self._load_targets(
            pilot_name=scope.pilot_name,
            episode_id=scope.episode_id,
        )
        if not targets:
            raise SentenceTokenizationError("no sentences found for tokenization")

        tokenized_sentences = 0
        created_tokens = 0
        skipped_sentences = 0
        episode_ids: set[int] = set()

        with connect(self.db_path) as connection:
            for target in targets:
                episode_ids.add(target.episode_id)
                if target.existing_token_count > 0 and not force:
                    skipped_sentences += 1
                    continue

                connection.execute(
                    """
                    DELETE FROM sentence_tokens
                    WHERE sentence_id = ?
                    AND tokenization_version = ?
                    """,
                    (target.sentence_id, TOKENIZATION_VERSION),
                )

                tokens = tokenize_sentence_text(target.sentence_text)
                for token in tokens:
                    connection.execute(
                        """
                        INSERT INTO sentence_tokens (
                            sentence_id,
                            episode_id,
                            segment_id,
                            tokenization_version,
                            token_index,
                            token_key,
                            surface_text,
                            char_start,
                            char_end,
                            token_type
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            target.sentence_id,
                            target.episode_id,
                            target.segment_id,
                            TOKENIZATION_VERSION,
                            token.token_index,
                            token.token_key,
                            token.surface_text,
                            token.char_start,
                            token.char_end,
                            token.token_type,
                        ),
                    )

                tokenized_sentences += 1
                created_tokens += len(tokens)

            connection.commit()

        return TokenizationResult(
            scope=scope.kind,
            scope_value=scope.scope_value,
            tokenization_version=TOKENIZATION_VERSION,
            selected_sentences=len(targets),
            tokenized_sentences=tokenized_sentences,
            created_tokens=created_tokens,
            skipped_sentences=skipped_sentences,
            episode_count=len(episode_ids),
        )

    def _load_targets(
        self,
        *,
        pilot_name: str | None,
        episode_id: int | None,
    ) -> list[_TokenizationTarget]:
        with connect(self.db_path) as connection:
            if pilot_name is not None:
                rows = connection.execute(
                    """
                    SELECT
                        ss.sentence_id,
                        ss.episode_id,
                        ss.segment_id,
                        ss.sentence_text,
                        COUNT(st.token_id) AS existing_token_count
                    FROM segment_sentences ss
                    JOIN pilot_run_episodes pre
                        ON pre.episode_id = ss.episode_id
                    JOIN pilot_runs pr
                        ON pr.pilot_run_id = pre.pilot_run_id
                    LEFT JOIN sentence_tokens st
                        ON st.sentence_id = ss.sentence_id
                        AND st.tokenization_version = ?
                    WHERE pr.name = ?
                    GROUP BY
                        ss.sentence_id,
                        ss.episode_id,
                        ss.segment_id,
                        ss.sentence_text,
                        pre.position,
                        ss.sentence_index
                    ORDER BY pre.position, ss.segment_id, ss.sentence_index
                    """,
                    (TOKENIZATION_VERSION, pilot_name),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT
                        ss.sentence_id,
                        ss.episode_id,
                        ss.segment_id,
                        ss.sentence_text,
                        COUNT(st.token_id) AS existing_token_count
                    FROM segment_sentences ss
                    LEFT JOIN sentence_tokens st
                        ON st.sentence_id = ss.sentence_id
                        AND st.tokenization_version = ?
                    WHERE ss.episode_id = ?
                    GROUP BY
                        ss.sentence_id,
                        ss.episode_id,
                        ss.segment_id,
                        ss.sentence_text,
                        ss.sentence_index
                    ORDER BY ss.segment_id, ss.sentence_index
                    """,
                    (TOKENIZATION_VERSION, episode_id),
                ).fetchall()

        return [
            _TokenizationTarget(
                sentence_id=int(row["sentence_id"]),
                episode_id=int(row["episode_id"]),
                segment_id=int(row["segment_id"]),
                sentence_text=str(row["sentence_text"]),
                existing_token_count=int(row["existing_token_count"]),
            )
            for row in rows
        ]
