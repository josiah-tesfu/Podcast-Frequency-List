from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from podcast_frequency_list.asr import AsrRunService
from podcast_frequency_list.asr.models import AsrRunResult
from podcast_frequency_list.asr.service import AsrRunError
from podcast_frequency_list.db import connect
from podcast_frequency_list.normalize import TranscriptNormalizationService
from podcast_frequency_list.normalize.models import NormalizationRunResult
from podcast_frequency_list.qc import SegmentQcService
from podcast_frequency_list.qc.models import QcRunResult
from podcast_frequency_list.sentences import SentenceSplitService
from podcast_frequency_list.sentences.models import SentenceSplitResult
from podcast_frequency_list.show_manifest import load_show_manifest
from podcast_frequency_list.show_slices import _build_slice_name
from podcast_frequency_list.tokens import CandidateInventoryService, SentenceTokenizationService
from podcast_frequency_list.tokens.models import CandidateInventoryResult, TokenizationResult

_NO_ASR_READY_MESSAGE = "no ASR-ready pilot episodes found"


class ShowProcessingError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShowProcessingRow:
    slug: str
    show_id: int
    slice_id: int
    slice_name: str
    selected_episodes: int
    asr_selected_episodes: int
    asr_completed_episodes: int
    asr_failed_episodes: int
    normalized_segments: int
    qc_processed_segments: int
    created_sentences: int
    created_tokens: int
    created_occurrences: int


@dataclass(frozen=True)
class ShowProcessingResult:
    selected_shows: int
    processed_slices: int
    selected_episodes: int
    asr_selected_episodes: int
    asr_completed_episodes: int
    asr_failed_episodes: int
    normalized_segments: int
    qc_processed_segments: int
    created_sentences: int
    created_tokens: int
    created_occurrences: int
    rows: tuple[ShowProcessingRow, ...]


@dataclass(frozen=True)
class _SliceInfo:
    show_id: int
    slice_id: int
    selected_episodes: int


class ShowProcessingService:
    def __init__(
        self,
        *,
        db_path: Path,
        asr_run_service: AsrRunService | None,
        transcript_normalization_service: TranscriptNormalizationService,
        segment_qc_service: SegmentQcService,
        sentence_split_service: SentenceSplitService,
        sentence_tokenization_service: SentenceTokenizationService,
        candidate_inventory_service: CandidateInventoryService,
    ) -> None:
        self.db_path = db_path
        self.asr_run_service = asr_run_service
        self.transcript_normalization_service = transcript_normalization_service
        self.segment_qc_service = segment_qc_service
        self.sentence_split_service = sentence_split_service
        self.sentence_tokenization_service = sentence_tokenization_service
        self.candidate_inventory_service = candidate_inventory_service

    def close(self) -> None:
        for service in (
            self.asr_run_service,
            self.transcript_normalization_service,
            self.segment_qc_service,
            self.sentence_split_service,
            self.sentence_tokenization_service,
            self.candidate_inventory_service,
        ):
            if service is None:
                continue
            close = getattr(service, "close", None)
            if close is not None:
                close()

    def process_manifest(
        self,
        *,
        manifest_path: Path | None = None,
        skip_asr: bool = False,
    ) -> ShowProcessingResult:
        manifest_rows = tuple(row for row in load_show_manifest(manifest_path) if row.enabled)
        if not manifest_rows:
            raise ShowProcessingError("no enabled shows found in manifest")

        rows: list[ShowProcessingRow] = []
        for manifest_row in manifest_rows:
            slice_name = _build_slice_name(manifest_row)
            slice_info = self._load_slice_info(slice_name=slice_name, slug=manifest_row.slug)
            asr_result = self._run_asr(slice_name=slice_name, skip_asr=skip_asr)
            normalization_result = self._run_normalization(slice_name=slice_name)
            qc_result = self._run_qc(slice_name=slice_name)
            split_result = self._run_split(slice_name=slice_name)
            tokenization_result = self._run_tokenize(slice_name=slice_name)
            inventory_result = self._run_generate(slice_name=slice_name)
            rows.append(
                ShowProcessingRow(
                    slug=manifest_row.slug,
                    show_id=slice_info.show_id,
                    slice_id=slice_info.slice_id,
                    slice_name=slice_name,
                    selected_episodes=slice_info.selected_episodes,
                    asr_selected_episodes=asr_result.selected_count,
                    asr_completed_episodes=asr_result.completed_count,
                    asr_failed_episodes=asr_result.failed_count,
                    normalized_segments=normalization_result.normalized_segments,
                    qc_processed_segments=qc_result.processed_segments,
                    created_sentences=split_result.created_sentences,
                    created_tokens=tokenization_result.created_tokens,
                    created_occurrences=inventory_result.created_occurrences,
                )
            )

        return ShowProcessingResult(
            selected_shows=len(manifest_rows),
            processed_slices=len(rows),
            selected_episodes=sum(row.selected_episodes for row in rows),
            asr_selected_episodes=sum(row.asr_selected_episodes for row in rows),
            asr_completed_episodes=sum(row.asr_completed_episodes for row in rows),
            asr_failed_episodes=sum(row.asr_failed_episodes for row in rows),
            normalized_segments=sum(row.normalized_segments for row in rows),
            qc_processed_segments=sum(row.qc_processed_segments for row in rows),
            created_sentences=sum(row.created_sentences for row in rows),
            created_tokens=sum(row.created_tokens for row in rows),
            created_occurrences=sum(row.created_occurrences for row in rows),
            rows=tuple(rows),
        )

    def _run_asr(self, *, slice_name: str, skip_asr: bool) -> AsrRunResult:
        if skip_asr:
            return self._empty_asr_result(slice_name=slice_name)
        if self.asr_run_service is None:
            raise ShowProcessingError("asr_run_service is required unless skip_asr is enabled")
        try:
            return self.asr_run_service.run_pilot(pilot_name=slice_name)
        except AsrRunError as exc:
            if str(exc) == _NO_ASR_READY_MESSAGE:
                return self._empty_asr_result(slice_name=slice_name)
            raise ShowProcessingError(f"slice={slice_name} stage=asr: {exc}") from exc
        except Exception as exc:
            raise ShowProcessingError(f"slice={slice_name} stage=asr: {exc}") from exc

    def _empty_asr_result(self, *, slice_name: str) -> AsrRunResult:
        model = str(
            getattr(
                getattr(self.asr_run_service, "transcriber", None),
                "model",
                "unknown",
            )
        )
        return AsrRunResult(
            pilot_name=slice_name,
            model=model,
            requested_limit=None,
            selected_count=0,
            completed_count=0,
            skipped_count=0,
            failed_count=0,
            chunk_count=0,
            episode_results=(),
        )

    def _run_normalization(self, *, slice_name: str) -> NormalizationRunResult:
        return self._run_stage(
            slice_name=slice_name,
            stage_name="normalize",
            run=lambda: self.transcript_normalization_service.normalize(pilot_name=slice_name),
        )

    def _run_qc(self, *, slice_name: str) -> QcRunResult:
        return self._run_stage(
            slice_name=slice_name,
            stage_name="qc",
            run=lambda: self.segment_qc_service.run(pilot_name=slice_name),
        )

    def _run_split(self, *, slice_name: str) -> SentenceSplitResult:
        return self._run_stage(
            slice_name=slice_name,
            stage_name="split",
            run=lambda: self.sentence_split_service.split(pilot_name=slice_name),
        )

    def _run_tokenize(self, *, slice_name: str) -> TokenizationResult:
        return self._run_stage(
            slice_name=slice_name,
            stage_name="tokenize",
            run=lambda: self.sentence_tokenization_service.tokenize(pilot_name=slice_name),
        )

    def _run_generate(self, *, slice_name: str) -> CandidateInventoryResult:
        return self._run_stage(
            slice_name=slice_name,
            stage_name="generate",
            run=lambda: self.candidate_inventory_service.generate(pilot_name=slice_name),
        )

    def _run_stage(self, *, slice_name: str, stage_name: str, run: object) -> object:
        try:
            return run()
        except Exception as exc:
            raise ShowProcessingError(f"slice={slice_name} stage={stage_name}: {exc}") from exc

    def _load_slice_info(self, *, slice_name: str, slug: str) -> _SliceInfo:
        with connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT
                    pr.pilot_run_id,
                    pr.show_id,
                    COUNT(pre.episode_id) AS selected_episodes
                FROM pilot_runs pr
                LEFT JOIN pilot_run_episodes pre
                    ON pre.pilot_run_id = pr.pilot_run_id
                WHERE pr.name = ?
                GROUP BY pr.pilot_run_id, pr.show_id
                """,
                (slice_name,),
            ).fetchone()
        if row is None:
            raise ShowProcessingError(f"manifest slice not found in DB for slug={slug}")
        if int(row["selected_episodes"]) <= 0:
            raise ShowProcessingError(f"manifest slice has no episodes for slug={slug}")
        return _SliceInfo(
            show_id=int(row["show_id"]),
            slice_id=int(row["pilot_run_id"]),
            selected_episodes=int(row["selected_episodes"]),
        )
