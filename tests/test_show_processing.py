from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pytest

from podcast_frequency_list.asr.models import AsrRunResult
from podcast_frequency_list.asr.service import AsrRunError
from podcast_frequency_list.db import bootstrap_database, connect, upsert_episode, upsert_show
from podcast_frequency_list.normalize.models import NormalizationRunResult
from podcast_frequency_list.pilot.service import PilotSelectionService
from podcast_frequency_list.qc.models import QcRunResult
from podcast_frequency_list.sentences.models import SentenceSplitResult
from podcast_frequency_list.show_manifest import load_show_manifest
from podcast_frequency_list.show_processing import ShowProcessingError, ShowProcessingService
from podcast_frequency_list.show_slices import _build_slice_name
from podcast_frequency_list.tokens.models import CandidateInventoryResult, TokenizationResult


def _build_manifest(tmp_path: Path) -> Path:
    manifest_path = tmp_path / "show_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "slug,title,feed_url,language,bucket,family,target_hours,selection_order,enabled,notes",
                "zack,Zack en Roue Libre,https://example.com/zack.xml,fr,native,test,1,newest,1,one",
                "flood,FloodCast,https://example.com/flood.xml,fr,native,test,1,newest,1,two",
            ]
        ),
        encoding="utf-8",
    )
    return manifest_path


def _seed_manifest_slices(tmp_path: Path) -> tuple[Path, Path]:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    manifest_path = _build_manifest(tmp_path)

    with connect(db_path) as connection:
        zack_id = upsert_show(
            connection,
            title="Zack en Roue Libre",
            feed_url="https://example.com/zack.xml",
        )
        flood_id = upsert_show(
            connection,
            title="FloodCast",
            feed_url="https://example.com/flood.xml",
        )
        for index in range(2):
            upsert_episode(
                connection,
                show_id=zack_id,
                guid=f"z-{index}",
                title=f"Episode Zack {index}",
                published_at=f"2025-01-0{index + 1}T00:00:00+00:00",
                audio_url=f"https://cdn.example.com/zack-{index}.mp3",
                duration_seconds=1_800,
            )
            upsert_episode(
                connection,
                show_id=flood_id,
                guid=f"f-{index}",
                title=f"Episode Flood {index}",
                published_at=f"2025-01-0{index + 1}T00:00:00+00:00",
                audio_url=f"https://cdn.example.com/flood-{index}.mp3",
                duration_seconds=1_800,
            )
        connection.commit()

    selection_service = PilotSelectionService(db_path=db_path)
    show_ids = {
        "zack": zack_id,
        "flood": flood_id,
    }
    for row in load_show_manifest(manifest_path):
        selection_service.create_pilot(
            show_id=show_ids[row.slug],
            name=_build_slice_name(row),
            target_seconds=round(row.target_hours * 3600),
            selection_order=row.selection_order,
        )

    return db_path, manifest_path


class _FakeAsrService:
    def __init__(self, calls: list[tuple[str, str]]) -> None:
        self.calls = calls
        self.transcriber = type("_T", (), {"model": "test-asr"})()
        self._call_counts: dict[str, int] = defaultdict(int)
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def run_pilot(
        self,
        *,
        pilot_name: str,
        limit: int | None = None,
        force: bool = False,
    ) -> AsrRunResult:
        del limit, force
        self.calls.append(("asr", pilot_name))
        self._call_counts[pilot_name] += 1
        if self._call_counts[pilot_name] > 1:
            raise AsrRunError("no ASR-ready pilot episodes found")
        return AsrRunResult(
            pilot_name=pilot_name,
            model="test-asr",
            requested_limit=None,
            selected_count=2,
            completed_count=2,
            skipped_count=0,
            failed_count=0,
            chunk_count=4,
            episode_results=(),
        )


class _FakeNormalizationService:
    def __init__(self, calls: list[tuple[str, str]]) -> None:
        self.calls = calls

    def normalize(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> NormalizationRunResult:
        del episode_id, force
        self.calls.append(("normalize", str(pilot_name)))
        return NormalizationRunResult(
            scope="pilot",
            scope_value=str(pilot_name),
            normalization_version="1",
            selected_segments=10,
            normalized_segments=8,
            skipped_segments=2,
            episode_count=2,
        )


class _FakeQcService:
    def __init__(self, calls: list[tuple[str, str]], *, fail_slice: str | None = None) -> None:
        self.calls = calls
        self.fail_slice = fail_slice

    def run(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> QcRunResult:
        del episode_id, force
        slice_name = str(pilot_name)
        self.calls.append(("qc", slice_name))
        if self.fail_slice == slice_name:
            raise RuntimeError("qc boom")
        return QcRunResult(
            scope="pilot",
            scope_value=slice_name,
            qc_version="1",
            selected_segments=8,
            processed_segments=8,
            skipped_segments=0,
            keep_segments=6,
            review_segments=1,
            remove_segments=1,
        )


class _FakeSentenceSplitService:
    def __init__(self, calls: list[tuple[str, str]]) -> None:
        self.calls = calls

    def split(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> SentenceSplitResult:
        del episode_id, force
        self.calls.append(("split", str(pilot_name)))
        return SentenceSplitResult(
            scope="pilot",
            scope_value=str(pilot_name),
            split_version="1",
            selected_segments=6,
            created_sentences=12,
            skipped_segments=0,
            episode_count=2,
        )


class _FakeTokenizationService:
    def __init__(self, calls: list[tuple[str, str]]) -> None:
        self.calls = calls

    def tokenize(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> TokenizationResult:
        del episode_id, force
        self.calls.append(("tokenize", str(pilot_name)))
        return TokenizationResult(
            scope="pilot",
            scope_value=str(pilot_name),
            tokenization_version="1",
            selected_sentences=12,
            tokenized_sentences=12,
            created_tokens=30,
            skipped_sentences=0,
            episode_count=2,
        )


class _FakeInventoryService:
    def __init__(self, calls: list[tuple[str, str]]) -> None:
        self.calls = calls

    def generate(
        self,
        *,
        pilot_name: str | None = None,
        episode_id: int | None = None,
        force: bool = False,
    ) -> CandidateInventoryResult:
        del episode_id, force
        self.calls.append(("generate", str(pilot_name)))
        return CandidateInventoryResult(
            scope="pilot",
            scope_value=str(pilot_name),
            inventory_version="1",
            selected_sentences=12,
            processed_sentences=12,
            skipped_sentences=0,
            created_candidates=20,
            created_occurrences=40,
            episode_count=2,
        )


def test_show_processing_service_processes_manifest_slices_and_allows_rerun(
    tmp_path: Path,
) -> None:
    db_path, manifest_path = _seed_manifest_slices(tmp_path)
    calls: list[tuple[str, str]] = []
    asr_service = _FakeAsrService(calls)
    service = ShowProcessingService(
        db_path=db_path,
        asr_run_service=asr_service,
        transcript_normalization_service=_FakeNormalizationService(calls),
        segment_qc_service=_FakeQcService(calls),
        sentence_split_service=_FakeSentenceSplitService(calls),
        sentence_tokenization_service=_FakeTokenizationService(calls),
        candidate_inventory_service=_FakeInventoryService(calls),
    )

    first_result = service.process_manifest(manifest_path=manifest_path)
    second_result = service.process_manifest(manifest_path=manifest_path)
    service.close()

    assert first_result.selected_shows == 2
    assert first_result.processed_slices == 2
    assert first_result.selected_episodes == 4
    assert first_result.asr_selected_episodes == 4
    assert first_result.asr_completed_episodes == 4
    assert first_result.normalized_segments == 16
    assert first_result.qc_processed_segments == 16
    assert first_result.created_sentences == 24
    assert first_result.created_tokens == 60
    assert first_result.created_occurrences == 80
    assert [row.slice_name for row in first_result.rows] == ["zack-1h-slice", "flood-1h-slice"]

    assert second_result.selected_shows == 2
    assert second_result.processed_slices == 2
    assert second_result.asr_selected_episodes == 0
    assert second_result.asr_completed_episodes == 0
    assert second_result.created_occurrences == 80
    assert asr_service.closed is True
    assert calls[:12] == [
        ("asr", "zack-1h-slice"),
        ("normalize", "zack-1h-slice"),
        ("qc", "zack-1h-slice"),
        ("split", "zack-1h-slice"),
        ("tokenize", "zack-1h-slice"),
        ("generate", "zack-1h-slice"),
        ("asr", "flood-1h-slice"),
        ("normalize", "flood-1h-slice"),
        ("qc", "flood-1h-slice"),
        ("split", "flood-1h-slice"),
        ("tokenize", "flood-1h-slice"),
        ("generate", "flood-1h-slice"),
    ]


def test_show_processing_service_stops_on_stage_failure(tmp_path: Path) -> None:
    db_path, manifest_path = _seed_manifest_slices(tmp_path)
    calls: list[tuple[str, str]] = []
    service = ShowProcessingService(
        db_path=db_path,
        asr_run_service=_FakeAsrService(calls),
        transcript_normalization_service=_FakeNormalizationService(calls),
        segment_qc_service=_FakeQcService(calls, fail_slice="zack-1h-slice"),
        sentence_split_service=_FakeSentenceSplitService(calls),
        sentence_tokenization_service=_FakeTokenizationService(calls),
        candidate_inventory_service=_FakeInventoryService(calls),
    )

    with pytest.raises(ShowProcessingError, match="slice=zack-1h-slice stage=qc: qc boom"):
        service.process_manifest(manifest_path=manifest_path)

    assert calls == [
        ("asr", "zack-1h-slice"),
        ("normalize", "zack-1h-slice"),
        ("qc", "zack-1h-slice"),
    ]


def test_show_processing_service_supports_skip_asr(tmp_path: Path) -> None:
    db_path, manifest_path = _seed_manifest_slices(tmp_path)
    calls: list[tuple[str, str]] = []
    service = ShowProcessingService(
        db_path=db_path,
        asr_run_service=None,
        transcript_normalization_service=_FakeNormalizationService(calls),
        segment_qc_service=_FakeQcService(calls),
        sentence_split_service=_FakeSentenceSplitService(calls),
        sentence_tokenization_service=_FakeTokenizationService(calls),
        candidate_inventory_service=_FakeInventoryService(calls),
    )

    result = service.process_manifest(manifest_path=manifest_path, skip_asr=True)

    assert result.asr_selected_episodes == 0
    assert result.asr_completed_episodes == 0
    assert result.asr_failed_episodes == 0
    assert calls[:5] == [
        ("normalize", "zack-1h-slice"),
        ("qc", "zack-1h-slice"),
        ("split", "zack-1h-slice"),
        ("tokenize", "zack-1h-slice"),
        ("generate", "zack-1h-slice"),
    ]


def test_show_processing_service_errors_when_manifest_slice_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    bootstrap_database(db_path)
    manifest_path = _build_manifest(tmp_path)
    calls: list[tuple[str, str]] = []
    service = ShowProcessingService(
        db_path=db_path,
        asr_run_service=_FakeAsrService(calls),
        transcript_normalization_service=_FakeNormalizationService(calls),
        segment_qc_service=_FakeQcService(calls),
        sentence_split_service=_FakeSentenceSplitService(calls),
        sentence_tokenization_service=_FakeTokenizationService(calls),
        candidate_inventory_service=_FakeInventoryService(calls),
    )

    with pytest.raises(ShowProcessingError, match="slug=zack"):
        service.process_manifest(manifest_path=manifest_path)
