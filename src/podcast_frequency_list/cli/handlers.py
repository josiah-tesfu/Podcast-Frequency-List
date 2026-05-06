from __future__ import annotations

from collections.abc import Iterable

from podcast_frequency_list.asr import AsrRunService
from podcast_frequency_list.cli.emitters import (
    emit_asr_result,
    emit_candidate_metrics_validation,
    emit_candidate_rows,
    emit_candidate_scores_result,
)
from podcast_frequency_list.cli.output import emit_fields
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.db import bootstrap_database
from podcast_frequency_list.tokens import (
    CandidateMetricsError,
    CandidateMetricsService,
    CandidateScoresError,
    CandidateScoresService,
)

_DEFAULT_CANDIDATE_INSPECTION_KEYS = (
    "en fait",
    "du coup",
    "je pense",
    "il y a",
    "tu vois",
)


def emit_info() -> None:
    settings = load_settings()
    emit_fields(
        (
            ("project_root", settings.project_root),
            ("db_path", settings.db_path),
            ("raw_data_dir", settings.raw_data_dir),
            ("processed_data_dir", settings.processed_data_dir),
        )
    )


def emit_initialized_db() -> None:
    emit_fields((("initialized_db", bootstrap_database()),))


def run_asr_and_emit(
    service: AsrRunService,
    *,
    pilot: str,
    limit: int | None,
    force: bool,
) -> int:
    result = service.run_pilot(pilot_name=pilot, limit=limit, force=force)
    emit_asr_result(result)
    return result.failed_count


def inspect_candidate_metrics(
    service: CandidateMetricsService,
    *,
    limit: int,
    candidate_keys: Iterable[str] | None,
) -> None:
    validation = service.validate()
    if validation.candidate_count == 0:
        raise CandidateMetricsError("no token candidates found for inventory inspection")

    emit_candidate_metrics_validation(validation)
    _emit_top_candidates(service, limit=limit)
    _emit_focus_candidates(service, candidate_keys=candidate_keys)


def _emit_top_candidates(
    service: CandidateMetricsService,
    *,
    limit: int,
) -> None:
    for ngram_size in (1, 2, 3):
        rows = service.list_top_candidates(ngram_size=ngram_size, limit=limit)
        emit_fields(((f"top_candidate_count_{ngram_size}gram", len(rows)),))
        emit_candidate_rows(
            rows,
            record_type=f"top_{ngram_size}gram",
            include_step4=ngram_size >= 2,
            include_step5=ngram_size <= 2,
        )


def _emit_focus_candidates(
    service: CandidateMetricsService,
    *,
    candidate_keys: Iterable[str] | None,
) -> None:
    inspection_keys = (
        tuple(candidate_keys) if candidate_keys else _DEFAULT_CANDIDATE_INSPECTION_KEYS
    )
    matched_rows = service.list_candidates_by_key(candidate_keys=inspection_keys)
    matched_keys = {row.candidate_key for row in matched_rows}
    missing_keys = tuple(key for key in inspection_keys if key not in matched_keys)

    emit_fields((("focus_candidate_count", len(matched_rows)),))
    emit_candidate_rows(
        matched_rows,
        record_type="focus_candidate",
        include_step4=True,
        include_step5=True,
    )
    emit_fields((("focus_missing_count", len(missing_keys)),))
    for missing_key in missing_keys:
        emit_fields((("focus_missing", missing_key),))


def inspect_candidate_scores(
    service: CandidateScoresService,
    *,
    limit: int,
    candidate_keys: Iterable[str] | None,
) -> None:
    summary = service.summarize()
    if summary.stored_candidates == 0:
        raise CandidateScoresError("no candidate scores found for score inspection")

    emit_candidate_scores_result(summary)
    _emit_top_scored_candidates(service, limit=limit)
    _emit_focus_scored_candidates(service, candidate_keys=candidate_keys)


def _emit_top_scored_candidates(
    service: CandidateScoresService,
    *,
    limit: int,
) -> None:
    for ngram_size in (1, 2, 3):
        rows = service.list_top_candidates(ngram_size=ngram_size, limit=limit)
        emit_fields(((f"top_candidate_count_{ngram_size}gram", len(rows)),))
        emit_candidate_rows(
            rows,
            record_type=f"top_{ngram_size}gram",
            include_step4=ngram_size >= 2,
            include_step5=ngram_size <= 2,
            include_step6=True,
        )


def _emit_focus_scored_candidates(
    service: CandidateScoresService,
    *,
    candidate_keys: Iterable[str] | None,
) -> None:
    inspection_keys = (
        tuple(candidate_keys) if candidate_keys else _DEFAULT_CANDIDATE_INSPECTION_KEYS
    )
    matched_rows = service.list_candidates_by_key(candidate_keys=inspection_keys)
    matched_keys = {row.candidate_key for row in matched_rows}
    missing_keys = tuple(key for key in inspection_keys if key not in matched_keys)

    emit_fields((("focus_candidate_count", len(matched_rows)),))
    emit_candidate_rows(
        matched_rows,
        record_type="focus_candidate",
        include_step4=True,
        include_step5=True,
        include_step6=True,
    )
    emit_fields((("focus_missing_count", len(missing_keys)),))
    for missing_key in missing_keys:
        emit_fields((("focus_missing", missing_key),))
