from __future__ import annotations

from podcast_frequency_list.asr import AsrEpisodeResult, AsrRunResult
from podcast_frequency_list.cli.field_specs import (
    ASR_RESULT_FIELDS,
    CANDIDATE_INVENTORY_RESULT_FIELDS,
    CANDIDATE_METRICS_RESULT_FIELDS,
    CANDIDATE_METRICS_VALIDATION_FIELDS,
    NORMALIZATION_RESULT_FIELDS,
    PILOT_RESULT_FIELDS,
    QC_RESULT_FIELDS,
    SAVED_SHOW_FIELDS,
    SENTENCE_SPLIT_RESULT_FIELDS,
    SYNC_RESULT_FIELDS,
    TOKENIZATION_RESULT_FIELDS,
    FieldSource,
    FieldSpec,
)
from podcast_frequency_list.cli.output import emit_fields, emit_inline_fields, emit_record
from podcast_frequency_list.discovery.models import SavedShow
from podcast_frequency_list.ingest import SyncFeedResult
from podcast_frequency_list.normalize import NormalizationRunResult
from podcast_frequency_list.pilot import PilotSelectionResult
from podcast_frequency_list.qc import QcRunResult
from podcast_frequency_list.sentences import SentenceSplitResult
from podcast_frequency_list.tokens import (
    CandidateInventoryResult,
    CandidateMetricsResult,
    CandidateMetricsValidationResult,
    CandidateSummaryRow,
    TokenizationResult,
)


def emit_saved_show(saved_show: SavedShow) -> None:
    _emit_result_fields(saved_show, SAVED_SHOW_FIELDS)


def emit_sync_result(result: SyncFeedResult) -> None:
    _emit_result_fields(result, SYNC_RESULT_FIELDS)


def emit_pilot_result(result: PilotSelectionResult) -> None:
    _emit_result_fields(result, PILOT_RESULT_FIELDS)


def emit_asr_episode_result(result: AsrEpisodeResult) -> None:
    emit_inline_fields(
        (
            ("episode_id", result.episode_id),
            ("status", result.status),
            ("chunks", result.chunk_count),
            ("chars", result.text_chars),
        )
    )
    if result.transcript_path:
        emit_fields((("transcript_path", result.transcript_path),))
    if result.preview:
        emit_fields((("preview", result.preview),))
    if result.error:
        emit_fields((("error", result.error),))


def emit_asr_result(result: AsrRunResult) -> None:
    _emit_result_fields(result, ASR_RESULT_FIELDS)
    for episode_result in result.episode_results:
        emit_asr_episode_result(episode_result)


def emit_normalization_result(result: NormalizationRunResult) -> None:
    _emit_result_fields(result, NORMALIZATION_RESULT_FIELDS)


def emit_qc_result(result: QcRunResult) -> None:
    _emit_result_fields(result, QC_RESULT_FIELDS)


def emit_sentence_split_result(result: SentenceSplitResult) -> None:
    _emit_result_fields(result, SENTENCE_SPLIT_RESULT_FIELDS)


def emit_tokenization_result(result: TokenizationResult) -> None:
    _emit_result_fields(result, TOKENIZATION_RESULT_FIELDS)


def emit_candidate_inventory_result(result: CandidateInventoryResult) -> None:
    _emit_result_fields(result, CANDIDATE_INVENTORY_RESULT_FIELDS)


def emit_candidate_metrics_result(result: CandidateMetricsResult) -> None:
    _emit_result_fields(result, CANDIDATE_METRICS_RESULT_FIELDS)


def emit_candidate_metrics_validation(result: CandidateMetricsValidationResult) -> None:
    _emit_result_fields(result, CANDIDATE_METRICS_VALIDATION_FIELDS)


def emit_candidate_rows(
    rows: tuple[CandidateSummaryRow, ...],
    *,
    record_type: str,
    include_step4: bool = False,
    include_step5: bool = False,
) -> None:
    for rank, row in enumerate(rows, start=1):
        fields: list[tuple[str, object]] = [
            ("record", record_type),
            ("rank", rank),
            ("candidate_key", row.candidate_key),
            ("display_text", row.display_text),
            ("ngram_size", row.ngram_size),
            ("raw_frequency", row.raw_frequency),
            ("episode_dispersion", row.episode_dispersion),
            ("show_dispersion", row.show_dispersion),
        ]
        if include_step4:
            fields.extend(
                [
                    ("t_score", _optional_metric_value(row.t_score)),
                    ("npmi", _optional_metric_value(row.npmi)),
                    (
                        "left_context_type_count",
                        _optional_metric_value(row.left_context_type_count),
                    ),
                    (
                        "right_context_type_count",
                        _optional_metric_value(row.right_context_type_count),
                    ),
                    ("left_entropy", _optional_metric_value(row.left_entropy)),
                    ("right_entropy", _optional_metric_value(row.right_entropy)),
                ]
            )
        if include_step5:
            fields.extend(
                [
                    (
                        "covered_by_any_count",
                        _optional_metric_value(row.covered_by_any_count),
                    ),
                    (
                        "covered_by_any_ratio",
                        _optional_metric_value(row.covered_by_any_ratio),
                    ),
                    (
                        "independent_occurrence_count",
                        _optional_metric_value(row.independent_occurrence_count),
                    ),
                    (
                        "direct_parent_count",
                        _optional_metric_value(row.direct_parent_count),
                    ),
                    (
                        "dominant_parent_key",
                        _optional_metric_value(row.dominant_parent_key),
                    ),
                    (
                        "dominant_parent_shared_count",
                        _optional_metric_value(row.dominant_parent_shared_count),
                    ),
                    (
                        "dominant_parent_share",
                        _optional_metric_value(row.dominant_parent_share),
                    ),
                    (
                        "dominant_parent_side",
                        _optional_metric_value(row.dominant_parent_side),
                    ),
                ]
            )
        emit_record(fields)


def _emit_result_fields(result: object, field_specs: tuple[FieldSpec, ...]) -> None:
    emit_fields(
        (
            (field_name, _resolve_field_value(result, field_source))
            for field_name, field_source in field_specs
        )
    )


def _resolve_field_value(result: object, field_source: FieldSource) -> object:
    if isinstance(field_source, str):
        return getattr(result, field_source)
    return field_source(result)


def _optional_metric_value(value: object | None) -> object:
    return "-" if value is None else value
