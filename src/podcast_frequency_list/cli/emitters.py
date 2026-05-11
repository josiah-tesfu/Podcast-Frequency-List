from __future__ import annotations

from podcast_frequency_list.asr import AsrEpisodeResult, AsrRunResult
from podcast_frequency_list.cli.field_specs import (
    ASR_RESULT_FIELDS,
    CANDIDATE_INVENTORY_RESULT_FIELDS,
    CANDIDATE_METRICS_RESULT_FIELDS,
    CANDIDATE_METRICS_VALIDATION_FIELDS,
    CANDIDATE_SCORES_RESULT_FIELDS,
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
from podcast_frequency_list.pilot import CorpusStatusResult, PilotSelectionResult
from podcast_frequency_list.qc import QcRunResult
from podcast_frequency_list.sentences import SentenceSplitResult
from podcast_frequency_list.tokens import (
    CandidateInventoryResult,
    CandidateMetricsResult,
    CandidateMetricsValidationResult,
    CandidateScoresResult,
    CandidateSummaryRow,
    TokenizationResult,
)


def emit_saved_show(saved_show: SavedShow) -> None:
    _emit_result_fields(saved_show, SAVED_SHOW_FIELDS)


def emit_sync_result(result: SyncFeedResult) -> None:
    _emit_result_fields(result, SYNC_RESULT_FIELDS)


def emit_pilot_result(result: PilotSelectionResult) -> None:
    _emit_result_fields(result, PILOT_RESULT_FIELDS)


def emit_corpus_status_result(result: CorpusStatusResult) -> None:
    emit_fields(
        (
            ("show_count", result.show_count),
            ("slice_count", result.slice_count),
            ("episode_count", result.episode_count),
            ("total_hours", f"{result.total_seconds / 3600:.2f}"),
            ("episodes_with_transcript_tag", result.episodes_with_transcript_tag),
            ("selected_slice_episodes", result.selected_slice_episodes),
            ("selected_slice_hours", f"{result.selected_slice_seconds / 3600:.2f}"),
            ("needs_asr_episodes", result.needs_asr_episodes),
            ("in_progress_asr_episodes", result.in_progress_asr_episodes),
            ("ready_asr_episodes", result.ready_asr_episodes),
            ("failed_asr_episodes", result.failed_asr_episodes),
        )
    )
    for row in result.rows:
        emit_record(
            (
                ("record", "show_status"),
                ("show_id", row.show_id),
                ("title", row.title),
                ("feed_url", row.feed_url),
                ("episode_count", row.episode_count),
                ("total_hours", f"{row.total_seconds / 3600:.2f}"),
                ("episodes_with_transcript_tag", row.episodes_with_transcript_tag),
                ("slice_id", row.slice_id if row.slice_id is not None else "-"),
                ("slice_name", row.slice_name or "-"),
                ("slice_selection_order", row.slice_selection_order or "-"),
                ("selected_episodes", row.selected_episodes),
                ("selected_hours", f"{row.selected_seconds / 3600:.2f}"),
                ("needs_asr_episodes", row.needs_asr_episodes),
                ("in_progress_asr_episodes", row.in_progress_asr_episodes),
                ("ready_asr_episodes", row.ready_asr_episodes),
                ("failed_asr_episodes", row.failed_asr_episodes),
            )
        )


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


def emit_candidate_scores_result(result: CandidateScoresResult) -> None:
    _emit_result_fields(result, CANDIDATE_SCORES_RESULT_FIELDS)


def emit_candidate_metrics_validation(result: CandidateMetricsValidationResult) -> None:
    _emit_result_fields(result, CANDIDATE_METRICS_VALIDATION_FIELDS)


def emit_candidate_rows(
    rows: tuple[CandidateSummaryRow, ...],
    *,
    record_type: str,
    include_step4: bool = False,
    include_follow_up: bool = False,
    include_step5: bool = False,
    include_step6: bool = False,
    rank_start: int = 1,
) -> None:
    for rank, row in enumerate(rows, start=rank_start):
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
        if include_follow_up:
            fields.extend(
                [
                    (
                        "punctuation_gap_occurrence_count",
                        _optional_metric_value(row.punctuation_gap_occurrence_count),
                    ),
                    (
                        "punctuation_gap_occurrence_ratio",
                        _optional_metric_value(row.punctuation_gap_occurrence_ratio),
                    ),
                    (
                        "punctuation_gap_edge_clitic_count",
                        _optional_metric_value(row.punctuation_gap_edge_clitic_count),
                    ),
                    (
                        "punctuation_gap_edge_clitic_ratio",
                        _optional_metric_value(row.punctuation_gap_edge_clitic_ratio),
                    ),
                    (
                        "max_component_information",
                        _optional_metric_value(row.max_component_information),
                    ),
                    (
                        "min_component_information",
                        _optional_metric_value(row.min_component_information),
                    ),
                    (
                        "high_information_token_count",
                        _optional_metric_value(row.high_information_token_count),
                    ),
                    (
                        "max_show_share",
                        _optional_metric_value(row.max_show_share),
                    ),
                    (
                        "top2_show_share",
                        _optional_metric_value(row.top2_show_share),
                    ),
                    (
                        "show_entropy",
                        _optional_metric_value(row.show_entropy),
                    ),
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
        if include_step6:
            fields.extend(
                [
                    ("score_version", _optional_metric_value(row.score_version)),
                    ("ranking_lane", _optional_metric_value(row.ranking_lane)),
                    ("passes_support_gate", _optional_metric_value(row.passes_support_gate)),
                    ("passes_quality_gate", _optional_metric_value(row.passes_quality_gate)),
                    ("discard_family", _optional_metric_value(row.discard_family)),
                    ("is_eligible", _optional_metric_value(row.is_eligible)),
                    ("frequency_score", _optional_metric_value(row.frequency_score)),
                    ("dispersion_score", _optional_metric_value(row.dispersion_score)),
                    ("association_score", _optional_metric_value(row.association_score)),
                    ("boundary_score", _optional_metric_value(row.boundary_score)),
                    ("redundancy_penalty", _optional_metric_value(row.redundancy_penalty)),
                    ("final_score", _optional_metric_value(row.final_score)),
                    ("lane_rank", _optional_metric_value(row.lane_rank)),
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
