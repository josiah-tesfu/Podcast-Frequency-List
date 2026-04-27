from __future__ import annotations

from collections.abc import Callable

FieldSource = str | Callable[[object], object]
FieldSpec = tuple[str, FieldSource]

SAVED_SHOW_FIELDS: tuple[FieldSpec, ...] = (
    ("saved_show_id", "show_id"),
    ("title", "title"),
    ("feed_url", "feed_url"),
)
SYNC_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("show_id", "show_id"),
    ("title", "title"),
    ("episodes_seen", "episodes_seen"),
    ("episodes_inserted", "episodes_inserted"),
    ("episodes_updated", "episodes_updated"),
    ("episodes_skipped_no_audio", "episodes_skipped_no_audio"),
    ("episodes_with_transcript_tag", "episodes_with_transcript_tag"),
)
PILOT_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("pilot_run_id", "pilot_run_id"),
    ("name", "name"),
    ("show_id", "show_id"),
    ("title", "show_title"),
    ("target_hours", lambda result: f"{result.target_seconds / 3600:.2f}"),
    ("selected_hours", lambda result: f"{result.total_seconds / 3600:.2f}"),
    ("selected_episodes", "selected_count"),
    ("skipped_ineligible_episodes", "skipped_count"),
    ("selection_order", "selection_order"),
    ("model", "model"),
    ("estimated_asr_cost_usd", lambda result: f"{result.estimated_cost_usd:.2f}"),
    ("first_selected_published_at", lambda result: result.first_published_at or "-"),
    ("last_selected_published_at", lambda result: result.last_published_at or "-"),
    ("status", lambda _result: "needs_asr"),
)
ASR_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("pilot", "pilot_name"),
    ("model", "model"),
    ("requested_limit", lambda result: result.requested_limit or "-"),
    ("selected_episodes", "selected_count"),
    ("completed_episodes", "completed_count"),
    ("skipped_episodes", "skipped_count"),
    ("failed_episodes", "failed_count"),
    ("chunks_transcribed", "chunk_count"),
)
NORMALIZATION_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("scope", "scope"),
    ("scope_value", "scope_value"),
    ("normalization_version", "normalization_version"),
    ("selected_segments", "selected_segments"),
    ("normalized_segments", "normalized_segments"),
    ("skipped_segments", "skipped_segments"),
    ("episodes_touched", "episode_count"),
)
QC_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("scope", "scope"),
    ("scope_value", "scope_value"),
    ("qc_version", "qc_version"),
    ("selected_segments", "selected_segments"),
    ("processed_segments", "processed_segments"),
    ("skipped_segments", "skipped_segments"),
    ("keep_segments", "keep_segments"),
    ("review_segments", "review_segments"),
    ("remove_segments", "remove_segments"),
)
SENTENCE_SPLIT_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("scope", "scope"),
    ("scope_value", "scope_value"),
    ("split_version", "split_version"),
    ("selected_segments", "selected_segments"),
    ("created_sentences", "created_sentences"),
    ("skipped_segments", "skipped_segments"),
    ("episodes_touched", "episode_count"),
)
TOKENIZATION_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("scope", "scope"),
    ("scope_value", "scope_value"),
    ("tokenization_version", "tokenization_version"),
    ("selected_sentences", "selected_sentences"),
    ("tokenized_sentences", "tokenized_sentences"),
    ("created_tokens", "created_tokens"),
    ("skipped_sentences", "skipped_sentences"),
    ("episodes_touched", "episode_count"),
)
CANDIDATE_INVENTORY_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("scope", "scope"),
    ("scope_value", "scope_value"),
    ("inventory_version", "inventory_version"),
    ("selected_sentences", "selected_sentences"),
    ("processed_sentences", "processed_sentences"),
    ("skipped_sentences", "skipped_sentences"),
    ("created_candidates", "created_candidates"),
    ("created_occurrences", "created_occurrences"),
    ("episodes_touched", "episode_count"),
)
CANDIDATE_METRICS_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("inventory_version", "inventory_version"),
    ("selected_candidates", "selected_candidates"),
    ("refreshed_candidates", "refreshed_candidates"),
    ("deleted_orphan_candidates", "deleted_orphan_candidates"),
    ("occurrence_count", "occurrence_count"),
    ("raw_frequency_total", "raw_frequency_total"),
    ("episode_dispersion_total", "episode_dispersion_total"),
    ("show_dispersion_total", "show_dispersion_total"),
    ("display_text_updates", "display_text_updates"),
)
CANDIDATE_SCORES_RESULT_FIELDS: tuple[FieldSpec, ...] = (
    ("inventory_version", "inventory_version"),
    ("score_version", "score_version"),
    ("selected_candidates", "selected_candidates"),
    ("stored_candidates", "stored_candidates"),
    ("eligible_candidates", "eligible_candidates"),
    ("eligible_1gram_candidates", "eligible_1gram_candidates"),
    ("eligible_2gram_candidates", "eligible_2gram_candidates"),
    ("eligible_3gram_candidates", "eligible_3gram_candidates"),
)
CANDIDATE_METRICS_VALIDATION_FIELDS: tuple[FieldSpec, ...] = (
    ("inventory_version", "inventory_version"),
    ("candidate_count", "candidate_count"),
    ("occurrence_count", "occurrence_count"),
    ("raw_frequency_mismatch_count", "raw_frequency_mismatch_count"),
    ("episode_dispersion_mismatch_count", "episode_dispersion_mismatch_count"),
    ("show_dispersion_mismatch_count", "show_dispersion_mismatch_count"),
    ("display_text_mismatch_count", "display_text_mismatch_count"),
    ("foreign_key_issue_count", "foreign_key_issue_count"),
)
