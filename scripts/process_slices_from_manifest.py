from __future__ import annotations

from podcast_frequency_list.cli.output import emit_fields, emit_record
from podcast_frequency_list.cli.service_factories import (
    build_asr_run_service,
    build_candidate_inventory_service,
    build_segment_qc_service,
    build_sentence_split_service,
    build_sentence_tokenization_service,
    build_transcript_normalization_service,
)
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.show_processing import ShowProcessingService


def main() -> None:
    settings = load_settings()
    service = ShowProcessingService(
        db_path=settings.db_path,
        asr_run_service=build_asr_run_service(),
        transcript_normalization_service=build_transcript_normalization_service(),
        segment_qc_service=build_segment_qc_service(),
        sentence_split_service=build_sentence_split_service(),
        sentence_tokenization_service=build_sentence_tokenization_service(),
        candidate_inventory_service=build_candidate_inventory_service(),
    )
    try:
        result = service.process_manifest()
    finally:
        service.close()

    emit_fields(
        (
            ("selected_shows", result.selected_shows),
            ("processed_slices", result.processed_slices),
            ("selected_episodes", result.selected_episodes),
            ("asr_selected_episodes", result.asr_selected_episodes),
            ("asr_completed_episodes", result.asr_completed_episodes),
            ("asr_failed_episodes", result.asr_failed_episodes),
            ("normalized_segments", result.normalized_segments),
            ("qc_processed_segments", result.qc_processed_segments),
            ("created_sentences", result.created_sentences),
            ("created_tokens", result.created_tokens),
            ("created_occurrences", result.created_occurrences),
        )
    )
    for row in result.rows:
        emit_record(
            (
                ("record", "show_processing"),
                ("slug", row.slug),
                ("show_id", row.show_id),
                ("slice_id", row.slice_id),
                ("slice_name", row.slice_name),
                ("selected_episodes", row.selected_episodes),
                ("asr_selected_episodes", row.asr_selected_episodes),
                ("asr_completed_episodes", row.asr_completed_episodes),
                ("asr_failed_episodes", row.asr_failed_episodes),
                ("normalized_segments", row.normalized_segments),
                ("qc_processed_segments", row.qc_processed_segments),
                ("created_sentences", row.created_sentences),
                ("created_tokens", row.created_tokens),
                ("created_occurrences", row.created_occurrences),
            )
        )


if __name__ == "__main__":
    main()
