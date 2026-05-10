from __future__ import annotations

import argparse

from podcast_frequency_list.cli.emitters import (
    emit_candidate_metrics_result,
    emit_candidate_metrics_validation,
    emit_candidate_rows,
    emit_candidate_scores_result,
)
from podcast_frequency_list.cli.output import emit_fields, emit_record
from podcast_frequency_list.cli.service_factories import (
    build_candidate_metrics_service,
    build_candidate_scores_service,
)
from podcast_frequency_list.config import load_settings
from podcast_frequency_list.corpus_review import CorpusMilestoneReviewService


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--refresh-first", action="store_true")
    parser.add_argument("--check-determinism", action="store_true")
    parser.add_argument("--validate-metrics", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    settings = load_settings()
    service = CorpusMilestoneReviewService(
        db_path=settings.db_path,
        candidate_metrics_service=build_candidate_metrics_service(),
        candidate_scores_service=build_candidate_scores_service(),
    )
    try:
        result = service.review(
            limit=args.limit,
            refresh_first=args.refresh_first,
            check_determinism=args.check_determinism,
            validate_metrics=args.validate_metrics,
        )
    finally:
        service.close()

    emit_candidate_metrics_result(result.metrics_result)
    if result.metrics_is_deterministic is not None:
        emit_fields((("metrics_is_deterministic", int(result.metrics_is_deterministic)),))
    if result.metrics_validation is not None:
        emit_candidate_metrics_validation(result.metrics_validation)
    emit_candidate_scores_result(result.scores_result)
    score_fields: list[tuple[str, object]] = [
        ("middle_offset", result.middle_offset),
        ("tail_offset", result.tail_offset),
    ]
    if result.scores_is_deterministic is not None:
        score_fields.insert(0, ("scores_is_deterministic", int(result.scores_is_deterministic)))
    emit_fields(tuple(score_fields))
    for row in result.dispersion_rows:
        emit_record(
            (
                ("record", "show_dispersion"),
                ("ngram_size", row.ngram_size),
                ("total_candidates", row.total_candidates),
                ("multi_show_candidates", row.multi_show_candidates),
                ("cross_show_3plus_candidates", row.cross_show_3plus_candidates),
                ("eligible_candidates", row.eligible_candidates),
                ("eligible_multi_show_candidates", row.eligible_multi_show_candidates),
                (
                    "eligible_cross_show_3plus_candidates",
                    row.eligible_cross_show_3plus_candidates,
                ),
                ("max_show_dispersion", row.max_show_dispersion),
            )
        )

    emit_fields((("top_candidate_count_global", len(result.top_rows)),))
    emit_candidate_rows(
        result.top_rows,
        record_type="top_global",
        include_step4=True,
        include_follow_up=True,
        include_step5=True,
        include_step6=True,
    )
    emit_fields((("middle_candidate_count_global", len(result.middle_rows)),))
    emit_candidate_rows(
        result.middle_rows,
        record_type="middle_global",
        include_step4=True,
        include_follow_up=True,
        include_step5=True,
        include_step6=True,
        rank_start=result.middle_offset + 1,
    )
    emit_fields((("tail_candidate_count_global", len(result.tail_rows)),))
    emit_candidate_rows(
        result.tail_rows,
        record_type="tail_global",
        include_step4=True,
        include_follow_up=True,
        include_step5=True,
        include_step6=True,
        rank_start=result.tail_offset + 1,
    )
    emit_fields((("focus_candidate_count", len(result.focus_rows)),))
    emit_candidate_rows(
        result.focus_rows,
        record_type="focus_candidate",
        include_step4=True,
        include_follow_up=True,
        include_step5=True,
        include_step6=True,
    )


if __name__ == "__main__":
    main()
