from __future__ import annotations

import math

from podcast_frequency_list.tokens.scores.errors import CandidateScoresError
from podcast_frequency_list.tokens.scores.policy import (
    _LANE_SPECS,
    REDUNDANCY_THRESHOLD,
)
from podcast_frequency_list.tokens.scores.types import _CandidateScoreInput, _CandidateScoreRow


def _build_scored_rows(
    *,
    inventory_version: str,
    score_version: str,
    candidate_inputs: tuple[_CandidateScoreInput, ...],
) -> tuple[_CandidateScoreRow, ...]:
    eligible_by_lane = {
        lane_spec.ranking_lane: [
            candidate
            for candidate in candidate_inputs
            if candidate.ranking_lane == lane_spec.ranking_lane and candidate.is_eligible
        ]
        for lane_spec in _LANE_SPECS.values()
    }

    _validate_lane_metrics(eligible_by_lane["2gram"], lane_name="2gram")
    _validate_lane_metrics(eligible_by_lane["3gram"], lane_name="3gram")

    scored_by_id: dict[int, _CandidateScoreRow] = {}
    for lane_name, eligible_candidates in eligible_by_lane.items():
        lane_rows = _score_lane(
            inventory_version=inventory_version,
            score_version=score_version,
            lane_name=lane_name,
            eligible_candidates=eligible_candidates,
        )
        scored_by_id.update({row.candidate_id: row for row in lane_rows})

    rows: list[_CandidateScoreRow] = []
    for candidate in candidate_inputs:
        scored_row = scored_by_id.get(candidate.candidate_id)
        if scored_row is not None:
            rows.append(scored_row)
            continue

        rows.append(
            _CandidateScoreRow(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_id=candidate.candidate_id,
                ranking_lane=candidate.ranking_lane,
                is_eligible=0,
                frequency_score=None,
                dispersion_score=None,
                association_score=None,
                boundary_score=None,
                redundancy_penalty=None,
                final_score=None,
                lane_rank=None,
            )
        )

    return tuple(rows)


def _validate_lane_metrics(
    candidates: list[_CandidateScoreInput],
    *,
    lane_name: str,
) -> None:
    for candidate in candidates:
        if candidate.t_score is None or candidate.npmi is None:
            raise CandidateScoresError(
                f"{lane_name} candidate {candidate.candidate_key!r} is missing association metrics"
            )
        if candidate.left_entropy is None or candidate.right_entropy is None:
            raise CandidateScoresError(
                f"{lane_name} candidate {candidate.candidate_key!r} is missing boundary metrics"
            )


def _score_lane(
    *,
    inventory_version: str,
    score_version: str,
    lane_name: str,
    eligible_candidates: list[_CandidateScoreInput],
) -> tuple[_CandidateScoreRow, ...]:
    if not eligible_candidates:
        return ()

    frequency_scores = _min_max_normalize(
        {
            candidate.candidate_id: math.log1p(candidate.raw_frequency)
            for candidate in eligible_candidates
        }
    )
    dispersion_scores = _min_max_normalize(
        {
            candidate.candidate_id: float(candidate.episode_dispersion)
            for candidate in eligible_candidates
        }
    )

    if lane_name == "1gram":
        scored_rows = [
            _CandidateScoreRow(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_id=candidate.candidate_id,
                ranking_lane=lane_name,
                is_eligible=1,
                frequency_score=frequency_scores[candidate.candidate_id],
                dispersion_score=dispersion_scores[candidate.candidate_id],
                association_score=None,
                boundary_score=None,
                redundancy_penalty=0.0,
                final_score=(
                    0.65 * frequency_scores[candidate.candidate_id]
                    + 0.35 * dispersion_scores[candidate.candidate_id]
                ),
                lane_rank=None,
            )
            for candidate in eligible_candidates
        ]
        return _assign_lane_ranks(scored_rows, eligible_candidates)

    association_scores = _association_scores(eligible_candidates)
    boundary_scores = _boundary_scores(eligible_candidates)

    scored_rows = []
    for candidate in eligible_candidates:
        frequency_score = frequency_scores[candidate.candidate_id]
        dispersion_score = dispersion_scores[candidate.candidate_id]
        association_score = association_scores[candidate.candidate_id]
        boundary_score = boundary_scores[candidate.candidate_id]

        if lane_name == "2gram":
            support_score = 0.60 * frequency_score + 0.40 * dispersion_score
            redundancy_penalty = _redundancy_penalty(candidate.dominant_parent_share)
            final_score = (
                0.20 * support_score
                + 0.50 * association_score
                + 0.20 * boundary_score
                - 0.10 * redundancy_penalty
            )
        else:
            support_score = 0.55 * frequency_score + 0.45 * dispersion_score
            redundancy_penalty = 0.0
            final_score = (
                0.20 * support_score
                + 0.55 * association_score
                + 0.25 * boundary_score
            )

        scored_rows.append(
            _CandidateScoreRow(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_id=candidate.candidate_id,
                ranking_lane=lane_name,
                is_eligible=1,
                frequency_score=frequency_score,
                dispersion_score=dispersion_score,
                association_score=association_score,
                boundary_score=boundary_score,
                redundancy_penalty=redundancy_penalty,
                final_score=final_score,
                lane_rank=None,
            )
        )

    return _assign_lane_ranks(scored_rows, eligible_candidates)


def _association_scores(
    candidates: list[_CandidateScoreInput],
) -> dict[int, float]:
    t_score_norms = _min_max_normalize(
        {
            candidate.candidate_id: float(candidate.t_score)
            for candidate in candidates
            if candidate.t_score is not None
        }
    )
    npmi_norms = _min_max_normalize(
        {
            candidate.candidate_id: float(candidate.npmi)
            for candidate in candidates
            if candidate.npmi is not None
        }
    )
    return {
        candidate.candidate_id: (
            0.65 * npmi_norms[candidate.candidate_id]
            + 0.35 * t_score_norms[candidate.candidate_id]
        )
        for candidate in candidates
    }


def _boundary_scores(
    candidates: list[_CandidateScoreInput],
) -> dict[int, float]:
    weaker_side_norms = _min_max_normalize(
        {
            candidate.candidate_id: min(
                float(candidate.left_entropy),
                float(candidate.right_entropy),
            )
            for candidate in candidates
            if candidate.left_entropy is not None and candidate.right_entropy is not None
        }
    )
    return {
        candidate.candidate_id: weaker_side_norms[candidate.candidate_id]
        for candidate in candidates
    }


def _redundancy_penalty(dominant_parent_share: float | None) -> float:
    if dominant_parent_share is None or dominant_parent_share < REDUNDANCY_THRESHOLD:
        return 0.0
    return min(
        1.0,
        (dominant_parent_share - REDUNDANCY_THRESHOLD) / (1.0 - REDUNDANCY_THRESHOLD),
    )


def _assign_lane_ranks(
    scored_rows: list[_CandidateScoreRow],
    candidates: list[_CandidateScoreInput],
) -> tuple[_CandidateScoreRow, ...]:
    candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    ordered_rows = sorted(
        scored_rows,
        key=lambda row: (
            -float(row.final_score),
            -candidate_by_id[row.candidate_id].raw_frequency,
            -candidate_by_id[row.candidate_id].episode_dispersion,
            candidate_by_id[row.candidate_id].candidate_key,
        ),
    )
    return tuple(
        _CandidateScoreRow(
            inventory_version=row.inventory_version,
            score_version=row.score_version,
            candidate_id=row.candidate_id,
            ranking_lane=row.ranking_lane,
            is_eligible=row.is_eligible,
            frequency_score=row.frequency_score,
            dispersion_score=row.dispersion_score,
            association_score=row.association_score,
            boundary_score=row.boundary_score,
            redundancy_penalty=row.redundancy_penalty,
            final_score=row.final_score,
            lane_rank=rank,
        )
        for rank, row in enumerate(ordered_rows, start=1)
    )


def _min_max_normalize(values: dict[int, float]) -> dict[int, float]:
    if not values:
        return {}

    min_value = min(values.values())
    max_value = max(values.values())
    if max_value == min_value:
        return {candidate_id: 1.0 for candidate_id in values}

    scale = max_value - min_value
    return {
        candidate_id: (value - min_value) / scale
        for candidate_id, value in values.items()
    }
