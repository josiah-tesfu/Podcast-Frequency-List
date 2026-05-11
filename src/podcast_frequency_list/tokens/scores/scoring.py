from __future__ import annotations

import math

from podcast_frequency_list.tokens.scores.errors import CandidateScoresError
from podcast_frequency_list.tokens.scores.policy import (
    _LANE_SPECS,
    ASSOCIATION_KEEP_THRESHOLD,
    BOUNDARY_KEEP_ASSOCIATION_FLOOR,
    BOUNDARY_KEEP_THRESHOLD,
    DISCARD_FAMILY_EDGE_CLITIC,
    DISCARD_FAMILY_SHOW_SPECIFICITY,
    DISCARD_FAMILY_SUPPORT,
    DISCARD_FAMILY_WEAK_MULTIWORD,
    LEXICAL_KEEP_THRESHOLD,
    LEXICAL_ONLY_ASSOCIATION_FLOOR,
    ONE_GRAM_SPECIFICITY_MAX_PENALTY,
    ONE_GRAM_SPECIFICITY_MAX_SHOW_SHARE,
    ONE_GRAM_SPECIFICITY_SHOW_DISPERSION,
    PUNCTUATION_GAP_REJECT_THRESHOLD,
    REDUNDANCY_THRESHOLD,
    SPECIFICITY_HARD_MAX_SHOW_SHARE,
    SPECIFICITY_HARD_SHOW_DISPERSION,
    SPECIFICITY_HARD_TOP2_SHOW_SHARE,
    SPECIFICITY_MAX_PENALTY,
    SPECIFICITY_SINGLE_SHOW_REJECT_DISPERSION,
    SPECIFICITY_SOFT_MAX_SHOW_SHARE,
    SPECIFICITY_SOFT_SHOW_DISPERSION,
    SPECIFICITY_SOFT_TOP2_SHOW_SHARE,
    TWO_GRAM_LEXICAL_ASSOCIATION_FLOOR,
    TWO_GRAM_LEXICAL_ENTROPY_FLOOR,
    TWO_GRAM_LEXICAL_PARENT_SHARE_CEILING,
)
from podcast_frequency_list.tokens.scores.types import _CandidateScoreInput, _CandidateScoreRow


def _build_scored_rows(
    *,
    inventory_version: str,
    score_version: str,
    candidate_inputs: tuple[_CandidateScoreInput, ...],
) -> tuple[_CandidateScoreRow, ...]:
    _validate_multiword_metrics(candidate_inputs)

    quality_gate_by_id = {
        candidate.candidate_id: _evaluate_quality_gate(candidate)
        for candidate in candidate_inputs
    }
    eligible_by_lane = {
        lane_spec.ranking_lane: [
            candidate
            for candidate in candidate_inputs
            if (
                candidate.ranking_lane == lane_spec.ranking_lane
                and candidate.passes_support_gate
                and quality_gate_by_id[candidate.candidate_id][0]
            )
        ]
        for lane_spec in _LANE_SPECS.values()
    }

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

        passes_quality_gate, discard_family = quality_gate_by_id[candidate.candidate_id]
        rows.append(
            _CandidateScoreRow(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_id=candidate.candidate_id,
                ranking_lane=candidate.ranking_lane,
                passes_support_gate=int(candidate.passes_support_gate),
                passes_quality_gate=int(passes_quality_gate),
                discard_family=(
                    DISCARD_FAMILY_SUPPORT
                    if not candidate.passes_support_gate
                    else discard_family
                ),
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


def _validate_multiword_metrics(
    candidates: tuple[_CandidateScoreInput, ...],
) -> None:
    for candidate in candidates:
        if candidate.ngram_size == 1 or not candidate.passes_support_gate:
            continue
        lane_name = candidate.ranking_lane
        if candidate.t_score is None or candidate.npmi is None:
            raise CandidateScoresError(
                f"{lane_name} candidate {candidate.candidate_key!r} is missing association metrics"
            )
        if candidate.left_entropy is None or candidate.right_entropy is None:
            raise CandidateScoresError(
                f"{lane_name} candidate {candidate.candidate_key!r} is missing boundary metrics"
            )
        if (
            candidate.punctuation_gap_occurrence_ratio is None
            or candidate.punctuation_gap_edge_clitic_ratio is None
            or candidate.max_component_information is None
            or candidate.max_show_share is None
            or candidate.top2_show_share is None
        ):
            raise CandidateScoresError(
                f"{lane_name} candidate {candidate.candidate_key!r} "
                "is missing unit identity or specificity metrics"
            )


def _evaluate_quality_gate(candidate: _CandidateScoreInput) -> tuple[bool, str | None]:
    if not candidate.passes_support_gate:
        return False, None

    if candidate.ngram_size == 1:
        return True, None

    if (candidate.punctuation_gap_edge_clitic_ratio or 0.0) > 0.0:
        return False, DISCARD_FAMILY_EDGE_CLITIC

    if (candidate.punctuation_gap_occurrence_ratio or 0.0) >= PUNCTUATION_GAP_REJECT_THRESHOLD:
        return False, DISCARD_FAMILY_EDGE_CLITIC

    npmi = float(candidate.npmi)
    min_entropy = min(float(candidate.left_entropy), float(candidate.right_entropy))
    if _is_hard_show_specificity_reject(
        candidate,
        npmi=npmi,
        min_entropy=min_entropy,
    ):
        return False, DISCARD_FAMILY_SHOW_SPECIFICITY

    max_component_information = float(candidate.max_component_information)
    passes_association_keep = npmi >= ASSOCIATION_KEEP_THRESHOLD
    passes_boundary_keep = (
        min_entropy >= BOUNDARY_KEEP_THRESHOLD and npmi >= BOUNDARY_KEEP_ASSOCIATION_FLOOR
    )
    passes_lexical_keep = max_component_information >= LEXICAL_KEEP_THRESHOLD
    if passes_lexical_keep and not (passes_association_keep or passes_boundary_keep):
        if candidate.ngram_size == 2:
            passes_lexical_keep = npmi >= TWO_GRAM_LEXICAL_ASSOCIATION_FLOOR and (
                min_entropy >= TWO_GRAM_LEXICAL_ENTROPY_FLOOR
                or candidate.dominant_parent_share is None
                or candidate.dominant_parent_share < TWO_GRAM_LEXICAL_PARENT_SHARE_CEILING
            )
        else:
            passes_lexical_keep = npmi >= LEXICAL_ONLY_ASSOCIATION_FLOOR

    if passes_association_keep or passes_boundary_keep or passes_lexical_keep:
        return True, None
    return False, DISCARD_FAMILY_WEAK_MULTIWORD


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
                passes_support_gate=1,
                passes_quality_gate=1,
                discard_family=None,
                is_eligible=1,
                frequency_score=frequency_scores[candidate.candidate_id],
                dispersion_score=dispersion_scores[candidate.candidate_id],
                association_score=None,
                boundary_score=None,
                redundancy_penalty=0.0,
                final_score=(
                    0.65 * frequency_scores[candidate.candidate_id]
                    + 0.35 * dispersion_scores[candidate.candidate_id]
                    - _specificity_penalty(candidate)
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
            specificity_penalty = _specificity_penalty(candidate)
            final_score = (
                0.20 * support_score
                + 0.50 * association_score
                + 0.20 * boundary_score
                - 0.10 * redundancy_penalty
                - specificity_penalty
            )
        else:
            support_score = 0.55 * frequency_score + 0.45 * dispersion_score
            redundancy_penalty = 0.0
            specificity_penalty = _specificity_penalty(candidate)
            final_score = (
                0.20 * support_score
                + 0.55 * association_score
                + 0.25 * boundary_score
                - specificity_penalty
            )

        scored_rows.append(
            _CandidateScoreRow(
                inventory_version=inventory_version,
                score_version=score_version,
                candidate_id=candidate.candidate_id,
                ranking_lane=lane_name,
                passes_support_gate=1,
                passes_quality_gate=1,
                discard_family=None,
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


def _is_hard_show_specificity_reject(
    candidate: _CandidateScoreInput,
    *,
    npmi: float,
    min_entropy: float,
) -> bool:
    if candidate.ngram_size == 1:
        return False
    if candidate.show_dispersion <= SPECIFICITY_SINGLE_SHOW_REJECT_DISPERSION:
        return True
    if candidate.show_dispersion > SPECIFICITY_HARD_SHOW_DISPERSION:
        return False
    if float(candidate.max_show_share or 0.0) < SPECIFICITY_HARD_MAX_SHOW_SHARE:
        return False
    if float(candidate.top2_show_share or 0.0) < SPECIFICITY_HARD_TOP2_SHOW_SHARE:
        return False
    return npmi < ASSOCIATION_KEEP_THRESHOLD and min_entropy < BOUNDARY_KEEP_THRESHOLD


def _specificity_penalty(candidate: _CandidateScoreInput) -> float:
    if candidate.max_show_share is None:
        return 0.0

    if candidate.ngram_size == 1:
        return _one_gram_specificity_penalty(candidate)

    if (
        candidate.show_dispersion > SPECIFICITY_SOFT_SHOW_DISPERSION
        or candidate.max_show_share < SPECIFICITY_SOFT_MAX_SHOW_SHARE
    ):
        return 0.0

    spread_severity = _clamp01(
        (SPECIFICITY_SOFT_SHOW_DISPERSION - candidate.show_dispersion)
        / (SPECIFICITY_SOFT_SHOW_DISPERSION - 1)
    )
    concentration_severity = _clamp01(
        (candidate.max_show_share - SPECIFICITY_SOFT_MAX_SHOW_SHARE)
        / (1.0 - SPECIFICITY_SOFT_MAX_SHOW_SHARE)
    )
    top2_show_share = float(candidate.top2_show_share or 0.0)
    top2_severity = _clamp01(
        (top2_show_share - SPECIFICITY_SOFT_TOP2_SHOW_SHARE)
        / (1.0 - SPECIFICITY_SOFT_TOP2_SHOW_SHARE)
    )
    specificity_severity = (spread_severity + concentration_severity + top2_severity) / 3.0
    if specificity_severity <= 0.0:
        return 0.0

    rescue_points = 0
    if candidate.npmi is not None and candidate.npmi >= ASSOCIATION_KEEP_THRESHOLD:
        rescue_points += 1
    if (
        candidate.left_entropy is not None
        and candidate.right_entropy is not None
        and min(candidate.left_entropy, candidate.right_entropy) >= BOUNDARY_KEEP_THRESHOLD
    ):
        rescue_points += 1
    if (
        candidate.max_component_information is not None
        and candidate.max_component_information >= LEXICAL_KEEP_THRESHOLD
    ):
        rescue_points += 1

    rescue_multiplier = 1.0 if rescue_points == 0 else 0.65 if rescue_points == 1 else 0.35
    return SPECIFICITY_MAX_PENALTY * specificity_severity * rescue_multiplier


def _one_gram_specificity_penalty(candidate: _CandidateScoreInput) -> float:
    if (
        candidate.show_dispersion > ONE_GRAM_SPECIFICITY_SHOW_DISPERSION
        or candidate.max_show_share < ONE_GRAM_SPECIFICITY_MAX_SHOW_SHARE
    ):
        return 0.0

    spread_severity = _clamp01(
        (ONE_GRAM_SPECIFICITY_SHOW_DISPERSION - candidate.show_dispersion)
        / (ONE_GRAM_SPECIFICITY_SHOW_DISPERSION - 1)
    )
    concentration_severity = _clamp01(
        (candidate.max_show_share - ONE_GRAM_SPECIFICITY_MAX_SHOW_SHARE)
        / (1.0 - ONE_GRAM_SPECIFICITY_MAX_SHOW_SHARE)
    )
    return ONE_GRAM_SPECIFICITY_MAX_PENALTY * (
        (spread_severity + concentration_severity) / 2.0
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
            passes_support_gate=row.passes_support_gate,
            passes_quality_gate=row.passes_quality_gate,
            discard_family=row.discard_family,
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


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))
