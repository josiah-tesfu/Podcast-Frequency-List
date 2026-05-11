from __future__ import annotations

from sqlite3 import Connection

from podcast_frequency_list.tokens.models import CandidateScoresResult
from podcast_frequency_list.tokens.scores.errors import CandidateScoresError
from podcast_frequency_list.tokens.scores.policy import _LANE_SPECS
from podcast_frequency_list.tokens.scores.scoring import _build_scored_rows
from podcast_frequency_list.tokens.scores.types import _CandidateScoreInput


class _CandidateScoresWorkflow:
    def __init__(
        self,
        *,
        connection: Connection,
        inventory_version: str,
        score_version: str,
    ) -> None:
        self.connection = connection
        self.inventory_version = inventory_version
        self.score_version = score_version

    def count_candidates(self) -> int:
        return int(
            self.connection.execute(
                """
                SELECT COUNT(*)
                FROM token_candidates
                WHERE inventory_version = ?
                """,
                (self.inventory_version,),
            ).fetchone()[0]
        )

    def summarize(self, *, selected_candidates: int) -> CandidateScoresResult:
        row = self.connection.execute(
            """
            SELECT
                COUNT(*) AS stored_candidates,
                COALESCE(SUM(passes_support_gate), 0) AS support_pass_candidates,
                COALESCE(SUM(passes_quality_gate), 0) AS quality_pass_candidates,
                COALESCE(SUM(is_eligible), 0) AS eligible_candidates,
                COALESCE(
                    SUM(
                        CASE
                            WHEN ranking_lane = '1gram' AND is_eligible = 1 THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS eligible_1gram_candidates,
                COALESCE(
                    SUM(
                        CASE
                            WHEN ranking_lane = '2gram' AND is_eligible = 1 THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS eligible_2gram_candidates,
                COALESCE(
                    SUM(
                        CASE
                            WHEN ranking_lane = '3gram' AND is_eligible = 1 THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS eligible_3gram_candidates
            FROM candidate_scores
            WHERE inventory_version = ?
            AND score_version = ?
            """,
            (self.inventory_version, self.score_version),
        ).fetchone()
        return CandidateScoresResult(
            inventory_version=self.inventory_version,
            score_version=self.score_version,
            selected_candidates=selected_candidates,
            stored_candidates=int(row["stored_candidates"]),
            support_pass_candidates=int(row["support_pass_candidates"]),
            quality_pass_candidates=int(row["quality_pass_candidates"]),
            eligible_candidates=int(row["eligible_candidates"]),
            eligible_1gram_candidates=int(row["eligible_1gram_candidates"]),
            eligible_2gram_candidates=int(row["eligible_2gram_candidates"]),
            eligible_3gram_candidates=int(row["eligible_3gram_candidates"]),
        )

    def refresh(self, *, selected_candidates: int) -> CandidateScoresResult:
        candidate_inputs = _load_candidate_inputs(
            self.connection,
            inventory_version=self.inventory_version,
        )
        scored_rows = _build_scored_rows(
            inventory_version=self.inventory_version,
            score_version=self.score_version,
            candidate_inputs=candidate_inputs,
        )

        self.connection.execute(
            """
            DELETE FROM candidate_scores
            WHERE inventory_version = ?
            AND score_version = ?
            """,
            (self.inventory_version, self.score_version),
        )
        self.connection.executemany(
            """
            INSERT INTO candidate_scores (
                inventory_version,
                score_version,
                candidate_id,
                ranking_lane,
                passes_support_gate,
                passes_quality_gate,
                discard_family,
                is_eligible,
                frequency_score,
                dispersion_score,
                association_score,
                boundary_score,
                redundancy_penalty,
                final_score,
                lane_rank
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.inventory_version,
                    row.score_version,
                    row.candidate_id,
                    row.ranking_lane,
                    row.passes_support_gate,
                    row.passes_quality_gate,
                    row.discard_family,
                    row.is_eligible,
                    row.frequency_score,
                    row.dispersion_score,
                    row.association_score,
                    row.boundary_score,
                    row.redundancy_penalty,
                    row.final_score,
                    row.lane_rank,
                )
                for row in scored_rows
            ],
        )
        return self.summarize(selected_candidates=selected_candidates)


def _load_candidate_inputs(
    connection: Connection,
    *,
    inventory_version: str,
) -> tuple[_CandidateScoreInput, ...]:
    rows = connection.execute(
        """
        WITH ranked_containment AS (
            SELECT
                cc.smaller_candidate_id AS candidate_id,
                cc.shared_occurrence_count AS dominant_parent_shared_count,
                ROW_NUMBER() OVER (
                    PARTITION BY cc.smaller_candidate_id
                    ORDER BY
                        cc.shared_occurrence_count DESC,
                        larger.raw_frequency DESC,
                        larger.candidate_key
                ) AS parent_rank
            FROM candidate_containment cc
            JOIN token_candidates larger
                ON larger.candidate_id = cc.larger_candidate_id
                AND larger.inventory_version = cc.inventory_version
            WHERE cc.inventory_version = ?
        ),
        dominant_parent AS (
            SELECT candidate_id, dominant_parent_shared_count
            FROM ranked_containment
            WHERE parent_rank = 1
        )
        SELECT
            cand.candidate_id,
            cand.candidate_key,
            cand.ngram_size,
            cand.raw_frequency,
            cand.episode_dispersion,
            cand.show_dispersion,
            cand.t_score,
            cand.npmi,
            cand.left_entropy,
            cand.right_entropy,
            cand.punctuation_gap_occurrence_ratio,
            cand.punctuation_gap_edge_clitic_ratio,
            cand.starts_with_standalone_clitic,
            cand.ends_with_standalone_clitic,
            cand.max_component_information,
            cand.max_show_share,
            cand.top2_show_share,
            cand.show_entropy,
            CASE
                WHEN dominant_parent.dominant_parent_shared_count IS NOT NULL
                AND cand.raw_frequency > 0
                THEN 1.0 * dominant_parent.dominant_parent_shared_count / cand.raw_frequency
            END AS dominant_parent_share
        FROM token_candidates cand
        LEFT JOIN dominant_parent
            ON dominant_parent.candidate_id = cand.candidate_id
        WHERE cand.inventory_version = ?
        ORDER BY cand.candidate_id
        """,
        (inventory_version, inventory_version),
    ).fetchall()

    candidate_inputs: list[_CandidateScoreInput] = []
    unsupported_sizes: list[int] = []
    for row in rows:
        ngram_size = int(row["ngram_size"])
        lane_spec = _LANE_SPECS.get(ngram_size)
        if lane_spec is None:
            unsupported_sizes.append(ngram_size)
            continue

        raw_frequency = int(row["raw_frequency"])
        episode_dispersion = int(row["episode_dispersion"])
        show_dispersion = int(row["show_dispersion"])
        candidate_inputs.append(
            _CandidateScoreInput(
                candidate_id=int(row["candidate_id"]),
                candidate_key=str(row["candidate_key"]),
                ngram_size=ngram_size,
                raw_frequency=raw_frequency,
                episode_dispersion=episode_dispersion,
                show_dispersion=show_dispersion,
                t_score=_optional_float(row["t_score"]),
                npmi=_optional_float(row["npmi"]),
                left_entropy=_optional_float(row["left_entropy"]),
                right_entropy=_optional_float(row["right_entropy"]),
                punctuation_gap_occurrence_ratio=_optional_float(
                    row["punctuation_gap_occurrence_ratio"]
                ),
                punctuation_gap_edge_clitic_ratio=_optional_float(
                    row["punctuation_gap_edge_clitic_ratio"]
                ),
                starts_with_standalone_clitic=_optional_int(
                    row["starts_with_standalone_clitic"]
                ),
                ends_with_standalone_clitic=_optional_int(
                    row["ends_with_standalone_clitic"]
                ),
                max_component_information=_optional_float(row["max_component_information"]),
                max_show_share=_optional_float(row["max_show_share"]),
                top2_show_share=_optional_float(row["top2_show_share"]),
                show_entropy=_optional_float(row["show_entropy"]),
                dominant_parent_share=_optional_float(row["dominant_parent_share"]),
                ranking_lane=lane_spec.ranking_lane,
                passes_support_gate=(
                    raw_frequency >= lane_spec.min_raw_frequency
                    and episode_dispersion >= lane_spec.min_episode_dispersion
                ),
            )
        )

    if unsupported_sizes:
        raise CandidateScoresError(
            "unsupported ngram sizes found for scoring lanes: "
            + ", ".join(str(size) for size in sorted(set(unsupported_sizes)))
        )

    return tuple(candidate_inputs)


def _optional_float(value: object | None) -> float | None:
    return None if value is None else float(value)


def _optional_int(value: object | None) -> int | None:
    return None if value is None else int(value)
