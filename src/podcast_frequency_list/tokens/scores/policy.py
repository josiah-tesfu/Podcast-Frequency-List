from __future__ import annotations

from dataclasses import dataclass

SCORE_VERSION = "pilot-v2"
REDUNDANCY_THRESHOLD = 0.80
DEFAULT_SUMMARY_LIMIT = 20
MIN_SHOW_DISPERSION = 8
PUNCTUATION_GAP_REJECT_THRESHOLD = 0.75
ASSOCIATION_KEEP_THRESHOLD = 0.30
BOUNDARY_KEEP_THRESHOLD = 1.50
BOUNDARY_KEEP_ASSOCIATION_FLOOR = 0.10
LEXICAL_KEEP_THRESHOLD = 6.0
LEXICAL_ONLY_ASSOCIATION_FLOOR = 0.20
DISCARD_FAMILY_SUPPORT = "support_floor"
DISCARD_FAMILY_EDGE_CLITIC = "edge_clitic_gap"
DISCARD_FAMILY_WEAK_MULTIWORD = "weak_multiword"


@dataclass(frozen=True)
class _LaneSpec:
    ranking_lane: str
    min_raw_frequency: int
    min_episode_dispersion: int


_LANE_SPECS: dict[int, _LaneSpec] = {
    1: _LaneSpec("1gram", 20, 5),
    2: _LaneSpec("2gram", 10, 3),
    3: _LaneSpec("3gram", 10, 3),
}
