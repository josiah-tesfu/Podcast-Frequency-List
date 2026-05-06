from __future__ import annotations

from dataclasses import dataclass

SCORE_VERSION = "pilot-v1"
REDUNDANCY_THRESHOLD = 0.80
DEFAULT_SUMMARY_LIMIT = 20


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
