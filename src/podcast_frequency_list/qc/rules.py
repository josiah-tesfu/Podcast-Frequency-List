from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass

from podcast_frequency_list.qc.models import SegmentQcEvaluation, SegmentQcFlag

INTRO_RULE_NAME = "repeated_intro_fingerprint"
OUTRO_RULE_NAME = "repeated_outro_fingerprint"
ASR_RULE_NAME = "repetition_artifact_heuristics"

OUTRO_CTA_TERMS = (
    "merci",
    "replay",
    "semaine prochaine",
    "a bientot",
    "à bientôt",
    "ciao",
    "abonne",
    "abonnez",
    "like",
)

TOKEN_PATTERN = re.compile(r"<num>|[a-zà-öø-ÿ]+", re.IGNORECASE)
SENTENCE_SPLIT_PATTERN = re.compile(r"[.!?]+")


@dataclass(frozen=True)
class QcInputSegment:
    segment_id: int
    episode_id: int
    chunk_index: int
    normalized_text: str


def evaluate_segment_qc(
    *,
    target_segments: list[QcInputSegment],
    reference_segments: list[QcInputSegment],
) -> dict[int, SegmentQcEvaluation]:
    intro_matches = _match_repeated_edge_fingerprints(
        segments=reference_segments,
        edge="intro",
        minimum_episodes=3,
    )
    outro_matches = _match_repeated_edge_fingerprints(
        segments=reference_segments,
        edge="outro",
        minimum_episodes=2,
        require_outro_lexicon=True,
    )

    evaluations: dict[int, SegmentQcEvaluation] = {}
    for segment in target_segments:
        flags: list[SegmentQcFlag] = []

        if segment.segment_id in intro_matches:
            flags.append(
                SegmentQcFlag(
                    flag="intro_boilerplate",
                    rule_name=INTRO_RULE_NAME,
                    details=intro_matches[segment.segment_id],
                    status="remove",
                )
            )

        if segment.segment_id in outro_matches:
            flags.append(
                SegmentQcFlag(
                    flag="outro_boilerplate",
                    rule_name=OUTRO_RULE_NAME,
                    details=outro_matches[segment.segment_id],
                    status="remove",
                )
            )

        artifact_flag = _detect_asr_artifact(segment.normalized_text)
        if artifact_flag is not None:
            flags.append(artifact_flag)

        status = _derive_status(flags)
        reason_summary = ", ".join(flag.flag for flag in flags) if flags else "no_qc_flags"
        evaluations[segment.segment_id] = SegmentQcEvaluation(
            segment_id=segment.segment_id,
            episode_id=segment.episode_id,
            status=status,
            reason_summary=reason_summary,
            flags=tuple(flags),
        )

    return evaluations


def _match_repeated_edge_fingerprints(
    *,
    segments: list[QcInputSegment],
    edge: str,
    minimum_episodes: int,
    require_outro_lexicon: bool = False,
) -> dict[int, str]:
    grouped: dict[int, list[QcInputSegment]] = defaultdict(list)
    for segment in segments:
        grouped[segment.episode_id].append(segment)

    fingerprints: dict[str, list[QcInputSegment]] = defaultdict(list)
    for episode_segments in grouped.values():
        ordered = sorted(episode_segments, key=lambda segment: segment.chunk_index)
        if edge == "intro":
            selected = ordered[:2]
        else:
            selected = ordered[-2:]

        for segment in selected:
            fingerprint = _build_fingerprint(segment.normalized_text)
            if not fingerprint:
                continue
            if require_outro_lexicon and not _has_outro_lexicon(segment.normalized_text):
                continue
            fingerprints[fingerprint].append(segment)

    matches: dict[int, str] = {}
    for fingerprint, fingerprint_segments in fingerprints.items():
        episode_count = len({segment.episode_id for segment in fingerprint_segments})
        if episode_count < minimum_episodes:
            continue
        details = f"fingerprint={fingerprint} episode_count={episode_count}"
        for segment in fingerprint_segments:
            matches[segment.segment_id] = details

    return matches


def _build_fingerprint(text: str) -> str:
    lowered = text.lower()
    lowered = re.sub(r"\d+", " <num> ", lowered)
    tokens = TOKEN_PATTERN.findall(lowered)
    return " ".join(tokens[:16])


def _has_outro_lexicon(text: str) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in OUTRO_CTA_TERMS)


def _detect_asr_artifact(text: str) -> SegmentQcFlag | None:
    tokens = TOKEN_PATTERN.findall(text.lower())
    if len(tokens) < 2:
        return None

    sentence_repeat = _max_repeated_sentence_fingerprint(text)
    if sentence_repeat is not None:
        return SegmentQcFlag(
            flag="asr_artifact",
            rule_name=ASR_RULE_NAME,
            details=sentence_repeat,
            status="remove",
        )

    consecutive_span_repeats = _max_consecutive_span_repeats(tokens, span_size=12)
    if consecutive_span_repeats >= 3:
        return SegmentQcFlag(
            flag="asr_artifact",
            rule_name=ASR_RULE_NAME,
            details=f"consecutive_span_repeats={consecutive_span_repeats}",
            status="remove",
        )

    if len(tokens) >= 12:
        counts = Counter(tokens)
        top_share = max(counts.values()) / len(tokens)
        if top_share > 0.45:
            return SegmentQcFlag(
                flag="asr_artifact",
                rule_name=ASR_RULE_NAME,
                details=f"top_token_share={top_share:.2f}",
                status="review",
            )

    return None


def _max_repeated_sentence_fingerprint(text: str) -> str | None:
    counts: Counter[str] = Counter()
    for sentence in SENTENCE_SPLIT_PATTERN.split(text):
        tokens = TOKEN_PATTERN.findall(sentence.lower())
        if len(tokens) < 8:
            continue
        fingerprint = " ".join(tokens[:12])
        if fingerprint:
            counts[fingerprint] += 1

    if not counts:
        return None

    fingerprint, count = counts.most_common(1)[0]
    if count < 3:
        return None

    return f"repeated_sentence_count={count} fingerprint={fingerprint}"


def _max_consecutive_span_repeats(tokens: list[str], *, span_size: int) -> int:
    if len(tokens) < span_size * 2:
        return 0

    best = 1
    last_start = len(tokens) - span_size
    for start in range(last_start + 1):
        span = tokens[start : start + span_size]
        repeats = 1
        next_start = start + span_size
        while next_start <= last_start and tokens[next_start : next_start + span_size] == span:
            repeats += 1
            next_start += span_size
        best = max(best, repeats)

    return best


def _derive_status(flags: list[SegmentQcFlag]) -> str:
    statuses = {flag.status for flag in flags}
    if "remove" in statuses:
        return "remove"
    if "review" in statuses:
        return "review"
    return "keep"
