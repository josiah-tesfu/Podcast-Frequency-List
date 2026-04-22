from __future__ import annotations

from collections.abc import Sequence

from podcast_frequency_list.tokens.models import CandidateSpan, SentenceToken

DEFAULT_MAX_NGRAM_SIZE = 3
STANDALONE_CLITIC_JUNK = frozenset({"c", "d", "j", "l", "m", "n", "s", "t"})
WORDLIKE_TOKEN_TYPES = frozenset({"word", "number"})


class SpanGenerationError(ValueError):
    pass


def generate_sentence_spans(
    *,
    sentence_id: int,
    episode_id: int,
    segment_id: int,
    sentence_text: str,
    tokens: Sequence[SentenceToken],
    max_ngram_size: int = DEFAULT_MAX_NGRAM_SIZE,
) -> tuple[CandidateSpan, ...]:
    if max_ngram_size < 1:
        raise SpanGenerationError("max_ngram_size must be at least 1")

    ordered_tokens = _validate_and_order_tokens(sentence_text=sentence_text, tokens=tokens)
    spans: list[CandidateSpan] = []

    for start_position in range(len(ordered_tokens)):
        for ngram_size in range(1, max_ngram_size + 1):
            end_position = start_position + ngram_size
            if end_position > len(ordered_tokens):
                break

            span_tokens = ordered_tokens[start_position:end_position]
            if not _is_valid_span(span_tokens):
                continue

            char_start = span_tokens[0].char_start
            char_end = span_tokens[-1].char_end
            surface_text = sentence_text[char_start:char_end]

            spans.append(
                CandidateSpan(
                    sentence_id=sentence_id,
                    episode_id=episode_id,
                    segment_id=segment_id,
                    candidate_key=" ".join(token.token_key for token in span_tokens),
                    display_text=surface_text,
                    ngram_size=ngram_size,
                    token_start_index=span_tokens[0].token_index,
                    token_end_index=span_tokens[-1].token_index + 1,
                    char_start=char_start,
                    char_end=char_end,
                    surface_text=surface_text,
                )
            )

    return tuple(spans)


def _validate_and_order_tokens(
    *,
    sentence_text: str,
    tokens: Sequence[SentenceToken],
) -> tuple[SentenceToken, ...]:
    ordered_tokens = tuple(sorted(tokens, key=lambda token: token.token_index))
    seen_indexes: set[int] = set()

    for expected_index, token in enumerate(ordered_tokens):
        if token.token_index in seen_indexes:
            raise SpanGenerationError(f"duplicate token_index: {token.token_index}")
        seen_indexes.add(token.token_index)

        if token.token_index != expected_index:
            raise SpanGenerationError(
                "token indexes must be contiguous from zero "
                f"(expected {expected_index}, got {token.token_index})"
            )
        if token.char_start < 0 or token.char_end < 0:
            raise SpanGenerationError("token offsets must be nonnegative")
        if token.char_start >= token.char_end:
            raise SpanGenerationError(
                f"token char_start must be before char_end: {token.token_index}"
            )
        if token.char_end > len(sentence_text):
            raise SpanGenerationError(
                f"token char_end exceeds sentence length: {token.token_index}"
            )
        if sentence_text[token.char_start : token.char_end] != token.surface_text:
            raise SpanGenerationError(
                f"token surface does not match sentence offsets: {token.token_index}"
            )

    return ordered_tokens


def _is_valid_span(tokens: Sequence[SentenceToken]) -> bool:
    if not tokens:
        return False

    if not any(token.token_type in WORDLIKE_TOKEN_TYPES for token in tokens):
        return False

    if all(token.token_type == "number" for token in tokens):
        return False

    if len(tokens) == 1 and tokens[0].token_key in STANDALONE_CLITIC_JUNK:
        return False

    return True
