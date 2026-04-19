from __future__ import annotations

import re
from functools import lru_cache

import spacy

from podcast_frequency_list.sentences.models import SentenceSpan

SUBSPLIT_PATTERN = re.compile(r"(?<=[.!?…])\s+")


@lru_cache(maxsize=1)
def _build_sentence_nlp():
    nlp = spacy.blank("fr")
    if "sentencizer" not in nlp.pipe_names:
        nlp.add_pipe("sentencizer")
    return nlp


def split_segment_text(text: str) -> tuple[SentenceSpan, ...]:
    document = _build_sentence_nlp()(text)

    sentences: list[SentenceSpan] = []
    for sentence in document.sents:
        sentences.extend(
            _subsplit_sentence_text(
                text=text,
                sentence_text=sentence.text,
                base_char_start=sentence.start_char,
                current_index=len(sentences),
            )
        )

    return tuple(sentences)


def _subsplit_sentence_text(
    *,
    text: str,
    sentence_text: str,
    base_char_start: int,
    current_index: int,
) -> list[SentenceSpan]:
    sentence_spans: list[SentenceSpan] = []
    relative_start = 0

    for match in SUBSPLIT_PATTERN.finditer(sentence_text):
        raw_part = sentence_text[relative_start : match.start()]
        sentence_span = _build_sentence_span(
            text=text,
            raw_text=raw_part,
            raw_char_start=base_char_start + relative_start,
            sentence_index=current_index + len(sentence_spans),
        )
        if sentence_span is not None:
            sentence_spans.append(sentence_span)
        relative_start = match.end()

    raw_tail = sentence_text[relative_start:]
    sentence_span = _build_sentence_span(
        text=text,
        raw_text=raw_tail,
        raw_char_start=base_char_start + relative_start,
        sentence_index=current_index + len(sentence_spans),
    )
    if sentence_span is not None:
        sentence_spans.append(sentence_span)

    return sentence_spans


def _build_sentence_span(
    *,
    text: str,
    raw_text: str,
    raw_char_start: int,
    sentence_index: int,
) -> SentenceSpan | None:
    leading_trim = len(raw_text) - len(raw_text.lstrip())
    trailing_trim = len(raw_text) - len(raw_text.rstrip())
    normalized_text = raw_text.strip()
    if not normalized_text:
        return None

    char_start = raw_char_start + leading_trim
    char_end = raw_char_start + len(raw_text) - trailing_trim
    return SentenceSpan(
        sentence_index=sentence_index,
        char_start=char_start,
        char_end=char_end,
        sentence_text=text[char_start:char_end],
    )
