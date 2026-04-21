from __future__ import annotations

import re

from podcast_frequency_list.tokens.models import SentenceToken

LETTER_CLASS = "A-Za-zÀ-ÖØ-öø-ÿ"
WORDLIKE_PATTERN = re.compile(
    rf"[{LETTER_CLASS}0-9]+(?:['’ʼ`-][{LETTER_CLASS}0-9]+)*"
)
APOSTROPHE_PATTERN = re.compile(r"['’ʼ`]")
NUMBER_TOKEN_PATTERN = re.compile(r"\d+(?:-\d+)*")

PROTECTED_FORMS = {
    "aujourd'hui",
    "c'est-à-dire",
    "quelqu'un",
    "quelqu'une",
}

APOSTROPHE_TRANSLATION = str.maketrans({"’": "'", "ʼ": "'", "`": "'"})


def tokenize_sentence_text(text: str) -> tuple[SentenceToken, ...]:
    tokens: list[SentenceToken] = []

    for match in WORDLIKE_PATTERN.finditer(text):
        raw_text = match.group(0)
        raw_start = match.start()
        normalized = _normalize_key(raw_text)

        if normalized in PROTECTED_FORMS or "'" not in normalized:
            tokens.append(
                _build_token(
                    token_index=len(tokens),
                    token_key=normalized,
                    surface_text=raw_text,
                    char_start=raw_start,
                    char_end=match.end(),
                )
            )
            continue

        tokens.extend(
            _split_apostrophe_token(
                raw_text=raw_text,
                raw_start=raw_start,
                token_start_index=len(tokens),
            )
        )

    return tuple(tokens)


def _split_apostrophe_token(
    *,
    raw_text: str,
    raw_start: int,
    token_start_index: int,
) -> list[SentenceToken]:
    tokens: list[SentenceToken] = []
    part_start = 0

    for match in APOSTROPHE_PATTERN.finditer(raw_text):
        part = raw_text[part_start : match.start()]
        if part:
            tokens.append(
                _build_token(
                    token_index=token_start_index + len(tokens),
                    token_key=_normalize_key(part),
                    surface_text=part,
                    char_start=raw_start + part_start,
                    char_end=raw_start + match.start(),
                )
            )
        part_start = match.end()

    tail = raw_text[part_start:]
    if tail:
        tokens.append(
            _build_token(
                token_index=token_start_index + len(tokens),
                token_key=_normalize_key(tail),
                surface_text=tail,
                char_start=raw_start + part_start,
                char_end=raw_start + len(raw_text),
            )
        )

    return tokens


def _build_token(
    *,
    token_index: int,
    token_key: str,
    surface_text: str,
    char_start: int,
    char_end: int,
) -> SentenceToken:
    return SentenceToken(
        token_index=token_index,
        token_key=token_key,
        surface_text=surface_text,
        char_start=char_start,
        char_end=char_end,
        token_type=_classify_token(token_key),
    )


def _normalize_key(text: str) -> str:
    return text.translate(APOSTROPHE_TRANSLATION).lower()


def _classify_token(token_key: str) -> str:
    return "number" if NUMBER_TOKEN_PATTERN.fullmatch(token_key) else "word"
