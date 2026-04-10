from __future__ import annotations

import re
import unicodedata

PUNCTUATION_TRANSLATION = str.maketrans(
    {
        "\u00a0": " ",
        "\u2009": " ",
        "\u202f": " ",
        "\u2018": "'",
        "\u2019": "'",
        "\u02bc": "'",
        "`": "'",
        "´": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u00ab": '"',
        "\u00bb": '"',
        "\u2026": "...",
        "\u2013": "-",
        "\u2014": "-",
    }
)


def normalize_transcript_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized.translate(PUNCTUATION_TRANSLATION)
    normalized = re.sub(r"(?<=\w)\s*'\s*(?=\w)", "'", normalized)
    normalized = re.sub(r"\s+([,.;:?!])", r"\1", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()
