from __future__ import annotations

import re
import unicodedata

DEFAULT_USER_AGENT = "podcast-frequency-list/0.1.0"


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", ascii_value.lower()).strip()
