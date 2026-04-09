from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = Path("data/db/podcast_frequency_list.db")
DEFAULT_RAW_DATA_DIR = Path("data/raw")
DEFAULT_PROCESSED_DATA_DIR = Path("data/processed")


def _resolve_path(env_name: str, default_relative_path: Path) -> Path:
    raw_value = os.getenv(env_name)
    if raw_value:
        candidate = Path(raw_value).expanduser()
        return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate
    return PROJECT_ROOT / default_relative_path


@dataclass(frozen=True)
class Settings:
    project_root: Path
    db_path: Path
    raw_data_dir: Path
    processed_data_dir: Path
    podcast_index_api_key: str
    podcast_index_api_secret: str

    def ensure_directories(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    return Settings(
        project_root=PROJECT_ROOT,
        db_path=_resolve_path("DB_PATH", DEFAULT_DB_PATH),
        raw_data_dir=_resolve_path("RAW_DATA_DIR", DEFAULT_RAW_DATA_DIR),
        processed_data_dir=_resolve_path("PROCESSED_DATA_DIR", DEFAULT_PROCESSED_DATA_DIR),
        podcast_index_api_key=os.getenv("PODCAST_INDEX_API_KEY", ""),
        podcast_index_api_secret=os.getenv("PODCAST_INDEX_API_SECRET", ""),
    )
