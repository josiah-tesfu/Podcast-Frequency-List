from podcast_frequency_list.config import (
    DEFAULT_ASR_MODEL,
    DEFAULT_DB_PATH,
    DEFAULT_PROCESSED_DATA_DIR,
    DEFAULT_RAW_DATA_DIR,
    PROJECT_ROOT,
    load_settings,
)


def test_load_settings_defaults(monkeypatch) -> None:
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv("RAW_DATA_DIR", raising=False)
    monkeypatch.delenv("PROCESSED_DATA_DIR", raising=False)
    monkeypatch.delenv("PODCAST_INDEX_API_KEY", raising=False)
    monkeypatch.delenv("PODCAST_INDEX_API_SECRET", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ASR_MODEL", raising=False)
    load_settings.cache_clear()

    settings = load_settings()

    assert settings.project_root == PROJECT_ROOT
    assert settings.db_path == PROJECT_ROOT / DEFAULT_DB_PATH
    assert settings.raw_data_dir == PROJECT_ROOT / DEFAULT_RAW_DATA_DIR
    assert settings.processed_data_dir == PROJECT_ROOT / DEFAULT_PROCESSED_DATA_DIR
    assert settings.podcast_index_api_key == ""
    assert settings.podcast_index_api_secret == ""
    assert settings.openai_api_key == ""
    assert settings.asr_model == DEFAULT_ASR_MODEL

    load_settings.cache_clear()
