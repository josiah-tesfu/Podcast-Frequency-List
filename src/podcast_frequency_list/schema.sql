CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shows (
    show_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    feed_url TEXT NOT NULL UNIQUE,
    site_url TEXT,
    language TEXT,
    bucket TEXT,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS episodes (
    episode_id INTEGER PRIMARY KEY,
    show_id INTEGER NOT NULL,
    guid TEXT NOT NULL,
    title TEXT NOT NULL,
    published_at TEXT,
    audio_url TEXT,
    episode_url TEXT,
    duration_seconds INTEGER,
    summary TEXT,
    has_transcript_tag INTEGER NOT NULL DEFAULT 0 CHECK (has_transcript_tag IN (0, 1)),
    transcript_url TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    UNIQUE (show_id, guid)
);

CREATE INDEX IF NOT EXISTS idx_episodes_show_id ON episodes (show_id);
CREATE INDEX IF NOT EXISTS idx_episodes_published_at ON episodes (published_at);

CREATE TABLE IF NOT EXISTS pilot_runs (
    pilot_run_id INTEGER PRIMARY KEY,
    show_id INTEGER NOT NULL,
    name TEXT NOT NULL UNIQUE,
    target_seconds INTEGER NOT NULL CHECK (target_seconds > 0),
    selection_order TEXT NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pilot_run_episodes (
    pilot_run_id INTEGER NOT NULL,
    episode_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    cumulative_seconds INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pilot_run_id, episode_id),
    FOREIGN KEY (pilot_run_id) REFERENCES pilot_runs(pilot_run_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE,
    UNIQUE (pilot_run_id, position)
);

CREATE INDEX IF NOT EXISTS idx_pilot_run_episodes_episode_id
    ON pilot_run_episodes (episode_id);

CREATE TABLE IF NOT EXISTS transcript_sources (
    source_id INTEGER PRIMARY KEY,
    episode_id INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    status TEXT NOT NULL,
    model TEXT,
    source_url TEXT,
    raw_path TEXT,
    estimated_cost_usd REAL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE,
    UNIQUE (episode_id, source_type, model)
);

CREATE INDEX IF NOT EXISTS idx_transcript_sources_episode_id
    ON transcript_sources (episode_id);
CREATE INDEX IF NOT EXISTS idx_transcript_sources_status
    ON transcript_sources (status);

CREATE TABLE IF NOT EXISTS transcript_segments (
    segment_id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL,
    episode_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    start_ms INTEGER,
    end_ms INTEGER,
    speaker TEXT,
    raw_text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES transcript_sources(source_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE,
    UNIQUE (source_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_transcript_segments_episode_id
    ON transcript_segments (episode_id);

CREATE TABLE IF NOT EXISTS normalized_segments (
    normalized_segment_id INTEGER PRIMARY KEY,
    segment_id INTEGER NOT NULL UNIQUE,
    episode_id INTEGER NOT NULL,
    normalization_version TEXT NOT NULL,
    normalized_text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (segment_id) REFERENCES transcript_segments(segment_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_normalized_segments_episode_id
    ON normalized_segments (episode_id);
