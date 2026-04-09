CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shows (
    show_id INTEGER PRIMARY KEY,
    podcast_index_id INTEGER UNIQUE,
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
