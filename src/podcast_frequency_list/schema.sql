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

CREATE TABLE IF NOT EXISTS segment_qc (
    segment_qc_id INTEGER PRIMARY KEY,
    segment_id INTEGER NOT NULL,
    episode_id INTEGER NOT NULL,
    qc_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('keep', 'review', 'remove')),
    reason_summary TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (segment_id) REFERENCES transcript_segments(segment_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE,
    UNIQUE (segment_id, qc_version)
);

CREATE INDEX IF NOT EXISTS idx_segment_qc_episode_id
    ON segment_qc (episode_id);

CREATE TABLE IF NOT EXISTS segment_qc_flags (
    segment_qc_flag_id INTEGER PRIMARY KEY,
    segment_id INTEGER NOT NULL,
    qc_version TEXT NOT NULL,
    flag TEXT NOT NULL,
    rule_name TEXT NOT NULL,
    details TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (segment_id) REFERENCES transcript_segments(segment_id) ON DELETE CASCADE,
    UNIQUE (segment_id, qc_version, flag, rule_name)
);

CREATE INDEX IF NOT EXISTS idx_segment_qc_flags_segment_id
    ON segment_qc_flags (segment_id);

CREATE TABLE IF NOT EXISTS segment_sentences (
    sentence_id INTEGER PRIMARY KEY,
    segment_id INTEGER NOT NULL,
    episode_id INTEGER NOT NULL,
    split_version TEXT NOT NULL,
    sentence_index INTEGER NOT NULL,
    char_start INTEGER NOT NULL,
    char_end INTEGER NOT NULL,
    sentence_text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (segment_id) REFERENCES transcript_segments(segment_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE,
    UNIQUE (segment_id, split_version, sentence_index)
);

CREATE INDEX IF NOT EXISTS idx_segment_sentences_episode_id
    ON segment_sentences (episode_id);

CREATE TABLE IF NOT EXISTS sentence_tokens (
    token_id INTEGER PRIMARY KEY,
    sentence_id INTEGER NOT NULL,
    episode_id INTEGER NOT NULL,
    segment_id INTEGER NOT NULL,
    tokenization_version TEXT NOT NULL,
    token_index INTEGER NOT NULL,
    token_key TEXT NOT NULL,
    surface_text TEXT NOT NULL,
    char_start INTEGER NOT NULL,
    char_end INTEGER NOT NULL,
    token_type TEXT NOT NULL CHECK (token_type IN ('word', 'number')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sentence_id) REFERENCES segment_sentences(sentence_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE,
    FOREIGN KEY (segment_id) REFERENCES transcript_segments(segment_id) ON DELETE CASCADE,
    UNIQUE (sentence_id, tokenization_version, token_index)
);

CREATE INDEX IF NOT EXISTS idx_sentence_tokens_sentence_id
    ON sentence_tokens (sentence_id);

CREATE INDEX IF NOT EXISTS idx_sentence_tokens_episode_id
    ON sentence_tokens (episode_id);

CREATE INDEX IF NOT EXISTS idx_sentence_tokens_token_key
    ON sentence_tokens (token_key);

CREATE TABLE IF NOT EXISTS token_candidates (
    candidate_id INTEGER PRIMARY KEY,
    inventory_version TEXT NOT NULL,
    candidate_key TEXT NOT NULL,
    display_text TEXT NOT NULL,
    ngram_size INTEGER NOT NULL CHECK (ngram_size BETWEEN 1 AND 4),
    raw_frequency INTEGER NOT NULL DEFAULT 0 CHECK (raw_frequency >= 0),
    episode_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (episode_dispersion >= 0),
    show_dispersion INTEGER NOT NULL DEFAULT 0 CHECK (show_dispersion >= 0),
    t_score REAL,
    npmi REAL,
    left_context_type_count INTEGER
        CHECK (left_context_type_count IS NULL OR left_context_type_count >= 0),
    right_context_type_count INTEGER
        CHECK (right_context_type_count IS NULL OR right_context_type_count >= 0),
    left_entropy REAL
        CHECK (left_entropy IS NULL OR left_entropy >= 0),
    right_entropy REAL
        CHECK (right_entropy IS NULL OR right_entropy >= 0),
    punctuation_gap_occurrence_count INTEGER
        CHECK (
            punctuation_gap_occurrence_count IS NULL
            OR punctuation_gap_occurrence_count >= 0
        ),
    punctuation_gap_occurrence_ratio REAL
        CHECK (
            punctuation_gap_occurrence_ratio IS NULL
            OR (
                punctuation_gap_occurrence_ratio >= 0
                AND punctuation_gap_occurrence_ratio <= 1
            )
        ),
    punctuation_gap_edge_clitic_count INTEGER
        CHECK (
            punctuation_gap_edge_clitic_count IS NULL
            OR punctuation_gap_edge_clitic_count >= 0
        ),
    punctuation_gap_edge_clitic_ratio REAL
        CHECK (
            punctuation_gap_edge_clitic_ratio IS NULL
            OR (
                punctuation_gap_edge_clitic_ratio >= 0
                AND punctuation_gap_edge_clitic_ratio <= 1
            )
        ),
    starts_with_standalone_clitic INTEGER
        CHECK (
            starts_with_standalone_clitic IS NULL
            OR starts_with_standalone_clitic IN (0, 1)
        ),
    ends_with_standalone_clitic INTEGER
        CHECK (
            ends_with_standalone_clitic IS NULL
            OR ends_with_standalone_clitic IN (0, 1)
        ),
    max_component_information REAL
        CHECK (max_component_information IS NULL OR max_component_information >= 0),
    min_component_information REAL
        CHECK (min_component_information IS NULL OR min_component_information >= 0),
    high_information_token_count INTEGER
        CHECK (
            high_information_token_count IS NULL
            OR high_information_token_count >= 0
        ),
    max_show_share REAL
        CHECK (
            max_show_share IS NULL
            OR (max_show_share >= 0 AND max_show_share <= 1)
        ),
    top2_show_share REAL
        CHECK (
            top2_show_share IS NULL
            OR (top2_show_share >= 0 AND top2_show_share <= 1)
        ),
    show_entropy REAL
        CHECK (
            show_entropy IS NULL
            OR (show_entropy >= 0 AND show_entropy <= 1)
        ),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (inventory_version, candidate_key),
    UNIQUE (candidate_id, inventory_version)
);

CREATE INDEX IF NOT EXISTS idx_token_candidates_inventory_version
    ON token_candidates (inventory_version);

CREATE INDEX IF NOT EXISTS idx_token_candidates_ngram_size
    ON token_candidates (inventory_version, ngram_size);

CREATE INDEX IF NOT EXISTS idx_token_candidates_frequency
    ON token_candidates (inventory_version, raw_frequency DESC);

CREATE TABLE IF NOT EXISTS token_occurrences (
    occurrence_id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL,
    sentence_id INTEGER NOT NULL,
    episode_id INTEGER NOT NULL,
    segment_id INTEGER NOT NULL,
    inventory_version TEXT NOT NULL,
    token_start_index INTEGER NOT NULL CHECK (token_start_index >= 0),
    token_end_index INTEGER NOT NULL CHECK (token_end_index > token_start_index),
    char_start INTEGER NOT NULL CHECK (char_start >= 0),
    char_end INTEGER NOT NULL CHECK (char_end > char_start),
    surface_text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (candidate_id, inventory_version)
        REFERENCES token_candidates(candidate_id, inventory_version)
        ON DELETE CASCADE,
    FOREIGN KEY (sentence_id) REFERENCES segment_sentences(sentence_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE,
    FOREIGN KEY (segment_id) REFERENCES transcript_segments(segment_id) ON DELETE CASCADE,
    UNIQUE (
        inventory_version,
        sentence_id,
        token_start_index,
        token_end_index
    )
);

CREATE INDEX IF NOT EXISTS idx_token_occurrences_candidate
    ON token_occurrences (candidate_id);

CREATE INDEX IF NOT EXISTS idx_token_occurrences_sentence
    ON token_occurrences (sentence_id);

CREATE INDEX IF NOT EXISTS idx_token_occurrences_episode
    ON token_occurrences (inventory_version, episode_id);

CREATE INDEX IF NOT EXISTS idx_token_occurrences_scope
    ON token_occurrences (inventory_version, episode_id, segment_id);

CREATE TABLE IF NOT EXISTS candidate_containment (
    inventory_version TEXT NOT NULL,
    smaller_candidate_id INTEGER NOT NULL,
    larger_candidate_id INTEGER NOT NULL,
    extension_side TEXT NOT NULL CHECK (extension_side IN ('left', 'right', 'both')),
    shared_occurrence_count INTEGER NOT NULL CHECK (shared_occurrence_count > 0),
    shared_episode_count INTEGER NOT NULL
        CHECK (
            shared_episode_count > 0
            AND shared_episode_count <= shared_occurrence_count
        ),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (inventory_version, smaller_candidate_id, larger_candidate_id),
    CHECK (smaller_candidate_id <> larger_candidate_id),
    FOREIGN KEY (smaller_candidate_id, inventory_version)
        REFERENCES token_candidates(candidate_id, inventory_version)
        ON DELETE CASCADE,
    FOREIGN KEY (larger_candidate_id, inventory_version)
        REFERENCES token_candidates(candidate_id, inventory_version)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_candidate_containment_larger
    ON candidate_containment (inventory_version, larger_candidate_id);

CREATE TABLE IF NOT EXISTS candidate_scores (
    inventory_version TEXT NOT NULL,
    score_version TEXT NOT NULL,
    candidate_id INTEGER NOT NULL,
    ranking_lane TEXT NOT NULL CHECK (ranking_lane IN ('1gram', '2gram', '3gram')),
    passes_support_gate INTEGER NOT NULL CHECK (passes_support_gate IN (0, 1)),
    passes_quality_gate INTEGER NOT NULL CHECK (passes_quality_gate IN (0, 1)),
    discard_family TEXT
        CHECK (
            discard_family IS NULL
            OR discard_family IN (
                'support_floor',
                'edge_clitic_gap',
                'weak_multiword',
                'show_specificity',
                'parent_fragment',
                'open_edge_fragment'
            )
        ),
    is_eligible INTEGER NOT NULL CHECK (is_eligible IN (0, 1)),
    frequency_score REAL,
    dispersion_score REAL,
    association_score REAL,
    boundary_score REAL,
    redundancy_penalty REAL,
    final_score REAL,
    lane_rank INTEGER CHECK (lane_rank IS NULL OR lane_rank > 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (inventory_version, score_version, candidate_id),
    CHECK (is_eligible = 1 OR lane_rank IS NULL),
    CHECK (is_eligible = 1 OR final_score IS NULL),
    CHECK (is_eligible = 0 OR discard_family IS NULL),
    CHECK (is_eligible = 1 OR discard_family IS NOT NULL),
    FOREIGN KEY (candidate_id, inventory_version)
        REFERENCES token_candidates(candidate_id, inventory_version)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_candidate_scores_lane_rank
    ON candidate_scores (
        inventory_version,
        score_version,
        ranking_lane,
        lane_rank
    );
