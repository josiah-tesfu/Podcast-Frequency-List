# Preliminary Software Architecture

## Shape

Local-first batch pipeline.

- Ingest podcast metadata and feeds
- Acquire or generate transcripts
- Normalize and clean transcript text
- Extract and rank spoken tokens
- Curate final notes
- Export Anki deck assets

## Main Components

### `discovery`

- Manual feed entry
- Canonical feed resolution

### `ingest`

- RSS parser
- Episode inventory builder
- Transcript downloader
- Episode-page transcript scraper
- Audio downloader
- Whisper runner

### `normalize`

- Transcript format adapters
- Segment schema mapper
- Language checks
- Quality scoring

### `process`

- Boilerplate removal
- Sentence splitting
- French tokenization
- N-gram generation
- Candidate scoring
- Context extraction

### `curation`

- Review dataset
- Keep / reject / blacklist decisions
- Final note assembly

### `export`

- CSV writer
- Audio clip mapper
- Anki field formatter

## Data Model

### `shows`

- `show_id`
- `title`
- `feed_url`
- `site_url`
- `language`
- `bucket`

### `episodes`

- `episode_id`
- `show_id`
- `guid`
- `title`
- `published_at`
- `audio_url`
- `episode_url`
- `duration_seconds`

### `transcript_sources`

- `source_id`
- `episode_id`
- `source_type`
- `source_url`
- `raw_path`
- `quality_score`

### `transcript_segments`

- `segment_id`
- `episode_id`
- `start_ms`
- `end_ms`
- `speaker`
- `raw_text`

### `cleaned_segments`

- `cleaned_segment_id`
- `segment_id`
- `cleaned_text`
- `removal_flags`

### `token_candidates`

- `candidate_id`
- `token_text`
- `ngram_size`
- `raw_frequency`
- `show_dispersion`
- `episode_dispersion`
- `spoken_score`
- `blacklist_flag`

### `token_examples`

- `example_id`
- `candidate_id`
- `episode_id`
- `context_text`
- `start_ms`
- `end_ms`

### `curated_notes`

- `note_id`
- `candidate_id`
- `english_gloss`
- `status`
- `primary_example_id`
- `tags`

## Pipeline Flow

`Manual feed URL -> RSS feeds -> episode inventory -> transcript fetch/ASR -> normalized segments -> cleaned segments -> n-gram candidates -> ranked shortlist -> curated notes -> Anki export`

## Recommended Repo Layout

```text
src/
  discovery/
  ingest/
  normalize/
  process/
  curation/
  export/
data/
  raw/
  normalized/
  processed/
  exports/
scripts/
docs/
```

## Design Choices

- Python CLI first, not a web app
- Local DB first, not external infra
- Raw and cleaned text stored separately
- Provenance preserved at every stage
- Ranking is automatic; final inclusion is manual
