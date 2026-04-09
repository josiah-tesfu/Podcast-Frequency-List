# Dev Steps

## Recommended Stack

- Python 3.12
- `uv` for env + deps
- SQLite or DuckDB
- `feedparser`, `httpx`, `beautifulsoup4`, `lxml`
- `faster-whisper`
- `spacy` + French model
- `polars` or `pandas`

## Build Order

### 1. Bootstrap repo

- Add `src/`, `data/`, `docs/`, `scripts/`
- Add config file for API keys, paths, and corpus settings
- Add `.env.example`

### 2. Define schema

- Create tables for shows, episodes, transcript sources, transcript segments, cleaned segments, token candidates, token examples, curated notes
- Add stable IDs and provenance fields everywhere

### 3. Feed discovery

- Build Podcast Index client
- Search shows and store canonical feed URLs
- Add RSS parser
- Save all episodes with metadata and transcript-tag detection

### 4. Transcript ingestion

- Download transcript files when present
- Scrape episode pages for transcript links or text blocks
- Download audio for fallback cases
- Run Whisper on missing-transcript episodes
- Store raw transcript artifacts unchanged

### 5. Transcript normalization

- Convert every source into one segment schema
- Keep timestamps and speaker labels when available
- Add language check and transcript quality score

### 6. Cleaning pipeline

- Strip boilerplate, sponsor reads, repeated intros/outros
- Preserve spoken fillers and contractions
- Save cleaned segments separately from normalized raw text

### 7. Token generation

- Sentence-split cleaned text
- Tokenize French text
- Generate 1-grams, 2-grams, 3-grams
- Keep inflected forms distinct

### 8. Ranking

- Score candidates by frequency
- Add dispersion across shows and episodes
- Penalize host names, show names, URLs, topic-locked phrases
- Store top 5,000 to 8,000 with evidence contexts

### 9. Curation tooling

- Build a review table or simple local UI
- Show token, gloss slot, rank metrics, context lines, source links
- Mark keep / reject / merge / blacklist

### 10. Deck export

- Output Anki-ready CSV
- Include note fields for token, gloss, contexts, source, rank, tags, audio path
- Create 2 card directions from one note type

### 11. Pilot

- Export first 100 to 150 notes
- Review ambiguity, production difficulty, context quality
- Adjust ranking and filtering rules once before full run

## Non-Negotiables

- Raw artifacts never overwritten
- Every downstream row links back to an episode ID
- Lemmatization never replaces card targets
- Blacklist stays explicit and versioned
- Manual curation remains the final gate
