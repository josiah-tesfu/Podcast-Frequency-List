# Project Plan

## Goal

Build a 2,000-item French spoken frequency deck from podcast transcripts drawn from a personally useful comprehensible-input corpus.

## Product Shape

- Source corpus: 8 to 12 French podcasts actually worth listening to
- Corpus size: 100 to 150 hours
- Card targets: 1-grams, 2-grams, 3-grams
- Focus: spoken tokens and chunks, not lemma study
- Verb handling: inflected forms stay distinct
- Card directions: French -> English for recognition, English -> French for production
- Card support: 3 to 5 context lines, optional source audio

## Core Learning Assumptions

- Spoken chunks matter more than a pure lemma list for fluency
- Common inflected forms are better speaking targets than infinitives
- Multiword units should occupy a large share of the deck
- Production cards are harder and more valuable than recognition-only cards

## End-to-End Plan

### 1. Define corpus

- Pick 8 to 12 shows across easy, medium, casual-native buckets
- Prefer conversational, turn-taking, everyday speech
- Reject scripted narration, audiobook style, narrow-topic shows
- Output: ranked show list and rejected-show list

### 2. Build episode inventory

- Use direct podcast RSS feeds as canonical sources
- Verify against show sites
- Parse feeds into one episode table
- Track transcript presence, audio validity, metadata completeness
- Output: shows table, episodes table, transcript availability report

### 3. Acquire transcripts

- Source order: RSS transcript tag -> episode page -> platform transcript -> Whisper fallback
- Preserve raw source files
- Normalize into one transcript schema
- Score transcript quality and flag weak episodes
- Output: raw store, normalized store, quality report

### 4. Clean and rank tokens

- Remove ads, intros, outros, repeated boilerplate
- Keep fillers, discourse markers, contractions, spoken grammar
- Generate 1 to 3 grams
- Score by frequency, dispersion, and spoken usefulness
- Attach multiple contexts per candidate
- Output: 5,000 to 8,000 ranked candidates

### 5. Curate and export deck

- Hand-pick final 2,000 items
- Keep a strong mix of single tokens and chunks
- Add glosses, contexts, tags, optional audio
- Export Anki-ready CSV
- Pilot on 100 to 150 notes before full build

## Success Criteria

- Corpus reflects real listening habits
- Transcript coverage is high enough for stable ranking
- Final deck feels spoken, not textbook
- Production cards demand exact retrieval of the target token
- Contexts are short, clear, and varied across episodes

## Main Risks

- Many feeds will not expose transcripts
- Whisper output may need heavy cleanup on noisy episodes
- Raw frequency will over-reward topic phrases without dispersion controls
- Manual curation load will be substantial near the final 2,000

## Immediate Decisions

- Choose stack: Python + local DB is the simplest path
- Choose storage: SQLite or DuckDB
- Choose ASR: faster-whisper is the most practical default
- Choose first target corpus: 3 to 5 shows for MVP, then expand
