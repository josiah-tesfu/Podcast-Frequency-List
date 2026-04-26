# CLI Utilities

This doc is the working reference for the current CLI.

Mental model:
- `show_id` = one podcast feed saved in the DB
- `pilot name` = one saved subset of episodes chosen from a show
- `limit` = how many items to process in this run, not the total size of the saved pilot
- `--force` = rerun items even if they already completed

Current command list:
- `info`
- `init-db`
- `add-show`
- `sync-feed`
- `create-pilot`
- `run-asr`
- `normalize-transcripts`
- `qc-segments`
- `split-sentences`
- `tokenize-sentences`
- `generate-candidates`
- `refresh-candidate-metrics`
- `inspect-candidate-metrics`

## Core Workflow

Normal flow:
1. add a show feed
2. sync the feed into episodes
3. create a pilot subset
4. run ASR on a small smoke test
5. run ASR on the rest of the pilot
6. normalize transcript text for downstream processing
7. add QC flags for intros/outros and obvious ASR junk
8. split kept chunks into sentence-like context lines
9. tokenize sentence rows for candidate generation
10. generate candidate inventory rows and occurrence evidence
11. refresh stored candidate metrics
12. inspect top candidate metrics

Example:

```bash
uv run podfreq add-show "https://feeds.360.audion.fm/Wmd7d5HyZ8wJGI3zZVaUq" --title "Zack en roue libre" --language fr --bucket native
uv run podfreq sync-feed --show-id 1
uv run podfreq create-pilot --show-id 1 --name zack-10h-pilot --hours 10
uv run podfreq run-asr --pilot zack-10h-pilot --limit 1
uv run podfreq run-asr --pilot zack-10h-pilot --limit 5
uv run podfreq normalize-transcripts --pilot zack-10h-pilot
uv run podfreq qc-segments --pilot zack-10h-pilot
uv run podfreq split-sentences --pilot zack-10h-pilot
uv run podfreq tokenize-sentences --pilot zack-10h-pilot
uv run podfreq generate-candidates --pilot zack-10h-pilot
uv run podfreq refresh-candidate-metrics
uv run podfreq inspect-candidate-metrics
```

## Command Reference

### `info`

What it does:
- prints project paths used by the app
- useful for checking where DB/raw files are going

Command:

```bash
uv run podfreq info
```

Typical use:
- confirm DB path
- confirm raw/processed directories

### `init-db`

What it does:
- creates the SQLite DB
- creates or updates schema tables

Command:

```bash
uv run podfreq init-db
```

When to run:
- first setup
- after schema changes

### `add-show`

What it does:
- verifies a feed URL
- creates one `show` row in the DB
- does not download episodes yet

Command shape:

```bash
uv run podfreq add-show "<feed-url>"
```

Examples:

```bash
uv run podfreq add-show "https://example.com/feed.xml"
uv run podfreq add-show "https://example.com/feed.xml" --title "Zack en roue libre" --language fr --bucket native
```

Inputs:
- `feed_url`: required
- `--title`: optional manual title override
- `--language`: optional
- `--bucket`: optional label like `native`, `ci`, `easy`

Output:
- `saved_show_id=...`
- verified title/feed URL

Use it when:
- feed is already known
- manual feed flow is the project standard

### `sync-feed`

What it does:
- fetches the RSS feed
- saves or updates episode rows
- stores episode metadata like title, date, audio URL, duration, transcript tag presence

Commands:

```bash
uv run podfreq sync-feed --show-id 1
uv run podfreq sync-feed --show-id 1 --limit 10
```

Inputs:
- `--show-id`: required
- `--limit`: optional; only sync the first N feed entries returned by the feed

Output:
- episodes seen
- inserted vs updated
- skipped missing-audio rows
- transcript-tag count

Use it when:
- first importing a show
- refreshing a feed later

Important:
- this step builds episode inventory
- this step does not transcribe audio

### `create-pilot`

What it does:
- creates a named episode subset for later ASR
- chooses episodes until target hours are reached or slightly exceeded
- marks those episodes as `needs_asr`

Commands:

```bash
uv run podfreq create-pilot --show-id 1 --name zack-10h-pilot --hours 10
uv run podfreq create-pilot --show-id 1 --name zack-3h-pilot --hours 3
uv run podfreq create-pilot --show-id 1 --name zack-20h-pilot --hours 20 --selection-order oldest
```

Inputs:
- `--show-id`: required
- `--name`: required pilot label
- `--hours`: target size in hours
- `--selection-order`: `newest` or `oldest`
- `--notes`: optional note stored with the pilot

How to think about it:
- same `show_id`, different `name` -> multiple saved pilots
- same `name` reused -> pilot definition gets updated/replaced
- hours are approximate because whole episodes are selected, not partial episodes

Output:
- pilot ID
- selected hours
- episode count
- estimated ASR cost

Good patterns:
- smoke test: `--hours 2`
- pilot: `--hours 10`
- larger corpus slice: `--hours 50`

### `run-asr`

What it does:
- finds pilot episodes still needing ASR
- downloads audio into local cache
- splits long audio into upload-safe chunks
- calls OpenAI transcription
- writes transcript text files
- writes transcript chunks into DB

Commands:

```bash
uv run podfreq run-asr --pilot zack-10h-pilot --limit 1
uv run podfreq run-asr --pilot zack-10h-pilot --limit 5
uv run podfreq run-asr --pilot zack-10h-pilot
uv run podfreq run-asr --pilot zack-10h-pilot --force
```

Inputs:
- `--pilot`: required pilot name
- `--limit`: optional; process only first N eligible episodes from that pilot
- `--force`: rerun already-ready pilot episodes too

Requires:
- `OPENAI_API_KEY`

How `limit` actually works:
- `--limit 1` = best smoke test
- `--limit 5` = next 5 eligible episodes in pilot order
- no `--limit` = all remaining eligible episodes in that pilot

How `--force` works:
- default behavior skips `ready` episodes
- `--force` ignores that and retranscribes them

Output:
- selected/completed/skipped/failed counts
- chunk count
- per-episode transcript path
- transcript preview

Files written:
- raw audio: `data/raw/audio/<show_id>/`
- chunked audio: `data/raw/audio_chunks/<episode_id>/`
- transcripts: `data/raw/transcripts/asr/episode-<episode_id>.txt`

DB effects:
- `transcript_sources.status` moves through states like `needs_asr`, `in_progress`, `ready`, `failed`
- chunk text is stored in `transcript_segments`

Scaling patterns:
- one episode smoke test:

```bash
uv run podfreq run-asr --pilot zack-10h-pilot --limit 1
```

- continue the rest of a pilot:

```bash
uv run podfreq run-asr --pilot zack-10h-pilot --limit 5
```

- finish all remaining pilot episodes:

```bash
uv run podfreq run-asr --pilot zack-10h-pilot
```

- rerun everything in a pilot:

```bash
uv run podfreq run-asr --pilot zack-10h-pilot --force
```

- use a smaller named pilot for cheaper iteration:

```bash
uv run podfreq create-pilot --show-id 1 --name zack-3h-pilot --hours 3
uv run podfreq run-asr --pilot zack-3h-pilot
```

### `qc-segments`

What it does:
- runs the first reversible cleanup layer on normalized transcript chunks
- writes `keep`, `review`, or `remove` status per chunk
- stores explicit QC flags instead of editing transcript text

Commands:

```bash
uv run podfreq qc-segments --pilot zack-10h-pilot
uv run podfreq qc-segments --episode-id 1
uv run podfreq qc-segments --pilot zack-10h-pilot --force
```

Inputs:
- `--pilot`: process all normalized chunks in one named pilot
- `--episode-id`: process one episode instead
- `--force`: rerun QC even if the current QC version already exists

Current v1 rules:
- repeated intro chunks across the same show -> `remove`
- repeated outro CTA chunks across the same show -> `remove`
- obvious repetition-heavy ASR junk -> `review` or `remove`

What changes:
- writes summary rows to `segment_qc`
- writes detailed flags to `segment_qc_flags`
- does not modify raw ASR text
- does not modify normalized text

Output:
- selected vs processed chunk counts
- keep/review/remove counts

How to use it:
- run after `normalize-transcripts`
- use `remove` chunks as default exclusions later
- inspect `review` chunks before stronger cleanup rules are added

### `split-sentences`

What it does:
- splits `keep` transcript chunks into sentence-like rows
- preserves chunk boundaries
- creates context units for later token extraction

Commands:

```bash
uv run podfreq split-sentences --pilot zack-10h-pilot
uv run podfreq split-sentences --episode-id 1
uv run podfreq split-sentences --pilot zack-10h-pilot --force
```

Inputs:
- `--pilot`: split all `keep` chunks in one named pilot
- `--episode-id`: split one episode instead
- `--force`: rerun sentence splitting for the current split version

What changes:
- writes rows to `segment_sentences`
- stores sentence order and character offsets
- does not modify normalized text
- does not modify QC rows

Output:
- selected chunk count
- sentence row count created
- skipped chunk count
- episode count touched

How to use it:
- run after `qc-segments`
- sentence splitting uses `keep` chunks only
- next downstream step is token / n-gram generation

### `tokenize-sentences`

What it does:
- converts sentence rows into ordered analysis tokens
- preserves exact surface text and character offsets
- splits French apostrophe contractions for analysis
- keeps protected forms like `aujourd'hui` together

Commands:

```bash
uv run podfreq tokenize-sentences --pilot zack-10h-pilot
uv run podfreq tokenize-sentences --episode-id 1
uv run podfreq tokenize-sentences --pilot zack-10h-pilot --force
```

Inputs:
- `--pilot`: tokenize all sentence rows in one named pilot
- `--episode-id`: tokenize one episode instead
- `--force`: rerun tokenization for the current tokenization version

What changes:
- writes rows to `sentence_tokens`
- stores token keys, surface text, token type, and sentence-relative offsets
- does not create candidate n-grams yet

Output:
- selected sentence count
- tokenized sentence count
- created token count
- skipped sentence count
- episode count touched

How to use it:
- run after `split-sentences`
- next downstream step is `generate-candidates`

### `generate-candidates`

What it does:
- turns tokenized sentence rows into candidate inventory rows
- stores one occurrence row per surviving contiguous span
- recomputes raw frequency counts for the selected scope

Commands:

```bash
uv run podfreq generate-candidates --pilot zack-10h-pilot
uv run podfreq generate-candidates --episode-id 1
uv run podfreq generate-candidates --pilot zack-10h-pilot --force
```

Inputs:
- `--pilot`: generate candidates for one named pilot
- `--episode-id`: generate candidates for one episode instead
- `--force`: rebuild candidate occurrences for the selected scope

Rules:
- exactly one of `--pilot` or `--episode-id`
- only current `split_version` + `tokenization_version` rows are used
- reruns skip sentences that already have current-version occurrences unless `--force` is set

What changes:
- upserts rows in `token_candidates`
- inserts rows in `token_occurrences`
- refreshes `raw_frequency` from current occurrence counts
- does not add scoring or ranking columns

Output:
- scope
- scope value
- inventory version
- selected sentence count
- processed sentence count
- skipped sentence count
- `created_candidates` for genuinely new candidate rows only
- `created_occurrences` for inserted occurrence rows
- episode count touched

How to use it:
- run after `tokenize-sentences`
- second non-force run should mostly increase `skipped_sentences`
- `--force` rebuilds the selected scope only

### `refresh-candidate-metrics`

What it does:
- recomputes stored candidate metrics from `token_occurrences`
- deletes current-version orphan candidates with no occurrence evidence
- refreshes deterministic `display_text`
- refreshes stored Step 4 association and boundary facts for multiword candidates

Command:

```bash
uv run podfreq refresh-candidate-metrics
```

What changes:
- updates `raw_frequency`
- updates `episode_dispersion`
- updates `show_dispersion`
- updates `display_text`
- updates `t_score`
- updates `npmi`
- updates `left_context_type_count`
- updates `right_context_type_count`
- updates `left_entropy`
- updates `right_entropy`

Output:
- inventory version
- selected candidate count
- refreshed candidate count
- orphan cleanup count
- occurrence count
- metric totals
- display text update count

How to use it:
- run after `generate-candidates`
- reruns should be deterministic

### `inspect-candidate-metrics`

What it does:
- validates stored candidate metrics against occurrence evidence
- prints top 1/2/3-gram summaries for manual inspection
- checks a small set of spoken chunks by candidate key

Commands:

```bash
uv run podfreq inspect-candidate-metrics
uv run podfreq inspect-candidate-metrics --limit 5
uv run podfreq inspect-candidate-metrics --candidate-key "en fait" --candidate-key "du coup"
```

Inputs:
- `--limit`: number of top rows per n-gram size
- `--candidate-key`: optional repeated key for focused inspection

Output:
- candidate and occurrence counts
- raw frequency mismatch count
- episode dispersion mismatch count
- show dispersion mismatch count
- display text mismatch count
- foreign key issue count
- top 1-gram rows with frequency and dispersion
- top 2/3-gram rows with frequency, dispersion, association, and boundary metrics
- matching focus rows with association and boundary metrics plus missing requested keys

How to use it:
- run after `refresh-candidate-metrics`
- zero mismatch counts means stored metrics match current occurrence evidence

### `normalize-transcripts`

What it does:
- reads ready transcript chunks from `transcript_segments`
- writes deterministic normalized text into `normalized_segments`
- keeps raw ASR chunk text untouched

Commands:

```bash
uv run podfreq normalize-transcripts --pilot zack-10h-pilot
uv run podfreq normalize-transcripts --episode-id 1
uv run podfreq normalize-transcripts --pilot zack-10h-pilot --force
```

Inputs:
- `--pilot`: normalize all transcript chunks in one pilot
- `--episode-id`: normalize one episode directly
- `--force`: rerun even if the current normalization version already exists

Rules:
- exactly one of `--pilot` or `--episode-id`
- only transcript sources with `status=ready` are used
- reruns skip current-version rows unless `--force` is set

Output:
- scope
- normalization version
- selected/normalized/skipped segment counts
- episodes touched

DB effects:
- source rows stay unchanged
- raw chunk text stays in `transcript_segments`
- derived normalized text goes to `normalized_segments`

## Common Permutations

### Refresh a feed later

```bash
uv run podfreq sync-feed --show-id 1
```

### Create multiple pilots from one show

```bash
uv run podfreq create-pilot --show-id 1 --name zack-3h-pilot --hours 3
uv run podfreq create-pilot --show-id 1 --name zack-10h-pilot --hours 10
uv run podfreq create-pilot --show-id 1 --name zack-50h-pilot --hours 50
```

### Test ASR cheaply before scaling

```bash
uv run podfreq run-asr --pilot zack-3h-pilot --limit 1
```

### Continue a partially completed pilot

```bash
uv run podfreq run-asr --pilot zack-10h-pilot
```

Reason:
- completed rows are skipped automatically unless `--force` is used

### Rebuild a pilot with a different size

```bash
uv run podfreq create-pilot --show-id 1 --name zack-10h-pilot --hours 15
```

Reason:
- same pilot name updates that saved pilot

## Practical Rules

- Use `add-show` once per feed.
- Use `sync-feed` whenever episode inventory needs refreshing.
- Use `create-pilot` to control scope and spend.
- Use `run-asr --limit 1` before large ASR runs.
- Use named pilots to experiment with different corpus sizes.
- Use `--force` carefully; it can duplicate cost.
