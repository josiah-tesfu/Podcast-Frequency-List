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
- `discover-show`

## Core Workflow

Normal flow:
1. add a show feed
2. sync the feed into episodes
3. create a pilot subset
4. run ASR on a small smoke test
5. run ASR on the rest of the pilot
6. normalize transcript text for downstream processing

Example:

```bash
uv run podfreq add-show "https://feeds.360.audion.fm/Wmd7d5HyZ8wJGI3zZVaUq" --title "Zack en roue libre" --language fr --bucket native
uv run podfreq sync-feed --show-id 1
uv run podfreq create-pilot --show-id 1 --name zack-10h-pilot --hours 10
uv run podfreq run-asr --pilot zack-10h-pilot --limit 1
uv run podfreq run-asr --pilot zack-10h-pilot --limit 5
uv run podfreq normalize-transcripts --pilot zack-10h-pilot
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
- manual flow is preferred over Podcast Index

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

### `discover-show`

What it does:
- searches Podcast Index
- lists candidate shows
- saves a selected one into the DB

Commands:

```bash
uv run podfreq discover-show "InnerFrench" --select 1
uv run podfreq discover-show "InnerFrench" --limit 5
```

Inputs:
- query text
- `--limit`
- `--select`

Important:
- this is fallback path right now
- current working path is manual `add-show`
- requires Podcast Index credentials if actually used

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
