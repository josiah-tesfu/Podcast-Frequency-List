# Podcast Frequency List

Local-first tooling for building a French spoken frequency deck from podcast transcripts.

## Stack

- Python 3.12
- `uv`
- `typer`
- SQLite
- `pytest`
- `ruff`

## Bootstrap

```bash
source "$HOME/.local/bin/env"
uv python install 3.12
uv sync --python 3.12 --group dev
cp .env.example .env
uv run podfreq --help
uv run pytest
```

## Repo Layout

```text
src/
  podcast_frequency_list/
data/
  raw/
  db/
  processed/
docs/
plans/
tests/
```

## Current Commands

```bash
uv run podfreq info
uv run podfreq init-db
uv run podfreq add-show "https://example.com/feed.xml"
uv run podfreq sync-feed --show-id 1
uv run podfreq create-pilot --show-id 1 --name zack-10h-pilot --hours 10
uv run podfreq run-asr --pilot zack-10h-pilot --limit 1
uv run podfreq normalize-transcripts --pilot zack-10h-pilot
uv run podfreq qc-segments --pilot zack-10h-pilot
uv run podfreq split-sentences --pilot zack-10h-pilot
uv run podfreq tokenize-sentences --pilot zack-10h-pilot
uv run podfreq generate-candidates --pilot zack-10h-pilot
```

## Notes

- `.env` is not auto-loaded by Python. Load it in the shell before running commands.
- Raw artifacts and local DB files stay out of git.
