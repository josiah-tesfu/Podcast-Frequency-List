# Corpus Expansion Scaffolding

## Goal

Prepare the repo for a `10 shows x 10 hours` French corpus run before moving to Step `7` sentence/example work.

Target shape:
- `~100` hours total
- `10` conversational / casual French podcasts
- stronger `show_dispersion`
- less single-show bias in Step `4` / `5` / `6`

## Locked Decisions

- transcript source: `ASR only`
- slice selection order: `newest`
- naming: by podcast name
- user-facing wording: prefer `slice` / `corpus`
- internal schema/code can keep `pilot` for now to avoid broad churn

Examples:
- `zack-en-roue-libre-10h-slice`
- `small-talk-konbini-10h-slice`
- `corpus-fr-casual-100h`

## Current State

What already exists:
- multi-show `shows` + `episodes` tables
- per-show saved slice selection via current `pilot_runs`
- per-slice ASR / normalize / QC / split / tokenize / candidate generation
- global candidate metrics + score refresh

Main gap:
- the base pipeline works
- the operational layer for a `10 show / 100 hour` run is still thin

Main risks if run now:
- too much manual repetition
- weak visibility into per-show progress
- bad episode selection from teaser / extrait / best-of style episodes
- avoidable operator mistakes across `10` separate slices

## Plan

### S1. Add corpus manifest

What:
- create one machine-readable source-of-truth file for the chosen shows

Recommended file:
- `data/show_manifest.csv`

Recommended columns:
- `slug`
- `title`
- `feed_url`
- `language`
- `bucket`
- `family`
- `target_hours`
- `selection_order`
- `enabled`
- `notes`

Why:
- current show list only lives in docs
- batch work should not depend on manual copy/paste
- this becomes the control surface for later corpus reruns

Complexity:
- straightforward

Validation:
- parse file successfully
- assert unique `slug`
- assert unique `feed_url`
- assert all enabled rows have `target_hours > 0`
- assert `selection_order = newest` for all current rows

Exit criteria:
- one canonical manifest exists
- all `10` chosen shows are represented there

### S2. Add show and slice status reporting

What:
- add a small reporting surface for corpus progress

Recommended outputs:
- saved shows
- `show_id`
- title
- feed URL
- episode count
- total duration hours
- episodes with transcript tag
- saved slice name
- slice episode count
- slice hours
- ASR `needs_asr / in_progress / ready / failed`

Why:
- `10` parallel slices are hard to track without one status surface
- this should exist before a long ASR run

Recommended shape:
- one CLI command like `inspect-corpus` or `show-status`
- read-only only

Complexity:
- straightforward to moderate

Validation:
- run on current DB
- verify show counts against known feeds
- verify slice counts match `pilot_run_episodes`
- verify ASR counts match `transcript_sources`

Exit criteria:
- one command can answer "what is loaded, what is selected, what is transcribed, what is left"

### S3. Add episode eligibility filter before slice creation

What:
- tighten slice selection so bad episode types are excluded before ASR

Current problem:
- current selector only checks audio URL + duration
- no guard against:
  - `teaser`
  - `bande-annonce`
  - `extrait`
  - `best of`
  - `rediff`
  - `live special`
  - short promo / announcement episodes

Recommended policy:
- add title-pattern excludes
- add optional minimum duration floor
- keep the policy explicit and easy to inspect

Recommended first rules:
- reject if normalized title contains obvious promo/repack keywords
- reject if duration is below a chosen floor for the main corpus run
- keep the rules narrow and inspect false positives manually

Why:
- this is the biggest quality-control gap before `100` hours

Complexity:
- moderate

Validation:
- unit tests for keep / drop episode titles
- dry-run selection summary per show
- manually inspect `20-30` selected episodes across several shows

Exit criteria:
- slice creation stops pulling in obvious junk episodes

### S4. Add batch show bootstrap

What:
- automate `add-show` + `sync-feed` from the manifest

Recommended shape:
- one small script under `scripts/`
- reads `data/show_manifest.csv`
- for each enabled row:
  - verify / add show
  - sync feed

Why:
- this avoids ten manual add/sync cycles
- lowers risk of inconsistent titles / buckets

Complexity:
- straightforward

Validation:
- idempotent rerun
- no duplicate show rows
- synced episode counts stable on second pass unless feeds changed

Exit criteria:
- all enabled shows can be loaded from one command

### S5. Add batch slice creation

What:
- automate one `10h` slice per enabled show

Recommended behavior:
- derive slice name from manifest `slug`
- use locked policy:
  - `target_hours = 10`
  - `selection_order = newest`
- optionally store manifest metadata in `notes`

Why:
- keeps naming consistent
- avoids manual `show_id` handling across ten shows

Complexity:
- straightforward

Validation:
- one slice per enabled show
- no naming collisions
- selected hours roughly match target
- second run updates in place rather than duplicating

Exit criteria:
- `10` clean per-show slices exist with predictable names

### S6. Add batch processing runner

What:
- automate the per-slice processing chain

Scope:
- `run-asr`
- `normalize-transcripts`
- `qc-segments`
- `split-sentences`
- `tokenize-sentences`
- `generate-candidates`

Recommended shape:
- script runner, not a large new service layer
- keep orchestration thin
- call the existing CLI/service boundaries

Why:
- this is repetitive operational work
- script is enough; no big architecture needed

Recommended behavior:
- run one slice at a time
- stop on failure
- emit per-slice summaries
- allow restart without corrupting prior work

Complexity:
- moderate

Validation:
- smoke test on `2` shows first
- verify rerun behavior after partial completion
- verify candidate generation totals increase as expected

Exit criteria:
- one command can process the enabled slices without manual command chaining

### S7. Add corpus milestone review

What:
- formal checkpoints before the full `100h` run is trusted

Milestones:
- `~30h`
- `~60h`
- `~100h`

At each checkpoint:
- refresh candidate metrics
- refresh candidate scores
- inspect `show_dispersion`
- inspect top / middle / tail candidate quality
- compare against prior single-show behavior

Questions to answer:
- are weak residues falling more naturally?
- are show-specific phrases dropping?
- are strong spoken chunks staying high?
- is `show_dispersion` finally informative?

Why:
- ranking should be retuned with more evidence, not assumed stable

Complexity:
- straightforward analytically, but important

Validation:
- DB sanity checks
- top-list audit
- score-tail audit
- repeated refresh determinism

Exit criteria:
- the `100h` corpus is trustworthy enough to resume Step `7`

## Recommended Build Order

1. `S1` manifest
2. `S3` episode eligibility filter
3. `S2` status reporting
4. `S4` batch show bootstrap
5. `S5` batch slice creation
6. `S6` batch processing runner
7. `S7` milestone review

Reason:
- manifest + episode filtering should come first
- there is no point automating bad selection logic

## What Not To Build Yet

- transcript-tag ingestion
- full internal rename from `pilot` to `slice`
- sentence/example tooling
- new ranking heuristics beyond what larger corpus evidence demands
- broad schema redesign

## Success Condition

This scaffolding phase is done when:
- all `10` chosen shows are represented in one manifest
- bad teaser / extrait / best-of episodes are filtered before selection
- one status surface shows progress across all shows
- one batch path can run load -> select -> process reliably
- Step `4` / `5` / `6` can be reassessed on a real multi-show corpus
