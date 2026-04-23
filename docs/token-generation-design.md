# Token Generation Design

## Purpose

Build a deterministic candidate-generation pipeline for spoken French deck items.

The goal is not to rank raw words only. The goal is to identify learnable spoken retrieval units:

- single wordforms
- inflected verb forms
- short spoken chunks
- clitic/reflexive frames
- discourse expressions
- fixed expressions

The system should stay programmatic, repeatable, and portable to other languages with minimal language-specific rules.

## Core Decision

Use language-profiled span mining.

Pipeline shape:

```text
sentence rows -> token rows -> contiguous spans -> candidate inventory -> occurrence evidence -> scoring -> redundancy suppression -> examples -> deck candidates
```

Do not start with hardcoded candidate families.

Instead, generate broad contiguous spans and score whether each span behaves like a stable, useful spoken unit.

## Main Concept: Unithood

The central ranking question:

```text
Does this span behave like one learnable spoken unit?
```

Strong examples:

- `du coup`
- `en fait`
- `il y a`
- `je pense`
- `je me dis`
- `j'ai envie`

Weak examples:

- `de la`
- `que je`
- `et le`
- `dans un`

Weak examples are not rejected manually. They should fall naturally through scoring.

## Tokenization Strategy

Use analysis tokens with surface-span display.

Example:

```text
J'ai envie de dire que l'homme est là.
```

Analysis tokens:

```text
j | ai | envie | de | dire | que | l | homme | est | là
```

Candidate display is recovered from character offsets:

- `j ai` displays as `J'ai`
- `l homme` displays as `l'homme`

Benefits:

- contractions remain useful as chunks
- individual wordforms remain available
- final card display stays natural
- candidate logic stays deterministic

## French Profile

The universal span-mining engine needs a small language profile.

French profile should define:

- tokenizer behavior
- apostrophe/contraction splitting
- protected forms
- stop/function word list
- numeric policy
- punctuation policy

Protected forms should likely include:

- `aujourd'hui`
- `quelqu'un`
- `quelqu'une`

Hyphenated forms usually stay together:

- `est-ce`
- `c'est-à-dire`
- `là-dessus`
- `peut-être`
- `vas-y`

## Candidate Generation

Generate all contiguous spans inside each sentence.

Default inventory:

- 1-token spans
- 2-token spans
- 3-token spans

Possible later support:

- 4-token spans for especially strong spoken chunks

No spans should cross sentence boundaries.

No spans should cross removed QC chunks because sentence rows already come from kept chunks only.

## Hard Filters

Keep hard filters minimal.

Reject:

- spans containing no word tokens
- pure numeric spans
- pure punctuation spans
- pure one-letter junk spans

Do not reject early:

- function-word spans
- clitic spans
- pronoun spans
- low-frequency spans
- strange-looking but valid spoken chunks

Those should be handled by scoring.

## Candidate Key And Display

Each candidate needs:

- `candidate_key`: normalized analysis form
- `display_text`: most common surface form

Examples:

| Candidate key | Display text |
| --- | --- |
| `je dirais` | `je dirais` |
| `j ai` | `j'ai` |
| `l homme` | `l'homme` |

The candidate key powers counting. The display text powers review and Anki export.

## Occurrence Evidence

Every candidate occurrence must link to the exact sentence.

Store:

- candidate ID
- sentence ID
- episode ID
- segment ID
- token start index
- token end index
- character start
- character end
- surface text

This supports:

- context examples
- source diversity
- sense/context review
- debugging
- Anki export

## Scoring Features

Raw frequency is only one feature.

Candidate scoring should include:

- frequency
- episode dispersion
- show dispersion later
- association strength
- left/right boundary strength
- context quality
- redundancy/coverage
- proper-noun penalty
- function-word penalty
- length-specific prior

## Association Metrics

Use multiple metrics over time.

Recommended:

- minimum frequency gate
- t-score for useful common chunks
- normalized PMI for cohesion
- log-likelihood later if needed
- left/right entropy for boundary quality

Avoid relying only on PMI because rare phrases get overpromoted.

Avoid relying only on raw frequency because grammar glue gets overpromoted.

## Redundancy Resolution

Keep overlapping candidates in inventory.

Suppress redundancy during ranking.

Example:

```text
dirais
je dirais
```

If `dirais` occurs mostly inside `je dirais`, then:

- `je dirais` gets promoted
- `dirais` gets a redundancy penalty

If `dirais` occurs across many frames, then:

- `dirais` remains a useful independent candidate

Same logic handles:

- `me dis` vs `je me dis`
- `se passe` vs `ça se passe`
- `envie` vs `j'ai envie`
- `fait` vs `ça me fait`

## Length Handling

1-grams, 2-grams, and 3-grams should share the same inventory pipeline.

They should not share identical thresholds.

Reason:

- 1-grams naturally have higher frequency
- 2/3-grams often have higher spoken retrieval value
- longer chunks need lower frequency thresholds but stronger association requirements

Use length-specific scoring weights and thresholds.

## Data Model

### `sentence_tokens`

- `token_id`
- `sentence_id`
- `episode_id`
- `segment_id`
- `token_index`
- `token_key`
- `surface_text`
- `char_start`
- `char_end`
- `token_type`

### `token_candidates`

- `candidate_id`
- `candidate_key`
- `display_text`
- `ngram_size`
- `raw_frequency`
- `episode_dispersion`
- `show_dispersion`
- `created_at`
- `updated_at`

### `token_occurrences`

- `occurrence_id`
- `candidate_id`
- `sentence_id`
- `episode_id`
- `segment_id`
- `token_start_index`
- `token_end_index`
- `char_start`
- `char_end`
- `surface_text`

### Later: `candidate_scores`

- `candidate_id`
- `score_version`
- `frequency_score`
- `dispersion_score`
- `association_score`
- `boundary_score`
- `redundancy_penalty`
- `final_score`

## Multi-Step Plan

### Step 1: Tokenization Foundation

Build tokenization from `segment_sentences`.

Deliverables:

- French tokenizer profile
- `sentence_tokens` table
- tokenization service
- CLI command, likely `tokenize-sentences`
- tests for apostrophes, hyphens, protected forms, offsets

Output:

- each sentence has ordered tokens with offsets

### Step 2: Candidate Inventory

Generate broad contiguous spans from `sentence_tokens`.

This step is intentionally split into smaller substeps. Candidate inventory is
the first stage where small implementation mistakes can distort every later
ranking result.

Final Step 2 output:

- raw 1/2/3-token candidate inventory
- every occurrence linked to exact sentence context
- deterministic reruns for pilot, episode, and full-corpus scopes
- inspectable DB state before scoring begins

#### Step 2A: Span Generator

Build the pure in-memory span generator.

Scope:

- input: one sentence plus its ordered `sentence_tokens`
- output: valid contiguous 1/2/3-token spans
- no database writes
- no scoring

Rules:

- generate only inside one sentence
- preserve `token_start_index` and exclusive `token_end_index`
- recover `surface_text` from sentence character offsets
- use token keys for `candidate_key`
- use sentence substring for display/surface evidence
- reject only hard invalid spans: empty, all punctuation, pure numeric, one-letter clitic junk as standalone 1-grams

Validation:

- offset validation: span surface equals sentence substring
- no span crosses a sentence boundary
- all emitted spans have stable candidate keys
- max n-gram size is configurable, default `3`

Sanity checks:

- `J'ai` can produce key `j ai` with surface `J'ai`
- `l'homme` can produce key `l homme` with surface `l'homme`
- `22` alone is rejected, but `22 fois` can be kept
- standalone `j`, `l`, `d`, etc. are rejected as 1-gram junk

Tests:

- apostrophe spans
- hyphen/protected-token spans
- numeric filtering
- one-letter junk filtering
- max n-gram limit
- offset recovery

Exit criteria:

- unit tests pass
- span output looks correct on hand-built French examples
- no DB schema changes required yet

#### Step 2B: Candidate Inventory Schema

Add persistent storage for candidates and occurrences.

Scope:

- create `token_candidates`
- create `token_occurrences`
- add indexes and constraints
- add inventory versioning
- no candidate generation service yet

Schema requirements:

- `token_candidates` stores one row per candidate key per inventory version
- `token_occurrences` stores every sentence-level occurrence
- occurrence rows link to candidate, sentence, episode, and segment
- occurrence rows store token indexes and character offsets
- duplicate occurrence inserts are prevented by a unique constraint on version, sentence, and token span

Validation:

- schema version increments once
- `PRAGMA foreign_key_check` returns no rows
- table/index existence is verified
- empty tables can be created from a fresh database
- existing DB upgrades cleanly through `init_db`

Sanity checks:

- candidate key uniqueness is scoped by inventory version
- occurrence uniqueness prevents duplicate reruns and duplicate candidates for the same sentence span
- indexes support candidate lookup, sentence lookup, and episode lookup

Tests:

- schema creation test
- foreign-key integrity test
- uniqueness constraint test
- fresh DB initialization test

Exit criteria:

- DB initializes cleanly
- full test suite passes
- no live pilot data written by this substep

#### Step 2C: Inventory Persistence Service

Connect the span generator to the database.

Scope:

- add `INVENTORY_VERSION = "1"`
- add `CandidateInventoryService`
- add `CandidateInventoryError`
- add `CandidateInventoryResult`
- load tokenized sentences by pilot or episode scope
- generate spans for selected sentences
- upsert candidate rows
- insert occurrence rows
- support `--force` reruns for the selected scope
- keep scoring out of scope

Out of scope:

- no CLI command yet
- no ranking or scoring
- no modal display-text selection pass
- no show-scope or full-corpus CLI surface yet

Recommended code shape:

- keep `generate_sentence_spans(...)` in `tokens/spans.py`
- add the persistence service in the `tokens` package beside tokenization
- reuse `resolve_transcript_scope(...)` for pilot vs episode resolution
- reuse current version constants from sentence/token stages when loading input rows

Data contract for the service result:

- `scope`
- `scope_value`
- `inventory_version`
- `selected_sentences`
- `processed_sentences`
- `skipped_sentences`
- `created_candidates`
- `created_occurrences`
- `episode_count`

Implementation slices:

1. Load current-version sentence targets

- query `segment_sentences` only for `split_version = SPLIT_VERSION`
- join `sentence_tokens` only for `tokenization_version = TOKENIZATION_VERSION`
- do not read stale sentence rows from older split versions
- do not read stale token rows from older tokenization versions
- include per-sentence `existing_occurrence_count` for the current `inventory_version`
- order pilot scope by `pilot_run_episodes.position`, `segment_id`, `sentence_index`, `token_index`
- order episode scope by `segment_id`, `sentence_index`, `token_index`
- fold the ordered rowset into one in-memory sentence object with ordered tokens
- fail fast if the resolved scope has no current-version tokenized sentences

2. Decide skip vs process before writing

- default mode skips sentences where `existing_occurrence_count > 0` for the current `inventory_version`
- processed sentences are the remainder of the selected scope
- keep the skip decision at sentence granularity, not candidate granularity
- count episodes touched from the selected sentence set, not from inserted rows only
- make the zero-span edge case explicit: without a per-sentence marker table, sentences that emit zero valid spans will be recomputed on later default runs

3. Generate spans in memory per sentence

- call `generate_sentence_spans(...)` once per processed sentence
- pass the sentence text and the already ordered `SentenceToken` rows
- keep `max_ngram_size = 3` as the default
- validate all span offsets before any inserts for that sentence
- treat each sentence as the atomic work unit for emitted spans

4. Upsert candidate rows deterministically

- candidate identity is `(inventory_version, candidate_key)`
- `display_text` in 2C is provisional; use the first-seen surface form from deterministic sentence order
- do not add modal-surface logic here
- `ngram_size` comes directly from the emitted span
- create an in-memory cache keyed by `candidate_key` to avoid repeated candidate lookups inside the run
- on conflict, keep the existing row and reuse its `candidate_id`
- count only genuinely new rows in `created_candidates`

5. Insert occurrence rows exactly once

- insert one occurrence row per emitted span
- always write `candidate_id`, `sentence_id`, `episode_id`, `segment_id`
- always write `token_start_index`, `token_end_index`, `char_start`, `char_end`, `surface_text`
- rely on the unique constraint `(inventory_version, sentence_id, token_start_index, token_end_index)` as the final duplicate guard
- do not use `INSERT OR IGNORE`; duplicate attempts should fail loudly during development
- `created_occurrences` should equal the emitted span count for processed sentences

6. Recompute candidate frequency after occurrence writes

- treat `raw_frequency` as a derived count from `token_occurrences`
- do not try to increment and decrement `raw_frequency` inline across force rebuilds
- after inserts, recompute `raw_frequency` from `token_occurrences` for the current `inventory_version`
- recommended first pass: recompute the whole current inventory version in one SQL update
- correctness is more important here than micro-optimizing small pilot runs

7. Handle forced rebuilds safely

- resolve the selected sentence IDs first
- delete current-version occurrence rows for those sentence IDs only
- rebuild only the selected scope
- recompute `raw_frequency` after the rebuild
- delete orphaned `token_candidates` rows for the current `inventory_version` after frequency recomputation
- orphan cleanup must not touch rows from other inventory versions
- forced pilot rebuild must not remove occurrences from episodes outside that pilot
- forced episode rebuild must not remove occurrences from other episodes

8. Keep the transaction model simple

- use one DB transaction for the selected run
- perform force-scope deletes, inserts, frequency recomputation, and orphan cleanup in that same transaction
- commit only after the scope is internally coherent
- let constraint failures abort the transaction rather than partially succeeding

Rerun behavior:

- default mode skips sentences that already have current-version occurrences
- `--force` deletes occurrence rows for the selected scope/version, then rebuilds them
- orphaned candidates for the current inventory version are cleaned up after forced scoped rebuilds

Recommended query shape:

- one loader query for pilot scope
- one loader query for episode scope
- one delete statement for forced scope occurrence cleanup
- one candidate upsert statement
- one candidate lookup fallback statement
- one occurrence insert statement
- one `raw_frequency` recomputation statement
- one orphan-candidate cleanup statement

Recommended internal records:

- `_CandidateInventorySentenceTarget`
- fields: `sentence_id`, `episode_id`, `segment_id`, `sentence_text`, `tokens`, `existing_occurrence_count`

Recommended validation queries during implementation:

- occurrence count for current version:

```sql
SELECT COUNT(*)
FROM token_occurrences
WHERE inventory_version = ?;
```

- duplicate occurrence check:

```sql
SELECT inventory_version, sentence_id, token_start_index, token_end_index, COUNT(*) AS dup_count
FROM token_occurrences
WHERE inventory_version = ?
GROUP BY inventory_version, sentence_id, token_start_index, token_end_index
HAVING COUNT(*) > 1;
```

- offset/surface integrity check:

```sql
SELECT COUNT(*) AS mismatch_count
FROM token_occurrences occ
JOIN segment_sentences sent
    ON sent.sentence_id = occ.sentence_id
WHERE occ.inventory_version = ?
AND occ.surface_text != substr(
    sent.sentence_text,
    occ.char_start + 1,
    occ.char_end - occ.char_start
);
```

- candidate frequency consistency check:

```sql
SELECT COUNT(*) AS mismatch_count
FROM token_candidates cand
LEFT JOIN (
    SELECT candidate_id, inventory_version, COUNT(*) AS occurrence_count
    FROM token_occurrences
    WHERE inventory_version = ?
    GROUP BY candidate_id, inventory_version
) occ
    ON occ.candidate_id = cand.candidate_id
    AND occ.inventory_version = cand.inventory_version
WHERE cand.inventory_version = ?
AND cand.raw_frequency != COALESCE(occ.occurrence_count, 0);
```

- scope leakage check for pilot rebuilds:

```sql
SELECT COUNT(*) AS outside_scope_count
FROM token_occurrences occ
WHERE occ.inventory_version = ?
AND occ.episode_id NOT IN (
    SELECT pre.episode_id
    FROM pilot_runs pr
    JOIN pilot_run_episodes pre
        ON pre.pilot_run_id = pr.pilot_run_id
    WHERE pr.name = ?
);
```

Validation:

- occurrence count equals emitted span count
- no duplicate occurrences
- every occurrence has a valid candidate and sentence
- all occurrence offsets match source sentence substrings
- all occurrences belong to selected pilot/episode scope
- candidate `raw_frequency` matches occurrence counts after every run
- loader queries only use current split/tokenization versions
- force rebuild leaves the selected scope internally identical on rerun

Sanity checks:

- sample candidates link back to readable sentences
- examples show natural surface spans
- counts are plausible by sentence count and token count
- skipped/processed counts make rerun behavior obvious
- candidates like `j ai`, `en fait`, `tu vois` collapse across repeated occurrences
- forced rebuild of one episode does not change counts for untouched episodes

Tests:

- pilot-scope generation
- episode-scope generation
- idempotent rerun without `--force`
- forced rebuild
- occurrence foreign-key links
- offset integrity
- current split/tokenization version filtering
- `raw_frequency` recomputation
- orphan-candidate cleanup after forced scoped rebuild

Fixture design for tests:

- at least two episodes in one show
- one pilot that includes both episodes
- sentence examples with apostrophes, numbers, and repeated chunks
- at least one candidate shared across episodes
- at least one candidate unique to one episode so force cleanup can prove orphan removal
- at least one sentence that emits many spans so counts are nontrivial

Suggested implementation order:

1. add result/error/version types
2. add scope loaders that return sentence targets with ordered tokens
3. add pure per-sentence persistence helper: spans in, candidate IDs out, occurrences written
4. add run-level frequency recomputation
5. add `--force` scoped delete + orphan cleanup
6. add tests before any CLI work

Exit criteria:

- inventory generation works on a small fixture DB
- full test suite passes
- no scoring columns or ranking logic added here
- force rebuild is deterministic for pilot and episode scopes
- DB validation queries above return zero mismatches

#### Step 2D: CLI And Reporting

Add the operator-facing command for inventory generation.

Command:

```text
podfreq generate-candidates --pilot <pilot-name>
podfreq generate-candidates --episode-id <episode-id>
podfreq generate-candidates --pilot <pilot-name> --force
```

Output should include:

- scope
- scope value
- inventory version
- selected sentences
- processed sentences
- skipped sentences
- created_candidates
- created occurrences
- episodes touched

Validation:

- CLI returns nonzero on invalid scope
- output is stable and script-readable
- docs explain normal run vs forced rerun

Sanity checks:

- first run creates occurrences
- second run skips already processed sentences
- forced run rebuilds the same scope
- command output matches DB counts

Tests:

- CLI help includes command
- pilot command calls service correctly
- episode command calls service correctly
- invalid scope flags fail
- output formatting test

Exit criteria:

- command can be safely run by hand
- CLI docs are updated
- full test suite passes

#### Step 2E: Pilot Run And Inventory Inspection

Run candidate generation on `zack-10h-pilot`.

Required commands:

```text
uv run podfreq generate-candidates --pilot zack-10h-pilot
uv run ruff check src tests
uv run pytest
```

DB validation:

- total candidates by n-gram size
- total occurrences by n-gram size
- duplicate occurrence count is zero
- foreign-key check is clean
- offset mismatch count is zero
- occurrence episode IDs match pilot episodes only

Sanity checks:

- inspect top 1-grams by occurrence count
- inspect top 2-grams by occurrence count
- inspect top 3-grams by occurrence count
- inspect random occurrence rows with sentence context
- confirm obvious spoken chunks appear: `en fait`, `du coup`, `je pense`, `il y a`
- confirm junk standalone clitics do not dominate 1-grams

Tests:

- unit and integration tests from 2A-2D remain green
- pilot run does not require network
- pilot run is deterministic across reruns

Exit criteria:

- pilot DB inventory is coherent
- generated candidates are broad enough for later scoring
- obvious junk is not catastrophically overrepresented
- Step 3 can compute frequency and dispersion from stored occurrences

### Step 3: Frequency And Dispersion

Compute initial candidate metrics.

Deliverables:

- raw frequency
- episode dispersion
- basic display-form selection
- basic candidate summary query

Output:

- inspectable top 1/2/3-grams

### Step 4: Association And Boundary Features

Add unithood metrics for multiword spans.

Deliverables:

- t-score
- normalized PMI
- left/right context counts
- left/right entropy

Output:

- better signal for true chunks vs glue phrases

### Step 5: Redundancy And Coverage

Detect when smaller candidates are mostly covered by larger candidates.

Deliverables:

- containment coverage metrics
- redundancy penalty
- overlap-aware ranking behavior

Output:

- better choices between `dirais` and `je dirais`

### Step 6: Candidate Ranking V1

Combine metrics into a first ranking.

Deliverables:

- `candidate_scores` table
- score versioning
- length-specific thresholds
- top candidate report

Output:

- ranked candidate list for review

### Step 7: Example Selection

Choose context sentences for candidates.

Deliverables:

- context quality rules
- diverse episode examples
- primary example selection
- candidate examples table or view

Output:

- candidates ready for curation/export work

### Step 8: Review And Tuning

Inspect candidate quality and tune deterministic rules.

Deliverables:

- top candidate inspection queries/CLI
- blacklist hooks
- threshold revisions
- notes on false positives/false negatives

Output:

- stable rules for pilot-scale ranking

## Recommended Immediate Next Step

Step 1 is implemented.

Current completed target:

```text
sentence rows -> sentence_tokens
```

Next implementation target:

```text
sentence_tokens -> token_candidates + token_occurrences
```

Reason:

- token rows now preserve offsets and surface text
- candidate generation can now build contiguous spans safely
- every future candidate can link back to exact sentence context

Next step: Step 2, Candidate Inventory.
