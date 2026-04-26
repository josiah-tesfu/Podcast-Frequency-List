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
- show dispersion
- basic display-form selection
- basic candidate summary query

Output:

- inspectable top 1/2/3-grams

Step 3 should not be implemented as one broad pass.

The frequency and dispersion math is deterministic, but the stage touches
schema, migration, derived metric ownership, display-form policy, and reporting.
Those are separate enough that splitting keeps the work reviewable and prevents
premature scoring logic from leaking in.

Deterministic parts:

- `raw_frequency = COUNT(token_occurrences)`
- `episode_dispersion = COUNT(DISTINCT episode_id)`
- `show_dispersion = COUNT(DISTINCT show_id)`
- summary queries ordered by stored metrics
- DB validation queries

Policy-driven but still deterministic after selection:

- `display_text` selection from observed occurrence surfaces

Out of scope for Step 3:

- score weights
- threshold tuning
- association metrics
- boundary entropy
- redundancy suppression
- `candidate_scores`
- ranking beyond simple metric-sorted summaries

#### Step 3A: Metrics Schema

Add storage for factual candidate metrics.

Scope:

- add `episode_dispersion INTEGER NOT NULL DEFAULT 0` to `token_candidates`
- add `show_dispersion INTEGER NOT NULL DEFAULT 0` to `token_candidates`
- keep `raw_frequency` on `token_candidates`
- keep `display_text` on `token_candidates`
- bump `SCHEMA_VERSION`
- support existing DB migration with `ALTER TABLE` when columns are missing
- no scoring table
- no ranking logic
- no CLI command yet

Reason:

- metrics are facts derived directly from `token_occurrences`
- storing them beside `raw_frequency` avoids a parallel metrics abstraction
- later scoring can read stable candidate facts without recomputing counts

Validation:

- fresh DB contains both new columns
- existing schema version 9 DB upgrades cleanly
- `PRAGMA foreign_key_check` returns no rows
- existing candidate rows receive default zero values before refresh
- indexes still support inventory and frequency queries

Sanity checks:

- `token_candidates` still has one row per `(inventory_version, candidate_key)`
- no candidate rows are created or deleted by schema migration
- no occurrence rows are changed by schema migration

Tests:

- fresh DB schema test
- legacy DB migration test from v9 shape
- schema version test
- foreign-key integrity test

Exit criteria:

- schema-only change is green
- full test suite passes
- no metric refresh logic included yet

#### Step 3B: Metrics Refresh Service

Recompute stored candidate facts from occurrence evidence.

Scope:

- add a deterministic metrics refresh service in the `tokens` package
- refresh metrics for one `inventory_version`
- recompute `raw_frequency`
- recompute `episode_dispersion`
- recompute `show_dispersion`
- recompute `display_text`
- keep all updates in one transaction
- clean up orphan candidates if needed, consistent with Step 2 behavior
- no ranking scores
- no association metrics

Recommended code shape:

- add `CandidateMetricsService`
- add `CandidateMetricsError`
- add `CandidateMetricsResult`
- keep service near `tokens/inventory.py`
- reuse `INVENTORY_VERSION`
- keep SQL set-based where possible

Display-form policy:

1. choose the most frequent observed `surface_text` for the candidate
2. tie-break toward the form whose first character is lowercase
3. tie-break by earliest `occurrence_id`

Reason:

- Step 2 uses first-seen display text as a provisional value
- Step 3 can replace that with evidence-backed modal surface text
- capitalization from sentence starts should not dominate ties
- the policy is deterministic and easy to validate

Required metric definitions:

```sql
raw_frequency = COUNT(token_occurrences)
episode_dispersion = COUNT(DISTINCT token_occurrences.episode_id)
show_dispersion = COUNT(DISTINCT episodes.show_id)
```

Validation queries:

- raw frequency mismatch:

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

- episode dispersion mismatch:

```sql
SELECT COUNT(*) AS mismatch_count
FROM token_candidates cand
LEFT JOIN (
    SELECT candidate_id, inventory_version, COUNT(DISTINCT episode_id) AS episode_count
    FROM token_occurrences
    WHERE inventory_version = ?
    GROUP BY candidate_id, inventory_version
) occ
    ON occ.candidate_id = cand.candidate_id
    AND occ.inventory_version = cand.inventory_version
WHERE cand.inventory_version = ?
AND cand.episode_dispersion != COALESCE(occ.episode_count, 0);
```

- show dispersion mismatch:

```sql
SELECT COUNT(*) AS mismatch_count
FROM token_candidates cand
LEFT JOIN (
    SELECT
        occ.candidate_id,
        occ.inventory_version,
        COUNT(DISTINCT e.show_id) AS show_count
    FROM token_occurrences occ
    JOIN episodes e
        ON e.episode_id = occ.episode_id
    WHERE occ.inventory_version = ?
    GROUP BY occ.candidate_id, occ.inventory_version
) occ
    ON occ.candidate_id = cand.candidate_id
    AND occ.inventory_version = cand.inventory_version
WHERE cand.inventory_version = ?
AND cand.show_dispersion != COALESCE(occ.show_count, 0);
```

- display text must come from occurrence evidence:

```sql
SELECT COUNT(*) AS mismatch_count
FROM token_candidates cand
WHERE cand.inventory_version = ?
AND NOT EXISTS (
    SELECT 1
    FROM token_occurrences occ
    WHERE occ.candidate_id = cand.candidate_id
    AND occ.inventory_version = cand.inventory_version
    AND occ.surface_text = cand.display_text
);
```

Sanity checks:

- top candidates do not change frequency after refresh
- `C'est` can settle to `c'est` if lowercase evidence is tied or stronger
- candidates like `en fait`, `du coup`, `tu vois`, `il y a` have expected dispersion
- single-episode topic phrases show low episode dispersion even with high frequency

Tests:

- raw frequency recomputation
- episode dispersion recomputation
- show dispersion recomputation
- display text modal selection
- lowercase tie-break
- idempotent refresh
- orphan cleanup remains scoped to current inventory version

Exit criteria:

- all mismatch queries return zero
- refresh is deterministic across repeated runs
- full test suite passes

#### Step 3C: Candidate Summary Query

Add a simple inspection surface for stored metrics.

Scope:

- provide an internal summary query
- support filtering by `ngram_size`
- order by `raw_frequency DESC`, then `episode_dispersion DESC`, then `candidate_key`
- include candidate key, display text, n-gram size, raw frequency, episode dispersion, and show dispersion
- no scoring columns
- no rank table

CLI decision:

- optional in Step 3
- docs-only SQL is enough if the immediate need is inspection
- add a CLI only if repeated operator workflow needs it

Recommended query:

```sql
SELECT
    candidate_key,
    display_text,
    ngram_size,
    raw_frequency,
    episode_dispersion,
    show_dispersion
FROM token_candidates
WHERE inventory_version = ?
AND ngram_size = ?
ORDER BY raw_frequency DESC, episode_dispersion DESC, candidate_key
LIMIT ?;
```

Validation:

- top 1/2/3-gram queries return rows
- ordering is stable
- query works after a fresh metrics refresh
- empty n-gram buckets return no rows without error

Sanity checks:

- inspect top 1-grams
- inspect top 2-grams
- inspect top 3-grams
- confirm high-frequency chunks remain visible
- confirm low-dispersion topic terms are easy to spot

Tests:

- summary query ordering
- n-gram filtering
- limit handling
- empty result handling

Exit criteria:

- candidate metrics are inspectable without ad hoc SQL edits
- no ranking behavior implied by the summary

#### Step 3D: Pilot Metrics Inspection

Run Step 3 on the current pilot DB and inspect output.

Required commands:

```text
uv run podfreq generate-candidates --pilot zack-10h-pilot
uv run ruff check src tests
uv run pytest
```

Additional command depends on Step 3C decision:

```text
uv run podfreq refresh-candidate-metrics
```

or a documented SQL/scripted inspection query.

DB validation:

- raw frequency mismatch count is zero
- episode dispersion mismatch count is zero
- show dispersion mismatch count is zero
- display text mismatch count is zero
- `PRAGMA foreign_key_check` returns no rows
- candidate count does not change except for legitimate orphan cleanup

Sanity checks:

- top 1-grams are plausible
- top 2-grams are plausible
- top 3-grams are plausible
- `en fait`, `du coup`, `je pense`, `il y a`, `tu vois` remain visible
- display text no longer over-preserves sentence-start capitalization
- high-frequency but narrow phrases can be identified by low dispersion

Tests:

- unit and integration tests from 3A-3C remain green
- pilot refresh does not require network
- repeated pilot refresh is deterministic

Exit criteria:

- stored metrics match occurrence evidence
- summary output is useful for manual inspection
- Step 4 can build association and boundary features from a stable candidate set

### Step 4: Association And Boundary Features

Add unithood metrics for multiword spans.

Deliverables:

- t-score
- normalized PMI
- left/right distinct context counts
- left/right entropy

Output:

- better signal for true chunks vs glue phrases

Step 4 should not be implemented as one broad pass.

The underlying counts are deterministic, but the stage touches metric contract,
nullable semantics, context definitions, association formulas, and inspection
surface shape. Splitting keeps the math reviewable and avoids premature ranking
policy leaking into a factual metrics stage.

Recommended Step 4 stance:

- treat Step 4 as stored factual unithood metrics, not ranking
- compute Step 4 only for multiword candidates (`ngram_size >= 2`)
- keep 1-gram Step 4 fields `NULL`
- store Step 4 metrics on `token_candidates`
- use immediate adjacent `token_key` context, not `surface_text`
- include sentence-boundary sentinels in context distributions
- compute trigram association by weakest internal split
- do not add score weights, thresholds, or pruning in Step 4

Deterministic parts:

- immediate left/right neighbor extraction from `sentence_tokens`
- sentence-boundary sentinel handling once selected
- distinct context counts
- entropy math
- association refresh for a fixed formula contract
- repeated refresh and validation queries

Policy-driven but still deterministic after selection:

- store metrics on `token_candidates` vs a separate table
- use `NULL` for Step 4 fields on 1-grams
- use distinct context counts rather than redundant total context counts
- shared normalizer for association formulas
- weakest-internal-split aggregation for 3-grams
- extend existing inspection output rather than creating a ranking surface

Out of scope for Step 4:

- final ranking weights
- minimum-score pruning
- `candidate_scores`
- redundancy suppression
- example selection
- proper-noun or function-word penalties
- language-specific manual chunk whitelists

#### Step 4A: Unithood Metric Contract And Schema

Define the factual Step 4 metric contract and add storage.

Scope:

- add nullable Step 4 columns to `token_candidates`
- keep Step 4 metrics on `token_candidates`, not a new metrics table
- support existing DB migration
- define which candidates receive Step 4 metrics
- define exact field semantics before refresh logic
- no ranking weights
- no CLI behavior change yet

Recommended stored fields:

- `t_score REAL`
- `npmi REAL`
- `left_context_type_count INTEGER`
- `right_context_type_count INTEGER`
- `left_entropy REAL`
- `right_entropy REAL`

Recommended semantics:

- Step 4 fields are populated only for `ngram_size >= 2`
- Step 4 fields remain `NULL` for 1-grams
- `left_context_type_count` and `right_context_type_count` count distinct
  adjacent context token keys, including boundary sentinels
- no separate total left/right context count column because total context count
  is redundant with `raw_frequency` once boundary sentinels are included

Reason:

- Step 3 already stores factual candidate metrics on `token_candidates`
- Step 4 is still about candidate facts, not final ranking policy
- keeping Step 4 facts beside Step 3 facts avoids a parallel storage layer
- nullable fields keep 1-gram semantics clean without inventing fake zeroes

Validation:

- fresh DB contains all Step 4 columns
- existing DB migrates cleanly
- 1-gram rows default to `NULL` in Step 4 columns
- existing Step 3 metrics remain untouched by schema migration
- foreign-key integrity remains clean

Sanity checks:

- no candidate or occurrence rows are created or deleted by migration
- Step 4 schema can coexist with Step 3 summary queries
- current Step 3 refresh still works before Step 4 refresh is added

Tests:

- fresh schema test
- migration test from Step 3 schema
- schema version test
- nullable-field contract test for 1-grams

Exit criteria:

- Step 4 storage contract is explicit
- migration is green
- no refresh logic included yet

#### Step 4B: Boundary Context Metrics

Compute boundary-context facts from stored occurrences and sentence tokens.

Scope:

- derive immediate left and right neighbor `token_key` for each occurrence
- treat sentence boundaries as explicit sentinel contexts
- aggregate distinct left/right context counts
- aggregate left/right entropy
- keep computation set-based and scoped by `inventory_version`
- no association metrics yet
- no ranking behavior

Recommended context policy:

- left context = token whose `token_index = token_start_index - 1`
- right context = token whose `token_index = token_end_index`
- when no such token exists, use deterministic sentinels:
  - `__BOS__`
  - `__EOS__`
- use analysis `token_key`, not `surface_text`

Recommended entropy definition:

```text
entropy = -SUM(p_i * LN(p_i))
```

where `p_i` is the probability of each observed adjacent context value for the
candidate side being measured.

Reason:

- immediate adjacency matches the project goal of boundary strength
- `token_key` keeps the metric portable and less noisy than surface strings
- boundary sentinels prevent sentence-edge occurrences from disappearing
- distinct counts plus entropy give both inspectability and actual signal

Validation:

- every multiword occurrence contributes one left context and one right context
- left/right distinct context counts are nonzero for multiword candidates
- entropy is zero when one side always appears in the same context
- entropy increases when context diversity increases

Sanity checks:

- `il y a` can show limited but nonzero context diversity
- glue phrases like `de la` or `que je` should often show broader context spread
- clause-edge chunks are not penalized for missing neighbors

Tests:

- left-context extraction
- right-context extraction
- boundary sentinel handling
- distinct-count aggregation
- zero-entropy fixed-context case
- higher-entropy varied-context case
- deterministic repeated refresh

Exit criteria:

- stored boundary metrics match occurrence evidence
- repeated refresh is deterministic
- no association math included yet

#### Step 4C: Association Metrics

Compute multiword cohesion metrics from stored candidate frequencies.

Scope:

- compute `t_score`
- compute `npmi`
- support bigrams and trigrams
- use stored candidate frequencies from Step 3
- use a deterministic split policy for 3-grams
- no ranking weights
- no pruning thresholds

Recommended association contract:

- compute association only for `ngram_size >= 2`
- use the total unigram occurrence count for the current `inventory_version`
  as the shared corpus normalizer
- compute bigram association directly
- compute trigram association on each internal binary split and keep the weaker
  value for both `t_score` and `npmi`

Recommended formulas:

For a candidate split into `left_part` and `right_part`:

```text
observed = freq(candidate)
left_freq = freq(left_part)
right_freq = freq(right_part)
N = total unigram occurrence count for inventory_version

expected = (left_freq * right_freq) / N
t_score = (observed - expected) / sqrt(observed)
npmi = ln((observed * N) / (left_freq * right_freq)) / -ln(observed / N)
```

For trigrams:

```text
candidate_metric = min(metric(split_1), metric(split_2))
```

where `split_1` and `split_2` are the two internal binary splits.

Reason:

- `t_score` favors common useful chunks
- `npmi` captures cohesion
- using both follows the design goal of avoiding raw-frequency-only and
  PMI-only behavior
- weakest-split aggregation better matches the unithood question for 3-grams
- shared unigram normalizer keeps the contract simple and portable

Validation:

- bigram metrics match hand-worked toy examples
- trigram metrics use the weaker internal split
- higher-frequency cohesive chunks outperform obvious glue phrases on at least
  one association signal
- 1-gram Step 4 fields remain `NULL`

Sanity checks:

- `en fait`, `du coup`, `il y a`, `je pense` show materially stronger
  association than `de la`, `que je`, `et le`
- rare phrases may have strong `npmi`, but `t_score` stays modest
- glue phrases can still be frequent while showing weaker cohesion

Tests:

- bigram `t_score` formula
- bigram `npmi` formula
- trigram weakest-split behavior
- 1-gram null semantics
- deterministic repeated refresh

Exit criteria:

- Step 4 stores factual association metrics for multiword candidates
- formulas are documented and test-backed
- no score weighting or pruning included yet

#### Step 4D: Inspection Surface

Expose Step 4 metrics for review without turning Step 4 into ranking.

Scope:

- extend existing candidate metrics inspection surface
- include Step 4 fields in focused candidate inspection
- include Step 4 fields in top 2/3-gram summaries
- keep existing Step 3 frequency-first ordering unless there is a strong reason
  to change it later in Step 6
- no `candidate_scores`
- no final ranking report yet

Recommended inspection stance:

- keep current top-candidate lists frequency-first for continuity
- show Step 4 metrics alongside Step 3 metrics
- use focused candidate inspection for side-by-side comparisons such as:
  - `en fait` vs `de la`
  - `du coup` vs `que je`
  - `je pense` vs `et le`

Reason:

- Step 4 is a factual metric stage, not ranking
- keeping ordering stable avoids hiding Step 4 problems behind early heuristics
- the existing inspect command already provides the right operator workflow

Validation:

- Step 4 values appear in inspection output
- top 2/3-gram inspection remains usable
- focused candidate inspection can compare strong chunks and weak glue phrases

Tests:

- summary row/report shape test
- focused candidate inspection output test
- no regression in existing Step 3 inspection behavior

Exit criteria:

- Step 4 metrics are inspectable without ad hoc SQL
- no ranking behavior is implied by the inspection surface

#### Step 4E: Pilot Validation

Run Step 4 refresh and inspect pilot-scale behavior.

Required commands:

```text
uv run podfreq refresh-candidate-metrics
uv run podfreq inspect-candidate-metrics --limit 10
uv run ruff check src tests
uv run pytest
```

DB validation:

- Step 3 mismatch counts remain zero
- 1-gram Step 4 fields remain `NULL`
- multiword Step 4 fields populate for candidates with occurrences
- repeated refresh remains deterministic
- foreign-key integrity remains clean

Sanity checks:

- `en fait`, `du coup`, `il y a`, `je pense`, `tu vois` are still visible
- those chunks show stronger cohesion or tighter boundary behavior than obvious
  glue phrases
- high-frequency glue phrases remain visible for inspection rather than being
  silently pruned
- boundary sentinels do not create strange missing-value patterns

Tests:

- unit and integration tests from 4A-4D remain green
- pilot refresh does not require network
- repeated pilot refresh is deterministic

Exit criteria:

- Step 4 metrics are stored, inspectable, and pilot-validated
- Step 5 can compare smaller candidates against larger candidates using a stable
  unithood base

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

Step 3A through 3D are implemented through metrics schema, deterministic metrics
refresh, candidate inspection commands, and pilot-scale validation.

Step 4 should now be implemented as explicit substeps 4A through 4E.

Current completed target:

```text
token_occurrences -> inspectable stored frequency and dispersion metrics
```

Next implementation target:

```text
stable candidate facts -> stored unithood metrics for multiword candidates
```

Reason:

- candidate facts can now be refreshed and inspected from occurrence evidence
- raw frequency, episode dispersion, show dispersion, and display text can be validated directly
- top 1/2/3-gram summaries can be inspected without ad hoc SQL
- Step 4 needs an explicit contract for null semantics, boundary contexts, and
  association formulas before refresh logic is added
- splitting Step 4 prevents factual metrics from getting mixed with Step 6
  ranking policy

Next step: Step 4A, Unithood Metric Contract And Schema.
