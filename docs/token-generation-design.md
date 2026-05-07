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

- `en fait`, `du coup`, `il y a`, `je pense`, `tu vois` are still visible in
  focused inspection
- contraction-heavy chunks such as `c'est`, `j'ai`, `n'est pas`, and
  `c'est un` still receive populated association metrics rather than unexpected
  `NULL`s
- number-heavy spans such as `100 000 clients` or similar multiword numeric
  chunks still receive populated association metrics when their internal split
  parts are valid sentence spans
- strong spoken chunks such as `du coup`, `en fait`, `il y a`, `je pense`, and
  `tu vois` show materially stronger cohesion than obvious glue phrases such as
  `de la`, `que je`, and `et le`
- rare but cohesive chunks can still show strong `npmi` even when `t_score`
  stays more modest
- high-frequency glue phrases remain visible for inspection rather than being
  silently pruned or hidden by the reporting surface
- top `1`-gram inspection stays frequency-and-dispersion only, with no fake
  Step 4 values introduced for `1`-grams
- top `2`-gram and `3`-gram inspection rows expose both association and
  boundary metrics alongside the existing Step 3 facts
- focused candidate inspection shows the same stored Step 4 values as the DB
  for manually requested keys
- `1`-gram Step 4 fields remain `NULL` after refresh
- multiword candidates with occurrence evidence do not retain `NULL` Step 4
  fields after refresh
- clause-edge chunks do not lose boundary metrics just because one side uses
  `__BOS__` or `__EOS__`
- boundary sentinels do not create strange missing-value patterns or collapse
  otherwise valid context distributions
- repeated refreshes leave candidate counts, mismatch counts, and Step 4 values
  unchanged apart from legitimate `display_text` normalization on the first run
- Step 4 inspection remains factual only: no new ranking surface, no pruning,
  and no candidate disappearance caused by the refresh itself

Tests:

- unit and integration tests from 4A-4D remain green
- pilot refresh does not require network
- repeated pilot refresh is deterministic

Exit criteria:

- Step 4 metrics are stored, inspectable, and pilot-validated
- Step 5 can compare smaller candidates against larger candidates using a stable
  unithood base

### Step 5: Redundancy And Coverage

Detect when a smaller candidate is mostly the same thing as a better larger
candidate.

Deliverables:

- direct-parent containment facts
- candidate-level redundancy summaries
- inspection surface for coverage and dominance

Output:

- better choices between `me dis` and `je me dis`
- better choices between `en tout` and `en tout cas`
- without collapsing productive chunks such as `du coup` or `en fait`

Step 5 should not be implemented as one broad pass.

The containment joins are deterministic, but the stage defines what counts as
coverage, which larger candidates matter, how to avoid chain double-counting,
and where ranking policy begins. Splitting keeps pairwise containment facts
reviewable and prevents Step 6 scoring policy from leaking into the storage
contract.

Recommended Step 5 stance:

- keep overlapping candidates in inventory
- compare only direct parents:
  - `1`-gram -> `2`-gram
  - `2`-gram -> `3`-gram
- measure coverage from distinct smaller occurrences, not raw overlap rows
- store pairwise containment facts in a new `candidate_containment` table
- derive candidate-level summaries by aggregating stored pair facts
- keep `covered by any larger candidate` and `dominated by one larger
  candidate` as separate questions
- reuse the existing refresh and inspection commands rather than adding a
  parallel Step 5 command surface
- keep numeric redundancy penalties and final ranking weights out of Step 5

Deterministic parts:

- direct-parent containment joins from `token_occurrences`
- extension-side classification from token offsets
- distinct covered occurrence counts
- distinct covered episode counts
- candidate-level summary aggregation from stored pair facts
- repeated refresh and validation queries

Policy-driven but still deterministic after selection:

- direct-parent-only comparison vs all-larger comparison
- pair table vs candidate-only storage
- exact dominant-parent tie-break rules
- which candidate-level summary fields to expose in inspection
- whether Step 5 stops at facts or also stores a provisional penalty

Out of scope for Step 5:

- `candidate_scores`
- final score weights
- hard pruning or candidate deletion
- keep/reject decisions
- blacklist hooks
- example selection
- final ranked candidate report

#### Step 5A: Direct-Parent Containment Contract And Schema

Define the factual containment contract and add storage.

Scope:

- add a new `candidate_containment` table
- define which smaller/larger pairs can appear
- define direct-parent semantics before refresh logic
- support DB migration
- add only the indexes needed for Step 5 refresh and inspection
- no candidate-level penalty yet
- no CLI behavior change yet

Recommended stored fields:

- `smaller_candidate_id INTEGER NOT NULL`
- `larger_candidate_id INTEGER NOT NULL`
- `inventory_version TEXT NOT NULL`
- `extension_side TEXT NOT NULL`
- `shared_occurrence_count INTEGER NOT NULL`
- `shared_episode_count INTEGER NOT NULL`

Recommended semantics:

- one row per `(inventory_version, smaller_candidate_id, larger_candidate_id)`
- only store direct-parent pairs where the larger candidate is exactly one token
  longer than the smaller candidate
- only store pairs with `shared_occurrence_count > 0`
- `extension_side = 'left'` when the larger candidate begins one token earlier
- `extension_side = 'right'` when the larger candidate ends one token later
- `extension_side = 'both'` when the same smaller/larger pair is attested from
  both sides across occurrences, which can happen with repeated-token parents
  such as `de -> de de`
- under the current `1`/`2`/`3`-gram pipeline, Step 5 pair rows can only point
  from `1`-grams to `2`-grams or from `2`-grams to `3`-grams
- do not add Step 5 coverage columns to `token_candidates` yet

Reason:

- containment is inherently relational and does not fit cleanly on a single
  candidate row
- direct-parent storage avoids chain inflation while preserving the actual
  redundancy choice the ranking stage needs
- counts stay factual and ratios remain derivable from existing
  `raw_frequency`

Validation:

- fresh DB contains `candidate_containment`
- existing DB migrates cleanly
- foreign keys and uniqueness constraints are enforced
- no Step 4 candidate metrics are changed by migration

Sanity checks:

- `3`-grams can appear as `larger_candidate_id`
- under the current max `ngram_size`, `3`-grams do not appear as covered
  smaller candidates
- Step 5 schema coexists with the current `refresh-candidate-metrics` workflow

Tests:

- fresh schema test
- migration test from Step 4 schema
- schema version test
- foreign-key and uniqueness contract test

Exit criteria:

- Step 5 storage contract is explicit
- migration is green
- no containment refresh logic included yet

#### Step 5B: Direct-Parent Containment Refresh

Compute pairwise containment facts from stored occurrences.

Scope:

- derive direct-parent containment from `token_occurrences`
- classify each pair as a left-extension or right-extension relationship
- aggregate `shared_occurrence_count`
- aggregate `shared_episode_count`
- keep computation set-based and scoped by `inventory_version`
- integrate refresh into the existing metrics workflow
- no candidate-level penalty yet
- no ranking behavior

Recommended containment contract:

- a pair qualifies when a larger occurrence strictly contains a smaller
  occurrence in the same sentence
- the larger span must be exactly one token longer than the smaller span
- count distinct smaller `occurrence_id` values per pair
- count distinct `episode_id` values per pair
- refresh should replace current `inventory_version` pair rows deterministically
- do not aggregate all larger candidates into one Step 5 fact row yet

Reason:

- distinct smaller-occurrence counting avoids overlap inflation in repeated-token
  or nested-span cases
- direct-parent restriction captures the real redundancy choice while avoiding
  transitive chain noise
- episode counts add portability beyond one-sentence or one-episode bursts

Validation:

- every Step 5 pair row has `larger.ngram_size = smaller.ngram_size + 1`
- `shared_occurrence_count` is less than or equal to the smaller candidate
  `raw_frequency`
- `shared_occurrence_count > larger.raw_frequency` is allowed only for
  repeated-token parents that collapse to `extension_side = 'both'`
- `extension_side` matches token-offset evidence
- repeated-token direct parents collapse to one pair row with
  `extension_side = 'both'`
- repeated refresh is deterministic

Sanity checks:

- `pense que -> je pense que` reaches full direct-parent coverage
- `me dis -> je me dis` shows a strong dominant-parent share
- productive chunks such as `du coup` and `en fait` can show high
  any-coverage while keeping much lower best-parent share
- `dirais` is not falsely treated as fully dominated if no single parent covers
  all occurrences

Tests:

- direct-parent join extraction
- left-extension classification
- right-extension classification
- distinct occurrence aggregation
- distinct episode aggregation
- deterministic repeated refresh
- no rows for zero-overlap pairs

Exit criteria:

- pair facts are stored and deterministic
- no candidate-level ranking penalty is included yet

#### Step 5C: Candidate-Level Coverage Summaries And Inspection

Expose Step 5 facts for review without turning them into final ranking.

Scope:

- aggregate `candidate_containment` into candidate-level summaries
- extend focused candidate inspection with Step 5 coverage fields
- extend top `1`-gram and `2`-gram summaries with Step 5 coverage fields
- keep top-list ordering frequency-first until Step 6 chooses ranking weights
- no `candidate_scores`
- no hard suppression yet

Recommended summary fields:

- `covered_by_any_count`
- `covered_by_any_ratio`
- `independent_occurrence_count`
- `direct_parent_count`
- `dominant_parent_key`
- `dominant_parent_shared_count`
- `dominant_parent_share`
- `dominant_parent_side`

Recommended summary semantics:

- `covered_by_any_count` counts distinct smaller occurrences covered by at least
  one direct parent
- `independent_occurrence_count = raw_frequency - covered_by_any_count`
- `direct_parent_count` counts direct-parent types with
  `shared_occurrence_count > 0`
- `dominant_parent_key` is the direct parent with the largest
  `shared_occurrence_count`
- dominant-parent ties break by:
  - higher parent `raw_frequency`
  - then lexical `candidate_key`
- `dominant_parent_share = dominant_parent_shared_count / raw_frequency`
- `dominant_parent_side` mirrors the stored pair side and may be `left`,
  `right`, or `both`
- Step 5 summaries are meaningful only for candidates that can have a direct
  parent under the current `max_n`

Reason:

- coverage by any larger candidate alone over-penalizes productive chunks
- dominant-parent share plus residual count better captures the redundancy
  question
- keeping Step 5 inspectable before Step 6 helps prevent premature ranking
  heuristics

Validation:

- candidate-level Step 5 summaries match `candidate_containment` rows
- dominant-parent selection is deterministic
- top lists remain readable and frequency-first
- focused inspection can compare clear redundancy cases and clear
  non-redundancy cases

Sanity checks:

- `pense que`, `en tout`, `tout cas`, and `ailleurs` show near-total
  dominant-parent coverage
- `du coup`, `en fait`, `je pense`, and `se passe` show lower dominant-parent
  share despite broad any-coverage
- `3`-grams show no fake covered-by-parent summaries under the current
  `1`/`2`/`3`-gram limit

Tests:

- pair-to-summary aggregation test
- dominant-parent tie-break test
- candidate inspection output test
- no regression in existing Step 4 inspection behavior

Exit criteria:

- containment facts are inspectable without ad hoc SQL
- coverage and dominance are visible as separate ideas
- no final score weighting is implied by the inspection surface

#### Step 5D: Pilot Validation

Run Step 5 refresh and inspect pilot-scale behavior.

Required commands:

```text
uv run podfreq refresh-candidate-metrics
uv run podfreq inspect-candidate-metrics --limit 10
uv run ruff check src tests
uv run pytest
```

DB validation:

- Step 4 mismatch counts remain zero
- `candidate_containment` rows only connect `1 -> 2` and `2 -> 3`
- `shared_occurrence_count` never exceeds the smaller candidate
  `raw_frequency`
- rows where `shared_occurrence_count > larger.raw_frequency` are limited to
  repeated-token parents with `extension_side = 'both'`
- candidate-level Step 5 summaries stay consistent with `raw_frequency`
- repeated refresh remains deterministic
- foreign-key integrity remains clean

Sanity checks:

- `me dis` and `pense que` look strongly dominated by one direct parent
- `en tout` and `tout cas` look strongly dominated by `en tout cas`
- `du coup`, `en fait`, and `je pense` remain visible as productive chunks even
  when they are covered by many direct parents
- `envie` shows meaningful containment structure without collapsing into one
  obvious dominant parent
- repeated-token pairs such as `de -> de de` collapse to one factual pair row
  with `extension_side = 'both'`
- top `3`-gram inspection does not invent fake Step 5 covered-by-parent values
- repeated refreshes leave pair counts and candidate-level Step 5 summaries
  unchanged
- Step 5 inspection remains factual only: no new pruning and no candidate
  disappearance caused by the refresh itself

Tests:

- unit and integration tests from 5A-5C remain green
- pilot refresh does not require network
- repeated pilot refresh is deterministic

Exit criteria:

- Step 5 containment facts are stored, inspectable, and pilot-validated
- Step 6 can apply a ranking penalty using stable Step 5 facts instead of
  ad hoc overlap guesses

### Step 6: Candidate Ranking V1

Combine stored candidate facts into a first ranked review/deck surface.

Deliverables:

- `candidate_scores` table
- score versioning and lane semantics
- support gates that cut the sparse tail before scoring
- lane-specific score formulas for `1`/`2`/`3`-gram candidates
- conservative redundancy penalty from Step 5
- lane-aware top candidate report and global review surface

Output:

- ranked review list that includes function words, glue phrases, and stronger
  chunks while preserving the natural score progression in the pilot

Step 6 should not be implemented as one broad pass.

This is the most policy-heavy stage in the pipeline. The stored metrics are
deterministic, but ranking still has to decide how to handle sparse tails, how
much raw frequency should matter after `t_score`, how to keep useful
function-word and glue material visible, and how Step 5 should affect ranking
without crushing productive candidates. Splitting keeps the score contract
inspectable and prevents review-surface policy from being hidden inside one
large refresh.

Recommended Step 6 stance:

- keep `1`/`2`/`3`-gram candidates in scope; do not hard blacklist glue phrases
  or function words in v1
- store scores in one `candidate_scores` table, but compute separate ranking
  lanes for `1gram`, `2gram`, and `3gram`
- gate candidates before normalization and scoring
- store both eligible and ineligible candidates so support-gate behavior stays
  inspectable
- cut the sparse tail with absolute support floors in the pilot
- compute normalization only inside the eligible lane, never over the full raw
  inventory
- ignore `show_dispersion` in the current pilot because it is constant
- treat support as necessary but secondary after gating
- let association carry most of the multiword ranking signal, with boundary as
  a secondary signal
- use Step 5 only as a conservative direct-parent penalty; never use
  `covered_by_any` as a general penalty
- keep `1`-gram redundancy impact off or very mild in v1
- keep lane-first inspection available as the primary review surface in `v1`
- allow a global review list to reflect the natural score ordering, even if it
  begins `1gram`-heavy
- keep score components and lane ranks inspectable and versioned
- defer proper-noun penalties, blacklist hooks, and semantic filters to later
  review/tuning work

Deterministic parts:

- lane assignment
- eligibility filtering from stored metrics
- within-lane normalization
- component score computation
- final score computation once weights are selected
- lane rank computation
- review/report generation from stored scores
- repeated reruns

Policy-driven but deterministic after selection:

- exact support floors
- whether to score all candidates or also store ineligible rows
- exact component weights
- whether the `1gram` lane gets any Step 5 penalty in v1
- whether to expose lane-first sections only or also a global view
- whether the eventual human-facing export should preserve the natural global
  progression or add a later composition layer outside Step `6`

Out of scope for Step 6:

- example selection
- blacklist hooks
- proper-noun or named-entity classifiers
- manual keep/reject annotations
- semantic clustering
- final export format
- cross-corpus tuning

#### Step 6A: Ranking Contract And Schema

Define score storage and versioning before implementing policy math.

Scope:

- add a new `candidate_scores` table
- define score versioning and lane semantics
- support DB migration
- add only the indexes needed for refresh and inspection
- no scoring math yet
- no ranking CLI change yet

Recommended stored fields:

- `candidate_id INTEGER NOT NULL`
- `inventory_version TEXT NOT NULL`
- `score_version TEXT NOT NULL`
- `ranking_lane TEXT NOT NULL`
- `is_eligible INTEGER NOT NULL`
- `frequency_score REAL`
- `dispersion_score REAL`
- `association_score REAL`
- `boundary_score REAL`
- `redundancy_penalty REAL`
- `final_score REAL`
- `lane_rank INTEGER`
- `created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP`

Recommended semantics:

- one row per `(inventory_version, score_version, candidate_id)`
- `ranking_lane` is one of `1gram`, `2gram`, `3gram`
- store all candidates for the current inventory version so ineligible rows
  remain inspectable
- ineligible rows keep `is_eligible = 0`, `lane_rank = NULL`, and
  `final_score = NULL`
- `frequency_score` and `dispersion_score` exist for all lanes
- `association_score` and `boundary_score` are `NULL` for `1`-grams in v1
- `redundancy_penalty` is `0` or very mild for `1`-grams in v1
- `lane_rank` orders only eligible rows within `(score_version, ranking_lane)`

Reason:

- Step 6 needs versioned output because score policy will change
- storing ineligible rows keeps support-gate decisions visible
- lane-specific null semantics avoid fake comparability between `1`-grams and
  multiword candidates

Validation:

- fresh DB contains `candidate_scores`
- migration is green
- uniqueness and foreign keys are enforced
- Step 4 and Step 5 data remain unchanged

Sanity checks:

- `candidate_scores` can store `1`-gram, `2`-gram, and `3`-gram rows together
- lane-specific null semantics are explicit
- schema supports multiple score versions for the same candidate

Tests:

- fresh schema test
- migration test from Step 5 schema
- uniqueness and foreign-key test
- lane/null semantics test

Exit criteria:

- score storage contract is explicit
- migration is green
- no ranking math included yet

#### Step 6B: Eligibility Gates And Lane Assignment

Cut the sparse tail before normalization or scoring.

Scope:

- assign ranking lanes from `ngram_size`
- define pilot support floors
- mark eligible vs ineligible candidates
- keep support-gate behavior inspectable
- no weighted scoring yet
- no global review surface yet

Recommended pilot lanes:

- `1gram`: single-token candidates
- `2gram`: two-token candidates
- `3gram`: three-token candidates

Recommended pilot support floors:

- `1gram`: `raw_frequency >= 20` and `episode_dispersion >= 5`
- `2gram`: `raw_frequency >= 10` and `episode_dispersion >= 3`
- `3gram`: `raw_frequency >= 10` and `episode_dispersion >= 3`

Recommended semantics:

- support floors apply before any percentile or rank normalization
- support floors are lane-specific but category-agnostic: no glue/function-word
  blacklist
- store all candidates, but set `is_eligible = 0` when a lane floor fails
- ignore `show_dispersion` in pilot v1 because the current pilot has only one
  show

Reason:

- the raw inventory tail is too sparse for stable whole-inventory scoring
- pilot data showed that modest raw counts already land at extreme percentiles
  if the tail is not cut first
- per-lane floors preserve useful function words and glue phrases while
  removing most singletons and doubletons

Current pilot reference counts:

- `1gram`: about `205` candidates at `freq >= 20`, `episode_dispersion >= 5`
- `2gram`: about `453` candidates at `freq >= 10`, `episode_dispersion >= 3`
- `3gram`: about `146` candidates at `freq >= 10`, `episode_dispersion >= 3`

Validation:

- eligible counts are deterministic for a fixed pilot snapshot
- ineligible rows remain queryable
- no score normalization is performed on ineligible tails

Sanity checks:

- common function words remain eligible in the `1gram` lane
- frequent glue phrases remain eligible in the `2gram` and `3gram` lanes
- low-support long-tail singletons/doubletons are filtered before scoring
- gates reduce the pilot review pool to a manageable scale without hand-written
  category rules

Tests:

- lane assignment test
- eligibility flag test
- deterministic count test on a fixture corpus
- ineligible rows keep null final scores

Exit criteria:

- support-gated candidate pool is explicit
- sparse-tail distortion is removed before score normalization

#### Step 6C: Lane-Specific Components And Final Score

Compute inspectable component scores inside the eligible pool.

Scope:

- compute normalized support, association, and boundary components
- compute conservative Step 5 penalty
- produce final lane-specific scores
- keep formulas inspectable and linear
- no review-surface composition yet

Recommended component semantics:

- `frequency_score`: normalized `log1p(raw_frequency)` within the eligible lane
- `dispersion_score`: normalized `episode_dispersion` within the eligible lane
- `association_score`:
  - `2`/`3`-gram only
  - blend `npmi` and `t_score`
  - weight `npmi` more heavily than `t_score` because `t_score` already tracks
    frequency strongly in the pilot
- `boundary_score`:
  - `2`/`3`-gram only
  - normalize weaker-side entropy: `min(left_entropy, right_entropy)`
- `redundancy_penalty`:
  - `2`-gram only in pilot v1
  - based on `dominant_parent_share`
  - zero below a high threshold such as `0.80`
  - ramps upward above that threshold
  - never uses `covered_by_any_ratio`
- `1`-gram lane:
  - support-only in v1
  - `association_score` and `boundary_score` remain `NULL`
  - `redundancy_penalty` stays `0` in v1

Recommended pilot formulas:

- `1gram final_score = 0.65 * frequency_score + 0.35 * dispersion_score`
- `2gram support_score = 0.60 * frequency_score + 0.40 * dispersion_score`
- `2gram association_score = 0.65 * npmi_norm + 0.35 * t_score_norm`
- `2gram final_score = 0.20 * support_score + 0.50 * association_score + 0.20 * boundary_score - 0.10 * redundancy_penalty`
- `3gram support_score = 0.55 * frequency_score + 0.45 * dispersion_score`
- `3gram association_score = 0.65 * npmi_norm + 0.35 * t_score_norm`
- `3gram final_score = 0.20 * support_score + 0.55 * association_score + 0.25 * boundary_score`

Reason:

- pilot data showed that raw frequency and `t_score` are strongly correlated, so
  support should not dominate again after gating
- pilot data also showed that `npmi` is the clearest signal separating cohesive
  spoken units from weak high-frequency fragments
- boundary entropy helps once low-support tails are removed, but it is not
  strong enough to stand alone
- Step 5 is useful as a guardrail against obvious direct-parent fragments, but
  only when applied conservatively

Validation:

- repeated scoring is deterministic
- in-lane normalization does not look outside the eligible pool
- `1gram` lane keeps `association_score` and `boundary_score` null
- `3gram` lane keeps `redundancy_penalty = 0`
- score-version reruns replace previous rows cleanly

Sanity checks:

- `du coup`, `en fait`, `je pense`, `il y a`, `je me dis`, and `j ai envie`
  land near the top of their lanes
- obvious direct-parent fragments such as `pense que`, `me dis`, `en tout`,
  and `tout cas` lose rank in the `2gram` lane
- useful glue/function-word material can still rank if it has enough support
- weak fragments such as `et le`, `est que`, `c est que`, and `c est pas` do
  not rise simply from frequency

Tests:

- support normalization test
- association blend test
- weakest-side boundary test
- high-share redundancy penalty test
- no-use-of-covered-by-any penalty test
- `1gram` / `2gram` / `3gram` null-semantics test
- deterministic rerun test

Exit criteria:

- final scores are stored, inspectable, and versioned
- the first ranking remains linear and auditable

#### Step 6D: Review Surface

Turn stored lane scores into a practical human-facing pilot review surface.

This stage should not ship as one opaque pass.

Reason:

- `6C` already computes and stores `lane_rank`, so `6D` is no longer a scoring
  step
- the remaining work is split across two different concerns:
  - `6D1`: read-only inspection of scored rows
  - `6D2`: optional global review surface over all eligible rows
- the first concern is straightforward and mostly mechanical
- the second concern is still policy-sensitive because it decides whether the
  repo should expose the natural cross-lane score progression directly or keep
  review lane-local only

Live pilot clarification:

- a flat global top list is not a neutral presentation
- with current `pilot-v1` scores, the global top `25` is `23` `1gram`, `1`
  `2gram`, `1` `3gram`
- the global top `100` is `81` `1gram`, `12` `2gram`, `7` `3gram`
- the global top `200` is `122` `1gram`, `42` `2gram`, `36` `3gram`
- this means a single unconstrained combined review surface begins strongly
  word-heavy
- but it also means later portions of the same list naturally shift toward more
  `2gram` and `3gram` material
- if that progression matches the intended learning story, then it is not a bug
- therefore, `v1` should preserve the natural ordering rather than injecting a
  composition layer here

Recommended stance:

- keep `candidate_scores` as the source of truth for ranking
- keep `1gram`, `2gram`, and `3gram` lane ranks separate
- make the primary `v1` review surface lane-first, not globally flattened
- if a global view is exposed, derive it directly from the stored scores
- keep all `6D` work read-only: no schema change, no new score storage, no
  hidden pruning, no composition overrides

#### Step 6D1: Ranked Review Surface

Expose the stored Step `6C` ranking in an inspectable CLI/query surface before
adding any broader export policy.

Scope:

- add scored-candidate query methods over `candidate_scores`
- expose top ranked candidates by lane
- expose focus lookup by candidate key
- expose score metadata such as `score_version`, eligible counts, and
  per-lane result counts
- keep the output read-only
- do not add a combined cross-lane review list yet

Recommended semantics:

- primary output is separate ranked sections for `1gram`, `2gram`, and `3gram`
- ordering within each lane is:
  - `lane_rank ASC`
  - this should already imply `final_score DESC`, `raw_frequency DESC`, then
    `candidate_key`
- output rows should expose:
  - Step `3` support facts
  - Step `4` association/boundary facts where present
  - Step `5` containment facts where present
  - Step `6` fields at minimum:
    - `score_version`
    - `ranking_lane`
    - `is_eligible`
    - `frequency_score`
    - `dispersion_score`
    - `association_score`
    - `boundary_score`
    - `redundancy_penalty`
    - `final_score`
    - `lane_rank`
- focus lookup should return scored rows for explicit candidate keys even if
  they are ineligible
- ineligible rows remain inspectable, but they should never appear in the
  default top-ranked sections

Recommended CLI surface:

- `refresh-candidate-scores` remains the write path
- add `inspect-candidate-scores --limit N`
- keep the same broad shape as `inspect-candidate-metrics`:
  - validation/header fields first
  - top rows by lane
  - focus rows by candidate key
- reuse the existing candidate-inspection emitter pattern where practical so
  the output stays coherent with earlier stages

Reason:

- `6C` produced a scored surface, but it is not yet conveniently inspectable
- review needs to happen against stored score components, not against ad hoc DB
  probes
- a lane-first inspection surface can validate score behavior without yet
  deciding whether the natural cross-lane ordering needs any later
  presentation policy

Validation:

- per-lane ranked output is deterministic
- focus lookup is deterministic
- no query path changes the stored score rows
- top sections exclude ineligible rows
- focus sections can still show ineligible rows

Sanity checks:

- `1gram` top rows still include function words and common lexical words
- `2gram` top rows include both glue-like scaffolding and stronger chunks
- `3gram` top rows include cohesive spoken frames
- focus rows show why a candidate ranks where it does by exposing component
  scores and penalties
- obvious direct-parent fragments such as `pense que` and `me dis` look weak
  for visible reasons rather than just disappearing

Tests:

- per-lane top list query test
- focus lookup test
- ineligible-row exclusion test for top sections
- score-field emission test
- no regression in existing candidate-metrics inspection output

Exit criteria:

- stored Step `6C` rankings are inspectable without raw SQL
- score components are visible enough to support policy review
- the repo has a stable read path for Step `6` before any optional cross-lane
  review surface is added

#### Step 6D2: Global Review Surface

Expose the natural cross-lane ranking as a read-only global review surface,
without any extra composition logic.

Scope:

- add a combined global top list over eligible scored rows
- keep lane labels visible inside the global view
- optionally expose a global rank field
- keep the global view read-only and directly derived from stored scores
- no example selection yet
- no blacklist hooks yet

Recommended pilot review stance:

- keep per-lane sections as the primary explanation surface
- treat the global list as a secondary inspection surface that shows what the
  current score policy naturally prefers
- do not inject hand-tuned interleaving rules
- if a bounded output is needed later, keep it outside this `v1` Step `6D`

Recommended pilot review order:

- global view:
  - `final_score DESC`
  - `raw_frequency DESC`
  - `candidate_key`
- keep `ranking_lane` visible in each global row so the user can see the lane
  mix directly
- avoid clever interleaving logic in `v1`

Reason:

- the current direction explicitly prefers preserving the natural progression
- live data already showed that the global list starts word-heavy but
  later shifts toward more `2gram` and `3gram` material
- exposing that natural progression is useful because it reveals what the
  current score policy is actually doing
- keeping `6D` free of composition logic makes the surface simpler and easier
  to audit

Validation:

- global review output is deterministic
- changing the requested `--limit` changes only how far down the natural list
  the output extends
- the global view never changes stored component scores or lane ranks
- no ineligible rows appear in the default global top list

Sanity checks:

- early global rows can be strongly `1gram`-heavy without that being treated as
  an automatic defect
- later global rows show increasing `2gram` and `3gram` presence
- no candidate disappears because of hidden pruning outside explicit support
  gates
- the per-lane sections and the global view tell a coherent story about the
  same stored scores
- useful function words, glue phrases, and stronger chunks all remain visible,
  just at different depths of the review surface

Tests:

- global list query test
- global ordering test
- global row emission test
- limit determinism test
- no regression in the lane-first `6D1` inspection surface

Exit criteria:

- the repo has both lane-first and global inspection surfaces
- the global view reflects stored scores directly, without hidden composition
  policy
- Step `6E` can validate both lane quality and the natural cross-lane
  progression

#### Step 6E: Pilot Validation

Run the first ranking and inspect whether the resulting lanes and global review
surface match the project goals.

Required commands:

```text
uv run podfreq refresh-candidate-metrics
uv run podfreq refresh-candidate-scores
uv run podfreq inspect-candidate-scores --limit 20
uv run ruff check src tests
uv run pytest
```

DB validation:

- `candidate_scores` rows exist for all current candidates and the current
  score version
- eligibility flags match lane support floors
- `1gram` rows have null association/boundary scores
- `3gram` rows have zero redundancy penalty
- repeated refresh is deterministic
- foreign-key integrity remains clean

Sanity checks:

- `du coup`, `en fait`, `il y a`, `je pense`, `je me dis`, and `j ai envie`
  surface strongly in their lanes
- `pense que`, `me dis`, `en tout`, and `tout cas` rank below their better
  direct parents in the `2gram` lane
- glue phrases and function words remain present when they clear the support
  gate
- `et le`, `est que`, `c est que`, `c est pas`, and similar weak fragments do
  not dominate the top of the eligible lanes
- the global review surface begins word-heavy but later shifts toward more
  `2gram` and `3gram` material in a way that still feels useful
- the global review surface reflects stored scores directly rather than hidden
  composition rules

Tests:

- unit and integration tests from `6A` through `6D` remain green
- pilot scoring does not require network
- repeated pilot refresh is deterministic

Exit criteria:

- first ranking is stored, inspectable, and pilot-validated
- Step 7 can choose examples from a stable, explicit ranked surface

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

Step 4A through 4E are implemented through unithood metric schema, deterministic
association and boundary refresh, candidate inspection commands, and pilot-scale
validation.

Step 5A through 5D are implemented through containment schema, deterministic
refresh, candidate-level inspection, and pilot validation.

Step 6A through 6E are implemented through score schema, eligibility gates,
stored scoring, lane/global inspection, and pilot validation.

Previous completed target:

```text
stable candidate facts -> stored unithood metrics for multiword candidates
```

Current completed target:

```text
global review surface -> pilot validation
```

Next implementation target:

```text
pilot-validated ranked surface -> example selection
```

Reason:

- candidate facts are now refreshed into inspectable frequency, dispersion,
  association, boundary, and containment metrics
- pilot analysis showed that whole-inventory ranking is too sparse and needed
  support gates before normalization
- pilot analysis also showed that `show_dispersion` is unusable in the current
  one-show snapshot and that `covered_by_any` is too degenerate for ranking
- stored `6C` scores now have both lane-first and global inspection surfaces
- pilot validation confirmed clean score storage, clean lane/global inspection,
  and deterministic reruns on the current snapshot
- live ranked data also showed that the global list starts heavily
  `1gram`-weighted but gradually shifts toward more multiword material, which
  may be a feature rather than a bug
- splitting `6D` into lane-first inspection first and global inspection second
  kept the read surface reviewable without imposing extra composition policy

Next step: Step 7, Example Selection.

## Follow-Up Adjustment Plan: Quality Gate Before Example Selection

If the Step 6 ranked surface still contains too much low-identity residue, run
this adjustment plan before Step 7.

Purpose:

- keep support gating and ranking, but stop obviously weak multiword rows from
  surviving forever just because they cleared the support floor
- preserve useful glue, function-heavy chunks, and opaque grammar chunks that
  really are worth learning
- fix structural false positives that are not really spoken units at all

Current pilot findings:

- Step 4 and Step 5 already detect many weak rows well; they are often low on
  association, weak on one boundary, and heavily covered by a direct parent
- the current Step 6 behavior mainly penalizes those rows rather than
  discarding them, so they eventually appear later in the ranked deck
- the current ranked tail therefore contains a large amount of frequent residue
  such as `est pour`, `est que`, `en a`, and similar fragments
- some weak-looking rows are still genuinely useful chunks, so a simple
  high-redundancy discard rule would be too crude
- there is also a separate structural leak earlier in the pipeline: span
  generation currently allows punctuation-bridging candidates like `en fait, c`
  and `moi, j`

Pilot snapshot notes:

- the current eligible pool contains comma-bearing multiword rows such as
  `en fait, c`, `Ouais, c`, `moi, j`, `toi, t`, and `du coup, c`
- those rows are not just low-ranked residue; some currently rank fairly high,
  which means a quality fix should not be treated as Step 8-only cleanup

Recommended stance:

- treat this as a deterministic follow-up plan, not a manual curation pass
- split the work into a structural cleanup track and a post-Step-5 quality-gate
  track
- keep the new heuristic inspectable and factual before turning it into a hard
  discard rule
- do not jump straight to blacklist hooks, POS tagging, or semantic classifiers
- do not discard purely by low final score; discard by interpretable quality
  conditions
- keep support gating separate from quality gating
- keep `1`-gram policy separate from `2`/`3`-gram policy in the first pass,
  because the current problem is mainly multiword residue

Main new heuristic:

- `unit_identity`

`unit_identity` should answer a different question from Step 4 and Step 5:

```text
Even if this candidate is frequent enough to score, does it still look like a
memorable, learnable unit rather than a loose fragment or clause-boundary shard?
```

`unit_identity` should start with two factual components.

### Unit Identity Component A: Surface Integrity

Measure whether the candidate tends to appear as one clean local unit in its
actual sentence surfaces.

Recommended factual metrics:

- `punctuation_gap_occurrence_count`
- `punctuation_gap_occurrence_ratio`
- `punctuation_gap_edge_clitic_count`
- `punctuation_gap_edge_clitic_ratio`

Recommended semantics:

- compute these from occurrence surfaces, not from one stored `display_text`
- count an occurrence as a punctuation-gap occurrence when the candidate spans
  over interior clause punctuation such as commas
- count an occurrence as an edge-clitic punctuation-gap occurrence when the
  candidate crosses such punctuation and one edge token is a stranded clitic
  fragment such as `c`, `j`, `t`, `l`, `d`, `m`, `n`, or `s`
- treat edge-clitic punctuation-bridging rows as structural red flags, because
  they often reflect clause-junction artifacts rather than stable chunks

Why this matters:

- Step 4 association and boundary metrics are computed over token sequences and
  can still look decent for some comma-bridging discourse patterns
- the ranked output showed that punctuation-bridging artifacts are one real
  source of false positives that should be addressed explicitly

### Unit Identity Component B: Lexical Anchor

Measure whether the candidate contains at least one strong lexical anchor rather
than being composed only of extremely common scaffold material.

Recommended factual metrics:

- `max_component_information`
- `min_component_information`
- `high_information_token_count`

Recommended semantics:

- derive component information from stored unigram frequencies
- use an information-content style signal such as `-log(p(token))`
- let `max_component_information` act as the main anchor signal
- keep this factual and language-light: no POS tagging, no lemma lookup, no
  hand-built stopword list

Why this matters:

- chunks such as `train de`, `faut que`, `ai envie`, and `ce moment` can look
  weaker on one Step 4 or Step 5 dimension but still have a clear lexical
  anchor that makes them card-worthy
- weak residues such as `est pour`, `est que`, `en a`, and `de le` often lack
  both strong cohesion and a strong anchor

### Existing Keep Reasons That Still Matter

The new heuristic should not replace Step 4 and Step 5. It should work beside
them.

Existing keep reasons:

- strong association
- usable weaker-side boundary independence
- low direct-parent domination
- clear lexical anchor
- clean surface integrity

The intended policy is not:

```text
high redundancy -> discard
```

The intended policy is:

```text
no strong keep reason + multiple weak signals -> discard
```

## Proposed Quality-Gate Model

Recommended ordering:

```text
support gate -> quality gate -> ranking -> example selection
```

Recommended semantics:

- Step 6B support floors continue to cut the sparse tail
- a new quality gate then removes eligible-but-low-value multiword residue
- final Step 6 ranking should run only over rows that pass both gates
- support-gate state and quality-gate state should both remain inspectable

Recommended storage direction:

- keep the new factual `unit_identity` metrics on `token_candidates`, alongside
  other stored candidate facts
- store quality-gate pass/fail state on `candidate_scores`
- keep one final `is_eligible` field if desired, but also expose separate
  support-pass and quality-pass status so discard reasons remain auditable

Recommended discard shape:

- `1`-grams:
  - leave unchanged in the first pass unless clear evidence appears that the
    same issue exists there
- `2`/`3`-grams:
  - hard-drop structural punctuation-bridging artifacts
  - otherwise require at least one strong keep reason before the row is allowed
    into the final ranked pool
  - if no keep reason is present, discard rather than merely pushing the row
    downward

Recommended first discard families:

- punctuation-gap edge-clitic artifacts such as `en fait, c`, `moi, j`, and
  `Ouais, c`
- low-association, weak-boundary, high-redundancy residues such as `est pour`,
  `est que`, `est des`, `en a`, and similar rows

Important caution:

- do not discard useful grammar chunks just because they are function-heavy
- do not discard purely because a row appears inside a larger parent
- do not use `covered_by_any` as a discard trigger
- do not use one arbitrary `display_text` value as the quality signal when the
  candidate has many occurrences

## Implementation Plan

This is moderately complicated and should not be done in one pass.

### Follow-Up A: Structural Span Cleanup Audit

Goal:

- decide which punctuation-bridging spans are structural junk and should stop
  being generated at all

Scope:

- inspect how `generate_sentence_spans(...)` currently treats token contiguity
  across punctuation gaps
- identify which punctuation classes should block span generation in the current
  French profile
- start with the clearest case: comma-bridging spans whose edge token is a
  stranded clitic fragment

Reason:

- this is not merely a ranking issue; some rows are artifacts of span
  construction itself
- fixing the earliest clean structural error reduces downstream complexity

Validation:

- read-only pilot query over punctuation-bearing candidates
- before/after count of comma-bridging eligible rows
- targeted span tests over cases like `en fait, c`, `Ouais, je`, `moi, j`,
  while preserving acceptable cases that should stay contiguous

Exit criteria:

- obviously bad punctuation-bridging artifacts are explicitly defined
- the design either blocks them in span generation or marks them for explicit
  downstream rejection

### Follow-Up B: Unit Identity Metric Contract

Goal:

- define the new factual metrics before adding any discard policy

Scope:

- add `surface_integrity` metrics
- add `lexical_anchor` metrics
- define null semantics
- decide whether all metrics live on `token_candidates` or whether one small
  helper table is truly needed

Recommended first metrics:

- `punctuation_gap_occurrence_count`
- `punctuation_gap_occurrence_ratio`
- `punctuation_gap_edge_clitic_count`
- `punctuation_gap_edge_clitic_ratio`
- `max_component_information`
- `min_component_information`
- `high_information_token_count`

Validation:

- fresh schema and migration tests if storage changes
- deterministic refresh test
- focused inspection over strong chunks, weak residues, and punctuation cases

Sanity checks:

- `du coup`, `en fait`, `il y a`, `je pense`, and `j ai envie` keep plausible
  unit-identity values
- `est pour`, `est que`, `en a`, and `de le` look weak on at least one new
  dimension
- punctuation-bridging artifacts score badly on `surface_integrity`

Exit criteria:

- the new heuristic is stored as factual evidence, not yet as a discard rule

### Follow-Up C: Inspection Surface And Threshold Discovery

Goal:

- make the new metrics reviewable before deciding exact discard thresholds

Scope:

- extend candidate inspection commands and summaries
- add focused comparison views for:
  - strong chunks
  - useful grammar chunks
  - obvious residue
  - punctuation-bridging artifacts

Recommended threshold-discovery method:

- review top, middle, and tail slices separately
- inspect borderline useful rows such as `train de`, `faut que`, `ai envie`,
  and `ce moment`
- inspect obvious weak rows such as `est pour`, `est que`, `de le`, `en a`,
  and punctuation-bridging shards

Reason:

- the discard thresholds should be learned from live pilot behavior, not guessed
  from one abstract formula

Validation:

- DB values match inspection output
- repeated refreshes leave threshold-audit slices stable

Exit criteria:

- a small set of threshold candidates exists for the quality gate
- known false-positive and false-negative examples are documented

### Follow-Up D: Quality Gate In Candidate Scores

Goal:

- convert the inspected heuristic into explicit discard logic

Scope:

- add support-pass vs quality-pass semantics to score refresh
- run final ranking only on rows that pass both
- keep dropped rows queryable and auditable
- record discard reason or discard family if that stays low-surface enough

Recommended policy shape:

- hard structural rejection for punctuation-gap edge-clitic artifacts
- soft factual rule for the remaining multiword rows:
  - keep if at least one strong keep reason is present
  - discard if no strong keep reason is present and multiple weak signals align

Recommended first strong keep reasons:

- association above a tuned threshold
- weaker-side boundary above a tuned threshold
- lexical-anchor signal above a tuned threshold
- low structural-fragment signal

Recommended first weak-signal cluster:

- low association
- weak weaker-side boundary
- high direct-parent domination
- no lexical anchor
- poor surface integrity

Validation:

- deterministic keep/drop counts on the pilot snapshot
- dropped rows no longer appear in ranked lane/global views
- useful borderline chunks remain in the final eligible pool

Sanity checks:

- `train de`, `faut que`, `ai envie`, and `ce moment` are not lost casually
- `est pour`, `est que`, `en a`, `de le`, and punctuation-bridging shards do
  not survive just because they were frequent enough

Exit criteria:

- the ranked pool is smaller and cleaner
- discard behavior is explicit rather than hidden inside score weights

Status:

- implemented in `pilot-v2`
- next step: Follow-Up E

### Follow-Up E: Pilot Validation And Threshold Revision

Goal:

- verify that the new gate improves card-worthiness without over-pruning

Validation:

- rerun metrics and scores twice to confirm deterministic results
- inspect top `200`, middle `200`, and tail slices before and after the change
- compare kept vs dropped rows for:
  - strong spoken chunks
  - useful grammar chunks
  - punctuation artifacts
  - obvious residue
- confirm that score ordering still looks sensible inside the reduced pool

Sanity checks:

- top ranked rows remain strong
- the late ranked tail is materially cleaner
- punctuation artifacts are largely gone
- higher-value lower-frequency rows are no longer drowned out by large amounts
  of weak residue

Exit criteria:

- the ranked surface is strong enough to proceed to Step 7
- if the ranked surface is still too noisy, revise thresholds before example
  selection rather than compensating later with example heuristics

Status:

- validated in `pilot-v2`
- no threshold revision needed after the pilot pass

Pilot validation notes:

- reruns were deterministic
- support gate held at `804` and final eligible pool settled at `624`
- kept borderline useful chunks included `train de`, `faut que`, `ai envie`,
  and `ce moment`
- dropped weak residue included `est pour`, `est que`, `de le`, `en a`,
  `moi, j`, and `en fait, c`
- top ranked rows stayed strong enough to proceed to Step 7
