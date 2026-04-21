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

Generate broad contiguous spans.

Deliverables:

- `token_candidates` table
- `token_occurrences` table
- candidate generation service
- CLI command, likely `generate-candidates`
- support for 1/2/3-token spans

Output:

- raw candidate inventory
- every occurrence linked to sentence context

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
