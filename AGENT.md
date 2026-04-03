# AGENT.md

## Project purpose

This repository implements an AI assistant for objective analysis of public regional signals:
news, social media posts, comments, public channel messages, and citizen обращения.

The system must:
1. collect public messages in real time,
2. normalize them into a unified schema,
3. detect semantically coherent topics/problems,
4. generate neutral summaries,
5. rank issues by operational importance,
6. produce an auditable top-10 agenda for regional authorities.

This is not a chatbot-first project.
This is a data pipeline and decision-support system with LLM components.

---

## Core engineering principles

1. Deterministic logic is preferred over LLM logic where possible.
2. LLM is used only for tasks that are inherently semantic:
   - cluster naming,
   - neutral summarization,
   - optional zero-shot categorization.
3. Ranking must remain explainable and auditable.
4. Every stage must have a single clear responsibility.
5. All source-specific logic must be isolated from the domain model.
6. All contracts between stages must be explicit and versioned.
7. No hidden coupling between ingestion, preprocessing, ML, and API layers.
8. The system must degrade gracefully if LLM is unavailable.

---

## Canonical pipeline

The pipeline is strictly ordered:

1. Ingestion
2. Structural normalization
3. Language detection
4. Content filtering
5. Text cleaning
6. Deduplication
7. Geo enrichment
8. Metadata enrichment
9. Embedding generation
10. Semantic clustering
11. Optional classification
12. Cluster summarization
13. Importance ranking
14. Top-10 issue generation
15. API / dashboard delivery

Agents must not bypass or reorder these stages without an explicit architecture decision.

---

## Domain concepts

Use these terms consistently:

- RawMessage: unprocessed record from a source
- NormalizedMessage: unified internal representation
- EnrichedMessage: normalized message with geo and metadata enrichment
- MessageEmbedding: vector representation of a message
- TopicCluster: group of semantically similar messages
- Issue: operationally meaningful topic prepared for decision-makers
- SummaryCard: neutral cluster summary
- RankingScore: explainable score used for prioritization
- SourceSignal: measurable metadata such as reach, engagement, officialness, locality

Do not invent synonyms for these entities in code.

---

## Boundaries of responsibility

### Ingestion
Responsible only for fetching and raw persistence.
Must not contain ML logic.

### Preprocessing
Responsible for text normalization, cleaning, deduplication, and enrichment.
Must not perform final ranking or summarization.

### ML
Responsible for embeddings, clustering, optional classification, and summaries.
Must not fetch source data directly.

### Ranking
Responsible for deterministic prioritization of issues.
Must not depend on opaque LLM judgments as the primary signal.

### API/UI
Responsible only for delivery and presentation.
Must not contain business scoring logic.

---

## Data contract expectations

All internal messages must preserve these minimum fields:

- message_id
- source_type
- source_id
- author_id
- text
- normalized_text
- created_at_utc
- language
- region_id
- municipality_id (nullable)
- geo_confidence
- reach
- engagement
- is_official
- media_type
- duplicate_group_id (nullable)
- near_duplicate_flag
- embedding_version
- pipeline_version

Never remove these fields silently.
Never rename them without updating contracts and downstream consumers.

---

## ML architecture rules

### Embeddings
Embeddings are mandatory.
They are the semantic foundation of clustering.

### Clustering
Topic detection is performed through embedding-based clustering.
Do not replace clustering with ad hoc keyword grouping.

### Classification
Optional.
Use only when fixed taxonomy assignment is needed.

### Summarization
LLM-based summarization is allowed only on clusters, not on isolated posts by default.
Summaries must be neutral, concise, and evidence-grounded.

### Ranking
Ranking is not a standalone neural task by default.
Use a deterministic formula based on:
- volume,
- growth,
- negativity,
- geographic spread,
- source diversity,
- reach,
- official-source presence,
- persistence over time.

Any ML-based ranking experiment must be additive, not the primary production path.

---

## Explainability requirements

Every produced issue must be explainable by:
1. source count,
2. time trend,
3. top locations,
4. representative messages,
5. ranking score breakdown,
6. cluster summary,
7. category/tags if available.

If a stage reduces explainability, do not introduce it into production.

---

## LLM usage rules

LLM may be used for:
- neutral summarization,
- cluster title generation,
- optional zero-shot category assignment,
- optional contradiction or ambiguity checks.

LLM must not be used for:
- raw data cleaning,
- hard filtering,
- primary deduplication,
- authoritative ranking,
- silent fact invention.

All LLM outputs must be validated by schema and post-processing rules.

---

## Prompting rules

Prompts must live outside application logic in `/prompts` or `/configs/prompts`.

Each prompt must define:
- task goal,
- allowed input fields,
- forbidden behavior,
- output schema,
- tone constraints.

For public-administration summaries:
- no emotional language,
- no political interpretation,
- no unsupported causal claims,
- no recommendations disguised as facts,
- no invented actors or locations.

---

## Guardrails

Agents must not:
- merge unrelated pipeline stages,
- move business rules into prompts,
- add hidden heuristics without documentation,
- introduce source-specific hacks into the domain layer,
- replace deterministic scoring with subjective LLM scoring,
- change Kafka/Postgres schemas without migration notes,
- remove audit fields,
- treat duplicates as deletable noise if they carry scale signal.

---

## Expected outputs

Production outputs should prioritize:
1. issue cards,
2. neutral summaries,
3. ranking transparency,
4. traceability to source messages,
5. temporal and geographic breakdown.

The main artifact is not generated prose.
The main artifact is a trustworthy operational issue representation.

---

## Coding rules for agents

1. Prefer small composable modules.
2. Keep pure functions pure.
3. Separate IO from transformation logic.
4. Use typed schemas and explicit interfaces.
5. Write tests for contracts and scoring logic.
6. Do not hardcode prompt text inside Python services.
7. Do not place experimental notebook logic into production modules.
8. For every non-trivial decision, update docs/decisions.

---

## What to optimize for

Optimize for:
- traceability,
- correctness,
- explainability,
- reproducibility,
- maintainability,
- graceful degradation.

Do not optimize first for:
- prompt cleverness,
- multi-agent complexity,
- end-to-end autonomy,
- flashy summaries.

---

## Definition of done for any change

A change is complete only if:
1. contracts remain valid,
2. tests pass,
3. explainability is preserved,
4. metrics are updated if logic changed,
5. docs are updated for architectural changes,
6. no hidden coupling was introduced.

---

## Change policy

When changing architecture-sensitive code, also update:
- docs/architecture/
- docs/decisions/
- tests/contract/
- configs/ if rules changed

If unsure, preserve the current pipeline boundaries and ask for an explicit architectural decision rather than improvising.

---

## Final instruction for all agents

Treat this repository as a decision-support platform with ML components, not as a general-purpose chatbot system.
Prefer stable, auditable, modular solutions over agentic complexity.