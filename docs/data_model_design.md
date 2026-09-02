# Data Model & Architecture Design

## 1. Introduction

This document outlines the design for mapping DIRAC job scheduling requirements and node characteristics to data
structures. The goal is to support high-performance matchmaking at a scale of ~10M pending jobs and ~100 sites (×500
nodes), ensuring atomicity and efficiency.

To evaluate the best approach, the system supports a baseline SQLite implementation and 5 distinct Redis implementation
strategies, each isolated in its respective directory.

## 2. Requirements & Query Patterns

- **Matchmaking Query:** A Pilot requests a job based on its characteristics (Available RAM, Available CPU, Tags, Site
  Name). The matcher must apply running-job limits and the configured job-type tier policy before selecting and claiming
  a compatible job.
- **Atomicity:** Concurrent pilots must not be assigned the same job. The limit check, selection, and claim must use a
  consistent view of mutable scheduling state.
- **Scale:** The working target is ~10,000,000 pending jobs and ~50,000 nodes. The number of active sites must be stated
  consistently and measured rather than inferred from the node count alone.

The selection policy depends on mutable execution counters and configuration. It must not be represented as a permanent
per-job priority score. In the current Python implementation, already-filtered jobs are ranked by:

1. the number of running jobs in the job's group;
2. the number of running jobs for the job's owner;
3. the submission date.

When a tier is configured as a dictionary, its values express weights between job types in that tier. The submission
date is only a tie-breaker in the current ranking: this is not a global FIFO policy, and an older job may be overtaken
by work from a less-served group or owner. The policy for choosing between groups and the final intra-group dispatch
policy must be defined explicitly before the Redis data structures are fixed.

## 3. Implementation Strategies & Data Models

The repository currently contains only the Python baseline in `matchmaking/core/`: it filters compatible waiting jobs,
applies limits, ranks them by group/owner usage and submission time, and assigns the selected job. The Redis
alternatives below are target designs and are not implemented yet.

### Baseline: Python + SQLite (`matchmaking/core/`)

- **Status:** Implemented baseline.
- **Does:** Loads jobs from SQLite, filters compatible waiting jobs, applies scheduling limits, ranks candidates, and
  assigns the selected job.

### Redis Strategy 1: Python Evaluation (`matchmaking/core/py_redis/`)

- **Status:** Not retained as a target design.
- **Reason:** Moving job data to Python adds network overhead and makes the search/claim sequence harder to keep
  consistent under concurrency.

### Alternative A: Redis Native Structures + Bounded Lua Evaluation (`matchmaking/core/lua/alt_a/`)

- **Status:** Selected execution mechanism.
- **Wanted:** Store shared requirements in groups, route candidate groups through drawers, and use bounded Lua for the
  compatibility check, scheduling decision, and atomic claim.
- **Limit:** Do not scan a global queue of individual jobs; keep all keys for one atomic operation in the same cluster
  slot.

### Alternative B: RedisJSON & RediSearch (`matchmaking/core/lua/alt_b/`) exploratory fallback

- **Status:** Optional fallback for groups, not for the hot path.
- **Wanted:** Index stable group attributes only if routing, bounded Lua scans, and caching are insufficient.
- **Limit:** Define the complete schema and site-agnostic representation before adding a query; the index only reduces
  candidates and never replaces the scheduling and claim protocol.

### Alternative C: Categorized Queues / Bucket Model (`matchmaking/core/lua/alt_c/`)

- **Status:** Selected primary organization.
- **Wanted:** Route jobs through drawers such as `eligible:{site}:{job_type}`, add dimensions only when measurements
  show useful selectivity, and group jobs with equivalent requirements.
- **Limit:** Evaluate ranges and tag expressions after routing; avoid unnecessary key dimensions and duplicated entries.

### Alternative D: Custom Native Redis Module (Rust) (`matchmaking/core/rust/`)

- **Status:** Rejected at this stage.
- **Wanted:** Reconsider only if measurements show that the grouped Redis/Lua design cannot meet the target.
- **Limit:** No native module before the candidate count, memory, latency, and contention costs of the standard design
  are measured.

## 4. Alternative Data Model Details

### Alternative A: Redis Native Structures + Bounded Lua Evaluation

- **Data Model:** Requirement groups are stored in native Redis structures. A group stores shared matching requirements
  and references to its pending jobs. Candidate group identifiers may be routed through drawers based on compatible
  exact attributes; this is not a global per-job priority queue.
- **Matching flow:** A bounded Lua script evaluates candidate groups against the Pilot's context, applies the current
  limits and tier policy, performs the configured group selection, and atomically claims a job from the selected group.
- **Pros:**
    - Uses core Redis structures and does not require a module.
    - Can make the state check, selection, and claim atomic on one Redis instance.
    - Keeps mutable scheduling inputs out of a stale per-job score.
- **Cons:**
    - A scan over individual jobs creates head-of-line blocking and does not scale to millions of candidates.
    - Lua runs atomically on Redis's main thread, so the number of inspected groups must remain bounded.
    - Fetching and evaluating every individual job is too expensive; requirements must be grouped before entering the
      hot path.
    - In Redis Cluster, all keys needed by one atomic operation must be placed in the same hash slot.

### Alternative B: RedisJSON & RediSearch (exploratory fallback)

- **Data Model:** Requirement groups, rather than individual jobs, may be stored as JSON documents or hashes. A
  RediSearch index can cover stable matching attributes such as rounded resource requirements, architecture, tags, and
  eligible sites. The pending jobs remain associated with their group.
- **Matching flow:** The index reduces the set of groups to examine. The matcher then applies running limits, job-type
  tiers, and the mutable selection policy before atomically or optimistically claiming a job.
- **Pros:**
    - Can delegate complex, stable matching predicates to an index.
    - Is a possible fallback when a bounded Lua scan over groups is too large.
    - Avoids treating mutable scheduling state as a static indexed job field.
- **Cons:**
    - Requires the RediSearch module and increases memory usage through document and index overhead.
    - Search is not inherently `O(1)` or `O(log N)`; cost depends on the predicates, result cardinality, sorting,
      document loading, index, and topology.
    - A search followed by a claim is not a single decision. It requires an atomic protocol or optimistic locking with
      retry.
    - RediSearch reduces the candidate set but does not replace the stateful scheduling step.

No `FT.SEARCH` command is included here deliberately. The index schema and the treatment of jobs open to every site are
not fixed by this design. If this fallback is prototyped, its exploratory documentation must define the complete index
schema and use matching field names, pass pilot values through an explicit `PARAMS` clause, and select `DIALECT 2`
when using tag unions. Site-agnostic jobs must either index an explicit sentinel such as `ANY` and query it together
with the pilot site, or use a separate query branch and field model. This fallback is not part of the recommended hot
path.

### Alternative C: Categorized Queues / Bucket Model

- **Data Model:** Jobs are routed using exact-match attributes, such as site and job type, and can then be grouped by
  equivalent requirements. A group owns its pending job references and shared matching data.
- **Matching flow:** A Pilot opens drawers compatible with its exact attributes. The matcher evaluates remaining range
  and expression requirements, applies the current limits and tier policy, selects a group, and claims a job according
  to the intra-group policy that is ultimately defined.
- **Pros:**
    - Provides a targeted starting point for reducing the candidate set.
    - Can make removal from a selected group very cheap using native Redis queues.
    - Separates stable matching attributes from mutable global scheduling state.
- **Cons:**
    - Encoding every dimension in a bucket key creates combinatorial growth.
    - Range requirements and boolean tag expressions cannot all be mapped to one exact drawer without opening many
      drawers or evaluating the requirements separately.
    - Duplicating jobs across queues creates stale entries and cross-queue deletion problems.
    - Selecting a compatible bucket does not by itself apply limits, fair-share counters, or the final group policy.

### Alternative D: Custom Native Redis Module (Rust)

- **Data Model:** Jobs, groups, and indexes would be managed by compact structures in a compiled Rust module loaded into
  Redis.
- **Matching flow:** A custom command would evaluate the matching and scheduling logic inside Redis and return a claimed
  job identifier.
- **Pros:**
    - May reduce interpreter and generic Redis object overhead.
    - Rust can provide memory-safety advantages over a C module.
    - Can keep the operation atomic within Redis's execution model.
- **Cons:**
    - Memory and latency claims require benchmarks with realistic requirements, cardinalities, and contention.
    - A native module adds packaging, deployment, maintenance, and operational complexity.
    - Faster comparisons do not solve an excessive number of candidates or groups.
    - An FFI boundary and unhandled Rust panics remain operational risks.

## 5. Memory Estimate at Target Scale

The following table gives an **illustrative theoretical estimate**, not a benchmark result. It is intended to make the
relative cost drivers visible while the Redis schemas are being compared.

The illustrative scenario assumes 10M pending jobs, 50,000 nodes, approximately 100 active sites, one primary Redis
instance, no replicas, and one logical copy of each job. The ranges are deliberately broad because the actual result
depends on the serialized requirement size, group cardinality, site fan-out, tag distribution, Redis version, allocator,
and module configuration. Replicas and cluster copies are not included.

| Memory component                      | Alternative A: Native Redis + Lua | Alternative B: RedisJSON + RediSearch | Alternative C: Categorized buckets | Alternative D: Rust module |
|:--------------------------------------|:----------------------------------|:--------------------------------------|:-----------------------------------|:---------------------------|
| Job data and identifiers for 10M jobs | ~1.5–2.5 GB                       | ~1.5–2.5 GB                           | ~1.0–2.0 GB                        | ~0.4–0.8 GB                |
| Requirement groups and metadata       | ~0.1–0.5 GB                       | ~0.2–0.8 GB                           | ~0.1–0.5 GB                        | ~0.05–0.2 GB               |
| Queue entries and group references    | ~0.5–1.2 GB                       | ~0.5–1.2 GB                           | ~0.8–2.5 GB                        | ~0.1–0.4 GB                |
| Search or routing indexes             | ~0.3–1.0 GB                       | ~2.0–5.0 GB                           | ~0.3–1.2 GB                        | ~0.1–0.4 GB                |
| Redis/object/module overhead          | ~0.3–0.8 GB                       | ~0.8–1.5 GB                           | ~0.5–1.2 GB                        | ~0.1–0.4 GB                |
| **Theoretical total**                 | **~2.7–6.0 GB**                   | **~5.0–11.0 GB**                      | **~2.7–7.4 GB**                    | **~0.8–2.2 GB**            |

These figures should be read as engineering placeholders only. In particular, Alternative C can move substantially with
site fan-out or duplicated bucket entries, while Alternative B can move substantially with the number of indexed fields
and matching groups. Alternative D's compact figures are only a representation hypothesis and require a real module
prototype to validate.

A useful measurement model is:

```text
total_memory ≈ job_data + group_data + queue_entries + indexes + Redis_overhead
```

Capacity must still be measured with a consistent target, a representative dataset, and a reproducible method. The
theoretical totals above must not be used as capacity guarantees or as the sole reason to select or reject an
alternative.
