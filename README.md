![Job Matchmaking tests](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/ci.yml/badge.svg?branch=main)
[![Job Matchmaking coverage](https://codecov.io/github/devlink42/poc-dirac-job-matchmaking/graph/badge.svg?token=NUR0RK0T0I)](https://codecov.io/github/devlink42/poc-dirac-job-matchmaking)

# POC Dirac/DiracX Job Matchmaking

## Matchmaking Performance Benchmark

This directory contains the performance test suite using Locust to benchmark the matchmaking system. This framework
establishes the baseline for the Python prototype and will be reused for subsequent phases (e.g., Redis, Lua).

### Features

- Evaluates the core `select_job` algorithm.
- Simulates realistic distributions (LHCb production distributions).
- Measures **throughput (matches/sec)** and **latency distributions**.
- Configurable scale parameters (number of jobs, nodes, users, arrival rate).

### Prerequisites

Ensure your environment is properly set up using Pixi. Locust is already included in the `pixi.toml` dependencies.

### Running the Benchmarks

#### Generate data

You need to generate a database with a large number of jobs and nodes before running the benchmark. You can do this
using the following command:

```bash
pixi run generate_db --num-jobs 10000000 --num-nodes 50000
```

And this is the list of available parameters for the `generate_db` command:

- `--num-jobs`: Number of jobs to generate. Use at least the benchmark `--num-jobs`; additional jobs only increase the
  diversity of the sampled circular window. (Default: 1000000)
- `--num-nodes`: Number of nodes to generate. (Default: 50000)
- `--seed`: Random seed for reproducibility. (Default: 0)
- `--output`: Output database path. (Default: `benchmark/benchmark.db`)
- `--overwrite`: Overwrite an existing database.
- `--log-level`: Logging verbosity level, it can be `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. To have better
  results, set it to `ERROR` or `CRITICAL`. (Default: `INFO`).

#### Headless Mode (Quick Baseline)

To run a 15 minutes benchmark directly in your terminal with 100 concurrent users (pilot that runs `select_job`)
generating load:

```bash
pixi run benchmark -u 100 -r 50 -t 15m --num-nodes 50000
```

`--num-nodes` must not exceed the number of nodes in the generated database. The database must contain at least as many
jobs as requested by `--num-jobs`.

#### Web UI Mode (Interactive Exploration)

To explore latency graphs, throughput curves, and easily tweak the user load:

```bash
pixi run benchmark-ui -u 100 -r 50 -t 15m --num-nodes 50000
```

`--num-nodes` must not exceed the number of nodes in the generated database. The database must contain at least as many
jobs as requested by `--num-jobs`.

Then, open your browser at http://localhost:8089.

#### Configurable parameters for the benchmark

You can pass custom arguments to adjust the scale of the pre-loaded data:

- `--num-jobs`: Number of jobs pulled from the database and evaluated in each selection cycle. In the benchmark command,
  this is the candidate-window size and must not exceed the number of jobs generated in the database.
  (Default: 10000000)
- `--num-nodes`: Defines the total number of nodes available in the persistent database. (Default: 50000)
- `--seed`: Random seed for reproducibility. (Default: 0)
- `--config-path`: Path to the scheduling configuration. (Default: `config/scheduling.yaml`)
- `--db-path`: Path to the SQLite benchmark database. (generate with `benchmark/generate_db.py`, default:
  `benchmark/benchmark.db`)
- `--log-level`: Logging verbosity level, it can be `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. To have better
  results, set it to `ERROR` or `CRITICAL`. (Default: `INFO`)

Locust core parameters:

- `-u` / `--users`: The number of concurrent users (threads simulating scheduler processes).
- `-r` / `--spawn-rate`: How many users to spawn per second.
- `-t` / `--run-time`: Automatically stop the test after a certain duration (e.g., `1m`, `30s`).
- `-w` / `--workers`: The number of worker processes to spawn (Distributed mode only, default: 5).

*Note: The arrival rate of requests per user is defined in `locustfile.py` via the `wait_time` attribute. Also, ALL
benchmark executions generate comprehensive CSV and HTML reports.*

### Baseline Benchmark Results (Python Prototype)

**Test Context:** 10,000,000 Jobs, 50,000 Nodes.

## Redis Data Model Design

### 1. Introduction

This document outlines the design space for mapping DIRAC job scheduling requirements and node characteristics to Redis
data structures. The goal is to support high-performance matchmaking at a scale of ~10M pending jobs while keeping the
selection policy, atomicity requirements, and operational costs explicit.

### 2. Requirements & Query Patterns

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

### 3. Alternative Data Models

#### Alternative A: Redis Native Structures + Bounded Lua Evaluation

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

#### Alternative B: RedisJSON & RediSearch (exploratory fallback)

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

No `FT.SEARCH` command is included here deliberately. The index schema and the treatment of jobs open to every site
are not fixed by this design. If this fallback is prototyped, its exploratory documentation must define the complete
index schema and use matching field names, pass pilot values through an explicit `PARAMS` clause, and select `DIALECT 2`
when using tag unions. Site-agnostic jobs must either index an explicit sentinel such as `ANY` and query it together with
the pilot site, or use a separate query branch and field model. This fallback is not part of the recommended hot path.

#### Alternative C: Categorized Queues / Bucket Model

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

#### Alternative D: Custom Native Redis Module (Rust)

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

### 4. Memory Estimate at Target Scale

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

### 5. Recommendation

The recommendation has two separate axes, and the alternatives are not all competing at the same level:

1. **Reduce the candidate set first.** Route a Pilot to compatible drawers and collapse jobs with equivalent
   requirements into groups. The target is a bounded number of groups to inspect, not a faster comparison over millions
   of jobs.
2. **Evaluate and claim atomically.** Use native Redis structures and a bounded Lua script to evaluate candidate groups,
   apply the current scheduling state, and claim a job from the selected group.

The first axis dominates the second: reducing the number of candidates has a much larger effect than reducing the cost
of one individual comparison. The selection policy must therefore be computed from current state when a Pilot requests
work. It must not be implemented as a static per-job priority score, and neither `ZPOPMAX` nor `ZPOPMIN` nor
`SORTBY priority DESC LIMIT 0 1` represents the policy described above.

The selected direction is therefore **Alternative C for candidate routing, combined with Alternative A for the
atomic evaluation and claim**. The choices and their limits are:

#### Alternative C — Categorized queues / buckets: selected as the primary organization

Alternative C addresses the dominant problem directly: it reduces the number of candidates before any detailed
matching takes place. The existing `eligible:{site}:{job_type}` keyspace is already a form of bucketing, so accepting C
does not require introducing a new principle. It means keeping that routing model and choosing its dimensions based on
measured selectivity, for example site, job type, architecture, or GPU class where those dimensions actually separate
the workload.

C is selected over a purely global search because it gives the matcher a bounded, targeted starting set. Requirements
that are ranges or boolean expressions still have to be evaluated after routing, and adding every requirement to the
key would create combinatorial growth. The problem is therefore the choice of key dimensions, possible duplicated
entries, and the handling of stale references—not the bucketing approach itself. This is why C is the recommended data
organization, subject to the measurements in the implementation plan.

#### Alternative A — ZSET/hash/Lua: retain the mechanism at group granularity

Alternative A remains the execution mechanism for the selected design. Native Redis structures and a bounded Lua script
provide the required atomic view of scheduling state, selection, and claim on one Redis instance. A group stores shared
requirements once and keeps its pending job references behind it; Lua scans candidate groups and claims a job from the
selected group.

A is not selected as a standalone global queue of individual jobs. Scanning millions of jobs in Lua would block Redis's
main thread and would leave the candidate-count problem unchanged. This is also consistent with the memory table:
Alternative A is the least costly classic Redis option in the illustrative estimate, but that does not make an
unbounded per-job scan viable. Grouping and routing are what make the bounded A mechanism viable.

#### Alternative B — RedisJSON / RediSearch: reserve for groups, not the hot path

Alternative B is not retained for job-granularity matchmaking. Its recommendation cannot be based on an `O(1)` or
`O(log N)` search promise: the actual cost depends on the predicates, result cardinality, sorting, document loading,
index configuration, and topology. The search result also does not include the complete stateful scheduling decision;
running limits, tier policy, and the final claim still need an atomic protocol.

B remains a reasonable fallback or optional index over requirement groups if measurements show that routed, bounded Lua
scans and caching are insufficient. At group granularity the index would contain far fewer documents than a per-job
index, and it could reduce the groups presented to Lua. Its additional document and index overhead is why it is not the
default hot-path choice.

#### Alternative D — Custom native Redis module in Rust: reject at this stage

Alternative D optimizes the cost of an individual comparison and may reduce representation overhead, but it does not
reduce the number of candidates. A faster comparison cannot compensate for a candidate volume that is orders of
magnitude too large. Its packaging, deployment, maintenance, and operational complexity also require evidence that the
standard Redis/Lua design is insufficient.

D can be reconsidered only after grouping, bucket selection, caching, and bounded Lua execution have been measured
against the target. Until then, the candidate-count reduction provided by C has a much better expected return than
moving the comparison loop into native code.

This also resolves the apparent memory contradiction. Alternative A is the least costly classic Redis option in the
illustrative table, so it must not be rejected for a supposedly high memory footprint. Alternative C may consume more
memory when bucket entries are duplicated, but remains acceptable because its dimensions can be measured and tuned.
Alternative B's additional index overhead is a reason to keep it out of the hot path, not a reason to promise a search
complexity that has not been established.

Before fixing the schema, measure the number of groups, eligible sites per job, candidate groups per Pilot, concurrent
claim latency, throughput, and memory per job, group, queue entry, index, replica, and cluster slot.
