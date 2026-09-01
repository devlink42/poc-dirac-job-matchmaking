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

## 3. Implementation Strategies & Data Models

The repository currently contains only the Python baseline in `matchmaking/core/`: it filters compatible waiting jobs,
applies limits, ranks them by group/owner usage and submission time, and assigns the selected job. The Redis alternatives
below are target designs and are not implemented yet.

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
- **Wanted:** Route jobs through drawers such as `eligible:{site}:{job_type}`, add dimensions only when measurements show
  useful selectivity, and group jobs with equivalent requirements.
- **Limit:** Evaluate ranges and tag expressions after routing; avoid unnecessary key dimensions and duplicated entries.

### Alternative D: Custom Native Redis Module (Rust) (`matchmaking/core/rust/`)

- **Status:** Rejected at this stage.
- **Wanted:** Reconsider only if measurements show that the grouped Redis/Lua design cannot meet the target.
- **Limit:** No native module before the candidate count, memory, latency, and contention costs of the standard design are
  measured.
