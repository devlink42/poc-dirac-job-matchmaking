# Data Model & Architecture Design

## 1. Introduction

This document outlines the design for mapping DIRAC job scheduling requirements and node characteristics to data
structures. The goal is to support high-performance matchmaking at a scale of ~10M pending jobs and ~100 sites (×500
nodes), ensuring atomicity and efficiency.

To evaluate the best approach, the system supports a baseline SQLite implementation and 5 distinct Redis implementation
strategies, each isolated in its respective directory.

## 2. Requirements & Query Patterns

- **Matchmaking Query:** A Pilot requests a job based on its characteristics (Available RAM, Available CPU, Tags, Site
  Name). The system must return the highest priority job that fits these constraints.
- **Atomicity:** Concurrent pilots must not be assigned the same job. Race conditions must be strictly prevented.
- **Scale:** ~10,000,000 pending jobs, ~100 active sites × 500 nodes.

## 3. Implementation Strategies & Data Models

### Baseline: Python + SQLite (`matchmaking/core/`)

- **Data Model:** Relational tables (e.g., `jobs`, `nodes`) stored locally.
- **Matching Flow:** The Python application queries the database using SQL `SELECT` statements with `WHERE` clauses
  matching the node's capabilities, ordered by priority.
- **Pros:**
    - Pure Python implementation, very easy to set up, test, and debug.
    - Standard SQL querying natively supports complex multi-criteria filtering.
- **Cons:**
    - **Disk/Concurrency Bottleneck:** SQLite is disk-bound and relies on file-level (or WAL) locks, making it entirely
      unsuited for the high-concurrency, sub-millisecond latency requirements of 10M jobs. It serves purely as a
      benchmark baseline.

### Redis Strategy 1: Python Evaluation (`matchmaking/core/py_redis/`)

- **Data Model:** - `jobs:pending` (ZSET): Scores represent priority, values are Job IDs.
    - `job:<id>` (HASH): Stores job requirements.
- **Matching Flow:** The Python application fetches the top N jobs from the ZSET, retrieves their data via `HMGET`, and
  evaluates the logic strictly in Python space. If a match is found, it attempts to atomically lock/remove it.
- **Pros:** Leverages `pydantic` natively. Easy to maintain.
- **Cons:** Transferring hundreds of job definitions over the network to the Python worker is heavily bottlenecked by
  network I/O. High risk of race conditions during lock attempts.

### Redis Strategy 2: ZSET Queue + Hash Data + Lua (`matchmaking/core/lua/alt_a/`)

- **Data Model:** Identical to the `py_redis` strategy (`ZSET` + `HASH`).
- **Matching Flow:** A Lua script fetches the top N jobs using `ZRANGE`, iterates through them, retrieves requirements
  via `HMGET`, evaluates constraints against the Pilot's context, and atomically removes (`ZREM`) the first matching
  job.
- **Pros:** 100% atomic execution within Redis. Predictable memory footprint.
- **Cons:** Head-of-line blocking (Lua wastes CPU cycles iterating over incompatible jobs). Long-running Lua scripts
  block the entire Redis instance.

### Redis Strategy 3: RedisJSON & RediSearch (`matchmaking/core/lua/alt_b/`)

- **Data Model:** Jobs are stored as JSON or Hashes. A RediSearch Index is built on top of the scheduling requirements.
- **Matching Flow:** The application executes a search query via `FT.SEARCH`. A small Lua script then attempts to lock
  it atomically.
- **Pros:** Delegates complex multi-criteria filtering to the database engine. Highly scalable for complex DIRAC JDL
  requirements.
- **Cons:** Requires Redis modules. Indexing significantly increases RAM usage. Atomicity requires a lock/retry pattern.

### Redis Strategy 4: Categorized Queues / Buckets (`matchmaking/core/lua/alt_c/`)

- **Data Model:** Jobs are pre-routed into multiple specific queues (e.g., `jobs:pending:site:LCG`) based on their
  requirements upon submission.
- **Matching Flow:** A Pilot checks specific queues matching its capabilities and directly pops from them.
- **Pros:** Extremely fast read operations. Zero head-of-line blocking.
- **Cons:** Combinatorial explosion for multi-dimensional requirements. Complex write logic and massive risk of stale
  data if jobs are duplicated across queues.

### Redis Strategy 5: Custom Native Redis Module (`matchmaking/core/rust/`)

- **Data Model:** Generic Redis types are bypassed. Jobs and sites are mapped to highly packed memory structures in a
  compiled Rust module.
- **Matching Flow:** The Pilot calls a custom command. The logic executes in compiled machine code within the main Redis
  thread.
- **Pros:** Absolute peak performance and extreme memory efficiency. 100% atomic. Memory safe (Rust).
- **Cons:** Introduces a low-level language into a Python ecosystem. Deployment complexity. Panic risk across the FFI
  boundary.

## 4. Memory Estimate at Target Scale (10M Jobs, 1000 Sites)

| Component           | `SQLite` (Base)       | `py_redis` & `alt_a` | `alt_b` (RediSearch)   | `alt_c` (Buckets)    | `rust` (Native Module)  |
|:--------------------|:----------------------|:---------------------|:-----------------------|:---------------------|:------------------------|
| **Storage Engine**  | Disk-based            | In-Memory (Generic)  | In-Memory (Indexed)    | In-Memory (Generic)  | In-Memory (Native)      |
| **10M Jobs Data**   | Variable (Disk)       | ~1.5 GB              | ~1.5 GB                | ~1.5 GB              | ~600 MB (Dense structs) |
| **Indexing/Queues** | Variable (Disk)       | ~1.1 GB (ZSET)       | ~3.5 to 5 GB (Indexes) | ~1.3 GB (Multi-ZSET) | ~200 MB (Custom index)  |
| **Total RAM Est.**  | **Minimal (App RAM)** | **~2.6 GB - 3.5 GB** | **~5 GB - 8 GB**       | **~2.8 GB - 4.0 GB** | **~800 MB - 1.0 GB**    |
