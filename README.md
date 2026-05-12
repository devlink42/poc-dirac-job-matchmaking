![Job Matchmaking tests](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/ci.yml/badge.svg?branch=main)
[![Job Matchmaking coverage](https://codecov.io/github/devlink42/poc-dirac-job-matchmaking/graph/badge.svg?token=NUR0RK0T0I)](https://codecov.io/github/devlink42/poc-dirac-job-matchmaking)

# POC Dirac/DiracX Job Matchmaking

## Matchmaking Performance Benchmark

This directory contains the performance test suite using Locust to benchmark the matchmaking system.
This framework establishes the baseline for the Python prototype and will be reused for subsequent phases (e.g., Redis,
Lua).

### Features

- Evaluates the core `valid_job_with_node` algorithm.
- Simulates realistic distributions (LHCb production distributions).
- Measures **throughput (matches/sec)** and **latency distributions**.
- Configurable scale parameters (number of jobs, nodes, users, arrival rate).

### Prerequisites

Ensure your environment is properly set up using Pixi. Locust is already included in the `pixi.toml` dependencies.

### Running the Benchmarks

#### Generate data

```bash
pixi run python -m benchmark.generate_db --num-jobs 10000000 --num-nodes 50000
```

#### Headless Mode (Quick Baseline)

To run a 15 minutes benchmark directly in your terminal with 100 concurrent users generating load:

```bash
pixi run benchmark -u 100 -r 50 -t 15m --num-jobs 10000000 --num-nodes 50000
```

`--num-jobs` and `--num-nodes` have to be set to the same value as the generated data.

#### Distributed Headless Mode (High Load Testing)

To run the benchmark in a distributed environment with multiple nodes:

```bash
pixi run benchmark-dist -u 100 -r 50 -t 15m --num-jobs 10000000 --num-nodes 50000
```

`--num-jobs` and `--num-nodes` have to be set to the same value as the generated data.

#### Web UI Mode (Interactive Exploration)

To explore latency graphs, throughput curves, and easily tweak the user load:

```bash
pixi run benchmark-ui -u 100 -r 50 -t 15m --num-jobs 10000000 --num-nodes 50000
```

`--num-jobs` and `--num-nodes` have to be set to the same value as the generated data.

#### Distributed Web UI Mode (High Load Testing)

To run the benchmark in a distributed environment with multiple nodes:

```bash
pixi run benchmark-dist-ui -u 100 -r 50 -t 15m --num-jobs 10000000 --num-nodes 50000
```

`--num-jobs` and `--num-nodes` have to be set to the same value as the generated data.

Then, open your browser at http://localhost:8089.

#### Configurable Parameters

You can pass custom arguments to adjust the scale of the pre-loaded data:

- `--num-jobs`: Defines the size of the job pool to generate (Default: 100000).
- `--num-nodes`: Defines the size of the node/pilot pool to generate (Default: 1000).
- `--candidates-count`: Number of jobs to evaluate in each selection cycle (Default: 500).
- `--config-path`: Path to the scheduling configuration (Default: `config/scheduling.yaml`).
- `--db-path`: Path to the SQLite benchmark database (generate with `benchmark/generate_db.py`, default:
  `benchmark/benchmark.db`).

Locust core parameters:

- `-u` / `--users`: The number of concurrent users (threads simulating scheduler processes).
- `-r` / `--spawn-rate`: How many users to spawn per second.
- `-t` / `--run-time`: Automatically stop the test after a certain duration (e.g., `1m`, `30s`).

*Note: The arrival rate of requests per user is defined in `locustfile.py` via the `wait_time` attribute.*

### Baseline Benchmark Results (Python Prototype)

**Test Context:** 10,000,000 Jobs, 20,000 Nodes.

Here is a summary of consecutive benchmarks performed on the system based on various worker configurations, user loads,
and candidate sizes.

#### Table 1: Default Settings (Single worker, various runs)

These tests reflect non-distributed or low-scale default executions.

| Time / Scenario      | Request Count | Throughput (req/s) | Min Resp. | Median (50%) | 95% Resp. | 99% Resp. | Max Resp. |
|:---------------------|:--------------|:-------------------|:----------|:-------------|:----------|:----------|:----------|
| **2026-05-04 14h33** | 115,333       | ~192 req/s         | 0.65 ms   | 1 ms         | 2 ms      | 2 ms      | 403.4 ms  |
| **2026-05-04 15h05** | 208,409       | ~347 req/s         | 0.61 ms   | 1 ms         | 2 ms      | 2 ms      | 92.9 ms   |

*Observation:* With basic configurations, the single-process Python implementation handles between 200 and 350 requests
per second. The median response time remains excellent (1ms), though maximum response times can occasionally spike.

#### Table 2: Impact of Candidate Dataset Size

These tests evaluate how the size of the initial candidate pool (JSON parsing + algorithmic filtering) impacts CPU and
latency.

| Scenario                         | Request Count | Throughput (req/s) | Min Resp. | Median (50%) | 95% Resp. | 99% Resp. | Max Resp. |
|:---------------------------------|:--------------|:-------------------|:----------|:-------------|:----------|:----------|:----------|
| **10 candidates** (5 workers)    | 178,777       | ~198 req/s         | 0.14 ms   | ~0 ms        | 1 ms      | 1 ms      | 442.3 ms  |
| **50 candidates** (10 workers)   | 177,694       | ~197 req/s         | 0.63 ms   | 2 ms         | 2 ms      | 3 ms      | 46.3 ms   |
| **100 candidates** (10 workers)  | 178,463       | ~198 req/s         | 0.01 ms   | 0 ms         | 0 ms      | 0 ms      | 3.4 ms    |
| **100 candidates** (10 workers)* | 174,694       | ~194 req/s         | 1.43 ms   | 4 ms         | 5 ms      | 21 ms     | 600.5 ms  |
| **100 candidates** (5 workers)   | 174,834       | ~194 req/s         | 1.31 ms   | 3 ms         | 4 ms      | 5 ms      | 128.3 ms  |
| **500 candidates** (5 workers)   | 35,954        | ~40 req/s          | 0.05 ms   | 0.05 ms      | 0 ms      | 1 ms      | 1.3 ms    |

*\*Note: The second 100 candidates run appears to have experienced temporary system load/spikes, resulting in anomalous
maximum values.*

*Observation:*

- When evaluating **50 to 100 candidates**, the throughput stays relatively stable at ~200 requests/second.
- However, when we jump to **500 candidates**, the throughput collapses dramatically down to **40 req/s**. This
  highlights that querying and parsing 500 jobs per schedule loop creates a substantial CPU bottleneck (JSON
  deserialization constraint).

#### Table 3: Extreme Load (1000 Concurrent Users)

These tests explore system boundaries with 5 workers handling an extreme number of simultaneous queries (1000
concurrent "scheduler" loops).

| Scenario (5 workers, 1000 users) | Request Count | Throughput (req/s) | Min Resp. | Median (50%) | 95% Resp. | 99% Resp. | Max Resp. |
|:---------------------------------|:--------------|:-------------------|:----------|:-------------|:----------|:----------|:----------|
| **Run 12h50**                    | 318,606       | ~350 req/s         | 1.48 ms   | 5 ms         | 6 ms      | 8 ms      | 217.2 ms  |
| **Run 13h09**                    | 322,635       | ~355 req/s         | 1.45 ms   | 5 ms         | 6 ms      | 7 ms      | 852.1 ms  |

*Observation:* Scaling up to 1000 concurrent virtual users drastically pushes the overall throughput (~350 req/sec).
However, the median response time naturally degrades slightly (from ~1ms to 5ms) due to contention for CPU time slices,
and extreme max response times (P100) become much more prominent (reaching up to 852ms) due to context-switching
overhead.

### Overall Conclusion

1. **JSON Overhead**: The benchmark proves that the system's performance is heavily tied to the `candidates-count`.
   Keeping the candidate subset small (e.g., `< 100`) via a strict primary query allows rapid response times.
2. **Concurrency capability**: The Python matching algorithm scales horizontally effectively, handling massive
   throughput under heavy user loads (1000 users) while maintaining sub-10 millisecond response times for the 99th
   percentile.

## Redis Data Model Design

### 1. Introduction

This document outlines the design for mapping DIRAC job scheduling requirements and node characteristics to Redis data
structures. The goal is to support high-performance matchmaking at a scale of ~10M pending jobs and ~100 sites (×500
nodes), ensuring atomicity and efficiency.

### 2. Requirements & Query Patterns

- **Matchmaking Query:** A Pilot requests a job based on its characteristics (Available RAM, Available CPU, Tags, Site
  Name). The system must return the highest priority job that fits these constraints.
- **Atomicity:** Concurrent pilots must not be assigned the same job.
- **Scale:** ~10,000,000 pending jobs, ~100 active sites ×500 nodes.

### 3. Alternative Data Models

#### Alternative A: ZSET Queue + Hash Data + Lua Evaluation (Classic Redis)

- **Data Model:**
    - `jobs:pending` (ZSET): Scores represent priority, values are Job IDs.
    - `job:<id>` (HASH): Stores job requirements (`min_ram`, `min_cpu`, `target_site`).
- **Matching flow:** A Lua script fetches the top N jobs from the ZSET using `ZRANGE`, iterates through them, retrieves
  requirements via `HMGET`, evaluates constraints against the Pilot's context, and atomically removes (`ZREM`) the first
  matching job.
- **Pros:** Uses core Redis structures (no modules). 100% atomic execution. Predictable memory footprint.
- **Cons:**
    - **"Head-of-line blocking" risk:** If the top 1000 jobs require GPU and the Pilot has none, the Lua script wastes
      CPU cycles iterating over incompatible jobs.
    - **Single-thread blocking:** Redis executes Lua scripts atomically, meaning a long-running script (iterating over
      hundreds of jobs) will block the entire Redis instance, delaying all other operations and pilots.
    - **Data fetching overhead:** Performing `HMGET` for every candidate job during the evaluation loop accumulates
      significant latency relative to in-memory evaluations.
    - **Costly priority updates:** Updating job priorities requires modifying the `ZSET` score, which is an O(log N)
      operation. At 10 million jobs, frequent priority reassessments can cause CPU spikes.

#### Alternative B: RedisJSON & RediSearch (Indexed Matching)

- **Data Model:** Jobs are stored as JSON or Hashes, and a RediSearch Index is built on top of the scheduling
  requirements (e.g., numeric index on `ram`, text/tag index on `site`).
- **Matching flow:** The application executes a search query:
  `FT.SEARCH idx:jobs "@req_ram:[-inf $pilot_ram] @target_site:{$pilot_site | ANY}" SORTBY priority DESC LIMIT 0 1`.
  Once a job is found, a small Lua script attempts to "lock" it atomically.
- **Pros:** Delegates complex multi-criteria filtering to the database engine. O(1) or O(log N) search time regardless
  of queue shape. Highly scalable for complex Dirac JDL requirements.
- **Cons:** Requires the RediSearch module. Indexing significantly increases memory usage. Atomicity requires an
  optimistic locking approach (Find -> Try to Lock -> Retry if locked by another pilot).

#### Alternative C: Categorized Queues / Bucket Model

- **Data Model:** Instead of a single queue, jobs are pre-routed into multiple specific queues based on their
  requirements upon submission.
    - Examples: `jobs:pending:site:LCG`, `jobs:pending:high_mem`, `jobs:pending:gpu`.
    - The queues can be simple `LIST`s or `ZSET`s (for priority within the bucket). Job details remain in `job:<id>`
      (HASH).
- **Matching flow:** A Pilot checks the specific queues that match its capabilities. If a Pilot is at the LCG site and
  has a GPU, it directly pops (`LPOP` or `ZPOPMIN`) from `jobs:pending:site:LCG` or `jobs:pending:gpu`.
- **Pros:**
    - Extremely fast read operations (O(1) or O(log N)).
    - Zero "head-of-line blocking" since Pilots only look at pre-validated compatible queues.
    - No complex Lua iteration required.
- **Cons:**
    - **Combinatorial Explosion:** DIRAC job requirements are complex and multi-dimensional (Site, RAM, CPU, Tags). If a
      job requires "Site=LCG" AND "RAM>4000", which queue does it go into?
    - **Complex Write Logic:** The insertion logic becomes highly complex. If jobs are duplicated across multiple queues
      to solve the combination issue, it creates a massive risk of race conditions and stale data (a job popped from the
      GPU queue must be hunted down and removed from the LCG queue).

#### Alternative D: Custom Native Redis Module (Rust)

- **Data Model:** Instead of generic Redis data types (ZSET, HASH), jobs and sites are mapped to highly packed memory
  structures (`structs` in Rust). The entire matchmaking logic and queue state are managed completely in-memory by a
  compiled Rust module loaded into the Redis server (`redis-module-rs`).
- **Matching flow:** The Pilot calls a custom command introduced by the module, such as `DIRAC.MATCH $ram $cpu $site`.
  The matching logic executes directly in compiled machine code within the main Redis thread, evaluating thousands of
  constraints in microseconds, and returns the assigned Job ID.
- **Pros:**
    - **Absolute peak performance:** CPU execution is native, bypassing Lua interpreter overhead or RediSearch index
      lookups.
    - **Extreme Memory Efficiency:** Using dense Rust structs reduces the memory footprint for 10M jobs to just a few
      hundred megabytes (no Redis dictionary or skiplist overhead).
    - **Memory Safety:** Unlike C modules, Rust guarantees memory safety at compile time, eliminating most risks of
      segfaults or memory leaks.
    - 100% atomic (runs on the main Redis thread).
- **Cons:**
    - **Ecosystem Gap:** DIRAC is primarily a Python ecosystem. Introducing a core component in low-level Rust creates a
      significant maintenance and contribution barrier.
    - **Deployment Complexity:** The compiled module (`.so`) must be explicitly built for the target architecture and
      loaded into the Redis server (`loadmodule`), complicating vanilla deployments.
    - **Panic Risk:** While safe from segfaults, unhandled Rust `panic!` macros across the FFI (Foreign Function
      Interface) boundary can still crash the entire Redis host.

### 4. Memory Estimate at Target Scale (10M Jobs, 1000 Sites)

| Component             | Alt A: ZSET + Hash      | Alt B: RediSearch             | Alt C: Categorized Queues         | Alt D: Custom Native Module     |
|:----------------------|:------------------------|:------------------------------|:----------------------------------|:--------------------------------|
| **1000 Sites**        | ~200 KB                 | ~200 KB                       | ~200 KB                           | ~50 KB (Dense structs)          |
| **10M Jobs Data**     | ~1.5 GB                 | ~1.5 GB                       | ~1.5 GB                           | ~600 MB (Dense structs)         |
| **Indexing / Queues** | ~1.1 GB (ZSET overhead) | ~3.5 to 5 GB (Search Indexes) | ~1.3 GB (Multiple ZSETs overhead) | ~200 MB (Custom internal index) |
| **Total Estimated**   | **~2.6 GB - 3.5 GB**    | **~5 GB - 8 GB**              | **~2.8 GB - 4.0 GB**              | **~800 MB - 1.0 GB**            |

*Conclusion on Memory:*

- **Alternatives A and C** fit comfortably within standard, generic Redis deployments with a minimal footprint.
- **Alternative B** trades RAM for search speed, requiring more provisioning but remaining viable on modern hardware
  (e.g., a 16GB RAM instance).
- **Alternative D** is by far the most memory-efficient and performant, but at the cost of significant engineering and
  deployment complexity.

### 5. Recommendation

...
