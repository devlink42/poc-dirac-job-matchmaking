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

The database must contain at least as many jobs as requested by `--num-jobs` and `--num-nodes` must not exceed the
number of nodes in the generated database.

#### Web UI Mode (Interactive Exploration)

To explore latency graphs, throughput curves, and easily tweak the user load:

```bash
pixi run benchmark-ui -u 100 -r 50 -t 15m --num-nodes 50000
```

The database must contain at least as many jobs as requested by `--num-jobs` and `--num-nodes` must not exceed the
number of nodes in the generated database.

Then, open your browser at http://localhost:8089.

#### Configurable parameters for the benchmark

You can pass custom arguments to adjust the scale of the pre-loaded data:

- `--match-mode`: The matching algorithm/target system to use. Allowed values: `python` (default), `python_redis`,
  `lua_alt_a`, `lua_alt_b`, `lua_alt_c`.
- `--num-jobs`: Number of jobs pulled from the database and evaluated in each selection cycle. In the benchmark command,
  this is the candidate-window size and must not exceed the number of jobs generated in the database. (Default:
  10000000)
- `--num-nodes`: Defines the total number of nodes available in the persistent database. (Default: 50000)
- `--seed`: Random seed for reproducibility. (Default: 0)
- `--config-path`: Path to the scheduling configuration. (Default: `config/scheduling.yaml`)
- `--db-path`: Path to the SQLite benchmark database. (generate with `benchmark/generate_db.py`, default:
  `benchmark/benchmark.db`).
- `--log-level`: Logging verbosity level, it can be `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. To have better
  results, set it to `ERROR` or `CRITICAL`. (Default: `INFO`).

Locust core parameters:

- `-u` / `--users`: The number of concurrent users (threads simulating scheduler processes).
- `-r` / `--spawn-rate`: How many users to spawn per second.
- `-t` / `--run-time`: Automatically stop the test after a certain duration (e.g., `1m`, `30s`).
- `-w` / `--workers`: The number of worker processes to spawn (Distributed mode only, default: 5).

*Note: The arrival rate of requests per user is defined in `locustfile.py` via the `wait_time` attribute. Also, ALL
benchmark executions generate comprehensive CSV and HTML reports.*

#### Using Redis Alternatives (Alternative A)

If you plan to run the benchmarks using a Redis-backed Matchmaking engine (`--match-mode lua_alt_a`), you must spawn the
Redis instance and pre-seed the jobs into it using the following steps:

1. Ensure the SQLite DB contains the desired amount of jobs (e.g. 10M jobs):

    ```bash
    pixi run generate_db --num-jobs 10000000 --num-nodes 50000
    ```

2. Make sure you have a Redis service running (a `docker-compose.yml` is provided at the root of the project):

    ```bash
    docker compose up -d redis
    ```

3. Seed the Redis instance using the generated SQLite data:

    ```bash
    pixi run seed_redis_alt_a --db-path benchmark/benchmark.db --redis-host localhost --log-level INFO
    ```

4. Run the benchmark specifying the correct matchmaking mode:

    ```bash
    pixi run benchmark -u 100 -r 50 -t 15m --match-mode lua_alt_a --num-jobs 10000000 --num-nodes 50000
    ```

### Baseline Benchmark Results (Python Prototype)

**Test Context:** 10,000,000 Jobs, 50,000 Nodes.

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
- **Pros:**
    - Uses core Redis structures (no modules).
    - 100% atomic execution.
    - Predictable memory footprint.
- **Cons:**
    - **"Head-of-line blocking" risk:** If the top 1000 jobs require GPU and the Pilot has none, the Lua script wastes
      CPU cycles iterating over incompatible jobs.
    - **Single-thread blocking:** Redis executes Lua scripts atomically, meaning a long-running script (iterating over
      hundreds of jobs) will block the entire Redis instance, delaying all other operations and pilots.
    - **Data fetching overhead:** Performing `HMGET` for every candidate job during the evaluation loop accumulates
      significant latency relative to in-memory evaluations.
    - **Costly priority updates:** Updating job priorities requires modifying the `ZSET` score, which is an O (log N)
      operation. At 10 million jobs, frequent priority reassessments can cause CPU spikes.

#### Alternative B: RedisJSON & RediSearch (Indexed Matching)

- **Data Model:** Jobs are stored as JSON or Hashes, and a RediSearch Index is built on top of the scheduling
  requirements (e.g., numeric index on `ram`, text/tag index on `site`).
- **Matching flow:** The application executes a search query:
  `FT.SEARCH idx:jobs "@req_ram:[-inf $pilot_ram] @target_site:{$pilot_site | ANY}" SORTBY priority DESC LIMIT 0 1`.
  Once a job is found, a small Lua script attempts to "lock" it atomically.
- **Pros:**
    - Delegates complex multi-criteria filtering to the database engine.
    - O (1) or O (log N) search time regardless of queue shape.
    - Highly scalable for complex Dirac JDL requirements.
- **Cons:**
    - Requires the RediSearch module.
    - Indexing significantly increases memory usage.
    - Atomicity requires an optimistic locking approach (Find -> Try to Lock -> Retry if locked by another pilot).

#### Alternative C: Categorized Queues / Bucket Model

- **Data Model:** Instead of a single queue, jobs are pre-routed into multiple specific queues based on their
  requirements upon submission.
    - Examples: `jobs:pending:site:LCG`, `jobs:pending:high_mem`, `jobs:pending:gpu`.
    - The queues can be simple `LIST`s or `ZSET`s (for priority within the bucket). Job details remain in `job:<id>`
      (HASH).
- **Matching flow:** A Pilot checks the specific queues that match its capabilities. If a Pilot is at the LCG site and
  has a GPU, it directly pops (`LPOP` or `ZPOPMIN`) from `jobs:pending:site:LCG` or `jobs:pending:gpu`.
- **Pros:**
    - Extremely fast read operations (O (1) or O (log N)).
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

All of these estimates are fictive and non-tested. It's probably an order of magnitude off, and it's likely that the
actual memory usage will be higher due to Redis's internal data structures and overhead.

| Component                    | Alt A: ZSET + Hash      | Alt B: RediSearch             | Alt C: Categorized Queues         | Alt D: Custom Native Module     |
|:-----------------------------|:------------------------|:------------------------------|:----------------------------------|:--------------------------------|
| **1000 Sites** (50000 nodes) | ~500 KB                 | ~500 KB                       | ~500 KB                           | ~50 KB (Dense structs)          |
| **10M Jobs Data**            | ~1.5 GB                 | ~1.5 GB                       | ~1.5 GB                           | ~600 MB (Dense structs)         |
| **Indexing / Queues**        | ~1.1 GB (ZSET overhead) | ~3.5 to 5 GB (Search Indexes) | ~1.3 GB (Multiple ZSETs overhead) | ~200 MB (Custom internal index) |
| **Total Estimated**          | **~2.6 GB - 3.5 GB**    | **~5 GB - 8 GB**              | **~2.8 GB - 4.0 GB**              | **~800 MB - 1.0 GB**            |

*Conclusion on Memory:*

- **Alternatives A and C** fit comfortably within standard, generic Redis deployments with a minimal footprint.
- **Alternative B** trades RAM for search speed, requiring more provisioning but remaining viable on modern hardware
  (e.g., a 16GB RAM instance).
- **Alternative D** is by far the most memory-efficient and performant, but at the cost of significant engineering and
  deployment complexity.

### 5. Recommendation

**Alternative A** is no longer viable due to its high memory footprint, limited scalability, and high latency when
finding corresponding nodes for jobs.

**Alternative B** is recommended for its balance between memory usage and search performance, making it suitable for
moderate to large-scale deployments. It offers a good trade-off between memory efficiency and search speed, aligning
well with the requirements of our application. However, atomicity and locking mechanisms must be carefully managed to
safely handle concurrent access to the search index.

**Alternative C** is a viable option for smaller deployments or when memory constraints are critical. It provides a
memory-efficient solution with a straightforward implementation, making it suitable for environments with limited
resources. However, it sacrifices some search performance compared to Alternative B.

Finally, **Alternative D** is an option for deployments with very strict memory and performance requirements, offering
absolute peak performance and extreme memory efficiency. However, this advantage comes at the cost of significant
engineering effort and deployment complexity.

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
