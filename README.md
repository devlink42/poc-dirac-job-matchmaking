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
  this is the candidate-window size and must not exceed the number of jobs generated in the database.
  (Default: 10000000)
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

#### Using Redis Alternatives (Alternative C)

If you plan to run the benchmarks using a Redis-backed Matchmaking engine (`--match-mode lua_alt_c`), you must spawn the
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
    pixi run seed_redis_alt_c --db-path benchmark/benchmark.db --redis-host localhost --log-level INFO
    ```

4. Run the benchmark specifying the correct matchmaking mode:

    ```bash
    pixi run benchmark -u 100 -r 50 -t 15m --match-mode lua_alt_c --num-jobs 10000000 --num-nodes 50000
    ```

### Baseline Benchmark Results (Python Prototype)

**Test Context:** 10,000,000 Jobs, 50,000 Nodes.

## Redis Data Model Design

The supporting requirements, alternative data models, and memory estimates are maintained in
[Data Model & Architecture Design](docs/data_model_design.md).

### Recommendation

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
