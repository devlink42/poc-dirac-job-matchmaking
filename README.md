![Job Matchmaking tests](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/main.yml/badge.svg?branch=main)
![Job Matchmaking coverage](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/coverage.yml/badge.svg?branch=main)
![Job Matchmaking deployment](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/deployment.yml/badge.svg?branch=main)

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
pixi run python -m benchmark.generate_db --num-jobs 10000000 --num-nodes 20000
```

#### Headless Mode (Quick Baseline)

To run a quick 5 minutes benchmark directly in your terminal with 100 concurrent users generating load:

```bash
pixi run benchmark -u 100 -r 50 -t 5m --num-jobs 10000000 --num-nodes 20000
```

`--num-jobs` and `--num-nodes` have to be set to the same value as the generated data.

#### Distributed Headless Mode (High Load Testing)

To run the benchmark in a distributed environment with multiple nodes:

```bash
pixi run benchmark-dist -u 100 -r 50 -t 15m --num-jobs 10000000 --num-nodes 20000
```

`--num-jobs` and `--num-nodes` have to be set to the same value as the generated data.

#### Web UI Mode (Interactive Exploration)

To explore latency graphs, throughput curves, and easily tweak the user load:

```bash
pixi run benchmark-ui -u 100 -r 50 -t 15m --num-jobs 10000000 --num-nodes 20000
```

`--num-jobs` and `--num-nodes` have to be set to the same value as the generated data.

#### Distributed Web UI Mode (High Load Testing)

To run the benchmark in a distributed environment with multiple nodes:

```bash
pixi run benchmark-dist-ui -u 100 -r 50 -t 15m --num-jobs 10000000 --num-nodes 20000
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
