![Job Matchmaking tests](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/main.yml/badge.svg?branch=main)
![Job Matchmaking coverage](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/coverage.yml/badge.svg?branch=main)
![Job Matchmaking deployment](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/deployment.yml/badge.svg?branch=main)

# POC Dirac/DiracX Job Matchmaking

## Matchmaking Performance Benchmark

This directory contains the performance test suite using Locust to benchmark the matchmaking system.
This framework establishes the baseline for the Python prototype and will be reused for subsequent phases (e.g., Redis, Lua).

### Features
- Evaluates the core `valid_job_with_node` algorithm.
- Simulates realistic distributions (LHCb production distributions).
- Measures **throughput (matches/sec)** and **latency distributions**.
- Configurable scale parameters (number of jobs, nodes, users, arrival rate).

### Prerequisites
Ensure your environment is properly set up using Pixi. Locust is already included in the `pixi.toml` dependencies.

### Running the Benchmarks

#### 1. Headless Mode (Quick Baseline)
To run a quick 30-second benchmark directly in your terminal with 10 concurrent users generating load:

```bash
pixi run benchmark --num-jobs 10000000 --num-nodes 20000
```

#### 2. Web UI Mode (Interactive Exploration)
To explore latency graphs, throughput curves, and easily tweak the user load:

```bash
pixi run benchmark-ui --num-jobs 10000000 --num-nodes 20000
```

Then, open your browser at http://localhost:8089.

### Configurable Parameters

You can pass custom arguments to adjust the scale of the pre-loaded data:

  - `--num-jobs`: Defines the size of the job pool to generate (Default: 1000).
  - `--num-nodes`: Defines the size of the node/pilot pool to generate (Default: 100).
  - `--candidates-count`: Number of jobs to evaluate in each selection cycle (Default: 50).
  - `--config-path`: Path to the scheduling configuration (Default: `config/scheduling.yaml`).

Locust core parameters:

  - `-u` / `--users`: The number of concurrent users (threads simulating scheduler processes).
  - `-r` / `--spawn-rate`: How many users to spawn per second.
  - `-t` / `--run-time`: Automatically stop the test after a certain duration (e.g., `1m`, `30s`).

*Note: The arrival rate of requests per user is defined in `locustfile.py` via the `wait_time` attribute.*

### Baseline Benchmark Results (Python Prototype)

**Test Context:** 10,000,000 Jobs, 20,000 Nodes, 50 candidates per cycle, 100 concurrent users,
with 1, 10, 25, 50, 75, 100 users to spawn per second.

*10 minutes of load.*

| Spawn Rate (users/sec) | Throughput (req/s) | Min     | 50% (Median) | 95%  | 99%  | 100% (Max) | Average |
|:-----------------------|:-------------------|:--------|:-------------|:-----|:-----|:-----------|:--------|
| **1**                  | ~698 matches/sec   | 0.66 ms | 1 ms         | 2 ms | 2 ms | 371.00 ms  | 1.26 ms |
| **10**                 | ~674 matches/sec   | 0.64 ms | 1 ms         | 2 ms | 2 ms | 230.20 ms  | 1.34 ms |
| **25**                 | ~682 matches/sec   | 0.65 ms | 1 ms         | 2 ms | 2 ms | 242.44 ms  | 1.33 ms |
| **50**                 | ~668 matches/sec   | 0.66 ms | 1 ms         | 2 ms | 2 ms | 785.74 ms  | 1.36 ms |
| **75**                 | ~655 matches/sec   | 0.67 ms | 1 ms         | 2 ms | 2 ms | 244.54 ms  | 1.39 ms |
| **100**                | ~684 matches/sec   | 0.65 ms | 1 ms         | 2 ms | 2 ms | 226.91 ms  | 1.33 ms |
