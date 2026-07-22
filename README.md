![Job Matchmaking tests](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/ci.yml/badge.svg?branch=main)
[![Job Matchmaking coverage](https://codecov.io/github/devlink42/poc-dirac-job-matchmaking/graph/badge.svg?token=NUR0RK0T0I)](https://codecov.io/github/devlink42/poc-dirac-job-matchmaking)

# POC Dirac/DiracX Job Matchmaking

## Matchmaking Performance Benchmark

This directory contains the performance test suite using Locust to benchmark the matchmaking system.
This framework establishes the baseline for the Python prototype and will be reused for subsequent phases (e.g., Redis,
Lua).

### Features

- Evaluates the core `select_job` algorithm.
- Simulates realistic distributions (LHCb production distributions).
- Measures **throughput (matches/sec)** and **latency distributions**.
- Configurable scale parameters (number of jobs, nodes, users, arrival rate).

### Prerequisites

Ensure your environment is properly set up using Pixi. Locust is already included in the `pixi.toml` dependencies.

### Running the Benchmarks

#### Generate data

You need to generate a database with a large number of jobs and nodes before running the benchmark.
You can do this using the following command:

```bash
pixi run generate_db --num-jobs 800000 --num-nodes 50000
```

And this is the list of available parameters for the `generate_db` command:

- `--num-jobs`: Number of jobs to generate. Use at least the benchmark `--num-jobs`; additional jobs only
  increase the diversity of the sampled circular window. (Default: 800000)
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

`--num-nodes` must not exceed the number of nodes in the generated database. The database must contain at least as
many jobs as requested by `--num-jobs`.

#### Web UI Mode (Interactive Exploration)

To explore latency graphs, throughput curves, and easily tweak the user load:

```bash
pixi run benchmark-ui -u 100 -r 50 -t 15m --num-nodes 50000
```

`--num-nodes` must not exceed the number of nodes in the generated database. The database must contain at least as
many jobs as requested by `--num-jobs`.

Then, open your browser at http://localhost:8089.

#### Configurable parameters for the benchmark

You can pass custom arguments to adjust the scale of the pre-loaded data:

- `--num-jobs`: Number of jobs pulled from the database and evaluated in each selection cycle. In the benchmark
  command, this is the candidate-window size and must not exceed the number of jobs generated in the database.
  (Default: 800000)
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
