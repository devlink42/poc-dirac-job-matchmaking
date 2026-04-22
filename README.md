![Job Matchmaking tests](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/main.yml/badge.svg?branch=main)
![Job Matchmaking coverage](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/coverage.yml/badge.svg?branch=main)
![Job Matchmaking deployment](https://github.com/devlink42/poc-dirac-job-matchmaking/actions/workflows/deployment.yml/badge.svg?branch=main)

# POC Dirac/DiracX Job Matchmaking

## Matchmaking Performance Benchmark

This directory contains the performance test suite using Locust to benchmark the matchmaking system. This framework establishes the baseline for the Python prototype and will be reused for subsequent phases (e.g., Redis, Lua).

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

**Test Context:** 10,000 Jobs, 1,000 Nodes, 50 candidates per cycle, 100 concurrent user, 50 users to spawn per second.

  - **Throughput:** \~XXX matches/sec
  - **Average Latency:** \~XX ms
  - **95th Percentile Latency:** \~XX ms
  - **99th Percentile Latency:** \~XX ms

### Explications supplémentaires :
1. **Pourquoi ne pas utiliser le décorateur `@task` classique avec HTTP ?** Le projet actuel est un algorithme purement Python (pas d'API REST). L'astuce est de faire un `time.perf_counter()` manuel et d'utiliser `events.request.fire()` pour nourrir Locust. Locust tracera alors les graphiques avec ces données exactes.
2. **Génération de charge** : L'argument `--num-jobs` et `--num-nodes` permet de générer la base de données en mémoire au tout début du test (`@events.test_start`). Ensuite, les utilisateurs Locust piochent aléatoirement dans ces listes pour tester la vitesse de l'algorithme sous concurrence.
3. Le code gère les "LHCb production distributions" de façon simulée pour l'instant dans `data_generator.py`. Quand l'équipe te fournira les CSV/JSON, tu auras juste à modifier `generate_mock_job` pour piocher dans leurs vraies data.
