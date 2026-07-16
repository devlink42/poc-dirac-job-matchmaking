#!/usr/bin/env python3
"""Locust load testing suite for the DIRAC matchmaking prototype.

This module tests the throughput and latency of the Python matching algorithm
by directly firing events to Locust's metric system.

Workflow:
  1. Generate the benchmark database once:
    pixi run generate_db --num-jobs 10000000 --num-nodes 50000

  2. Run the benchmark:
    pixi run benchmark -u 100 -r 50 -t 15m --match-mode python --num-jobs 10000000 --num-nodes 50000 --log-level ERROR
"""

from __future__ import annotations

import random
import sqlite3
import sys
import time
from collections.abc import Iterable

import redis
from locust import User, between, events, task
from locust.runners import MasterRunner

from matchmaking.config.logger import configure_logger, logger
from matchmaking.config.py_redis.config import PY_REDIS_JOB_KEY, PY_REDIS_NODES_KEY
from matchmaking.core import utils
from matchmaking.core.main import select_job
from matchmaking.core.py_redis.scheduler import fetch_candidate_jobs
from matchmaking.core.router import MatchMode
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

MAX_JOB_ID_IN_DB = 0
JOB_POOL_SIZE = 0
NODES_POOL: list[Node] = []
CANDIDATE_POOL: list[Job] = []

_CANDIDATE_WINDOW_QUERY = """
    SELECT data
    FROM jobs
    WHERE id BETWEEN ? AND ?
    UNION ALL
    SELECT data
    FROM jobs
    WHERE id BETWEEN 1 AND ?
"""


def _resolve_job_pool_size(num_jobs: int, max_job_id_in_db: int) -> int:
    """Return the effective job pool size used by the benchmark.

    Args:
        num_jobs: Requested job pool size from the CLI.
        max_job_id_in_db: Maximum job identifier available in the SQLite database.

    Returns:
        The number of jobs effectively available to the benchmark.
    """
    return min(num_jobs, max_job_id_in_db)

redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def _load_nodes(db_path: str, num_nodes: int) -> list[Node]:
    """Load node pools from the SQLite benchmark database."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        nodes = [
            Node.model_validate_json(row[0]) for row in conn.execute("SELECT data FROM nodes LIMIT ?", (num_nodes,))
        ]
    finally:
        conn.close()

    return nodes


def _get_max_job_id(db_path: str) -> int:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return conn.execute("SELECT MAX(id) FROM jobs").fetchone()[0] or 0
    finally:
        conn.close()


def _load_candidate_data(
    connection: sqlite3.Connection,
    start_id: int,
    candidate_count: int,
    pool_size: int,
) -> Iterable[tuple[str]]:
    """Load one circular candidate window with a single indexed query.

    Args:
        connection: Read-only benchmark database connection.
        start_id: First job identifier in the window.
        candidate_count: Exact number of jobs to load.
        pool_size: Number of densely indexed jobs available to the benchmark.

    Returns:
        An iterable over the serialized jobs in circular key order.

    Raises:
        ValueError: If the requested window cannot fit the configured pool.
    """
    if pool_size <= 0 or not 1 <= start_id <= pool_size or not 0 <= candidate_count <= pool_size:
        raise ValueError("Invalid candidate window for the configured job pool.")

    if candidate_count == 0:
        return ()

    last_unwrapped_id = start_id + candidate_count - 1
    first_range_end = min(last_unwrapped_id, pool_size)
    second_range_end = max(last_unwrapped_id - pool_size, 0)

    return connection.execute(
        _CANDIDATE_WINDOW_QUERY,
        (start_id, first_range_end, second_range_end),
    )


def _load_candidate_jobs(db_path: str, start_id: int, candidate_count: int, pool_size: int) -> list[Job]:
    """Load and validate the candidate pool once before the benchmark starts."""
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return [
            Job.model_validate_json(row[0])
            for row in _load_candidate_data(connection, start_id, candidate_count, pool_size)
        ]
    finally:
        connection.close()


@events.init_command_line_parser.add_listener
def _(parser):
    """Register custom benchmark arguments."""
    parser.add_argument(
        "--match-mode",
        type=str,
        choices=[mode.value for mode in MatchMode],
        default=MatchMode.PYTHON.value,
        help="Matchmaking algorithm to evaluate",
    )
    parser.add_argument("--num-jobs", type=int, default=10000000, help="Number of jobs to load from the database")
    parser.add_argument("--num-nodes", type=int, default=50000, help="Number of nodes to load from the database")
    parser.add_argument(
        "--candidate-jobs-count",
        type=int,
        default=800000,
        help="Number of candidate jobs to evaluate per select_job call",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Seed for the random number generator",
    )
    parser.add_argument(
        "--config-path",
        type=str,
        default="matchmaking/config/scheduling.yaml",
        help="Path to the scheduling configuration YAML",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="benchmark/benchmark.db",
        help="Path to the SQLite benchmark database (generate with benchmark/generate_db.py)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "debug", "info", "warning", "error", "critical"],
        help="Logging verbosity level.",
    )


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Load pools from the database and the scheduling config before the test starts."""
    if isinstance(environment.runner, MasterRunner):
        return

    opts = environment.parsed_options
    global CANDIDATE_POOL, JOB_POOL_SIZE, MAX_JOB_ID_IN_DB, NODES_POOL

    configure_logger(opts.log_level)

    try:
        utils.CONFIG_PATH = opts.config_path
        utils._CONFIG_CACHE = SchedulingConfig.load_from_yaml(opts.config_path)
        logger.info("Loaded scheduling config from %s", opts.config_path)
    except Exception as e:
        logger.error("Failed to load scheduling config: %s", e)
        raise SystemExit(1) from e

    try:
        if MatchMode(opts.match_mode) is MatchMode.PYTHON_REDIS:
            raw_nodes = redis_client.hvals(PY_REDIS_NODES_KEY)
            NODES_POOL = [Node.model_validate_json(n) for n in raw_nodes][: opts.num_nodes]
            MAX_JOB_ID_IN_DB = redis_client.hlen(PY_REDIS_JOB_KEY)
            logger.info("Loaded from Redis")
        elif MatchMode(opts.match_mode) is MatchMode.PYTHON:
            MAX_JOB_ID_IN_DB = _get_max_job_id(opts.db_path)
            NODES_POOL = _load_nodes(opts.db_path, opts.num_nodes)
            logger.info("Loaded from SQLite")
        else:
            raise ValueError(f"Unsupported match mode: {opts.match_mode}")
    except Exception as e:
        logger.error(
            "Failed to load pools: %s\n"
            "Generate the database first: pixi run generate_db --num-jobs 10000000 --num-nodes 50000",
            e,
        )
        raise SystemExit(1) from e

    JOB_POOL_SIZE = _resolve_job_pool_size(opts.num_jobs, MAX_JOB_ID_IN_DB)

    if JOB_POOL_SIZE != opts.candidate_jobs_count:
        logger.warning(
            "Database contains %s jobs, but --num-jobs is %s. Benchmark will use %s jobs.",
            MAX_JOB_ID_IN_DB,
            opts.num_jobs,
            JOB_POOL_SIZE,
        )

    if JOB_POOL_SIZE < opts.candidates_count:
        logger.warning(
            "Job pool (%s) is smaller than --candidates-count (%s). Candidates will be capped to pool size.",
            JOB_POOL_SIZE,
            opts.candidate_jobs_count,
        )

    candidate_count = min(opts.candidates_count, JOB_POOL_SIZE)
    start_id = random.Random(opts.seed).randint(1, JOB_POOL_SIZE)  # noqa: S311
    CANDIDATE_POOL = _load_candidate_jobs(opts.db_path, start_id, candidate_count, JOB_POOL_SIZE)
    utils.JOBS = CANDIDATE_POOL

    logger.info(
        "Ready: %s nodes, %s candidates loaded from %s available jobs in %s.",
        len(NODES_POOL),
        len(CANDIDATE_POOL),
        JOB_POOL_SIZE,
        opts.db_path,
    )


class MatchmakingUser(User):
    """Simulates a scheduler process matching jobs to nodes."""

    wait_time = between(0.001, 1.0)

    def __init__(self, environment):
        super().__init__(environment)
        self._rng = None
        self._db_conn = None
        self.job_ids = []

    def on_start(self):
        """Create the per-user random generator outside the hot path."""
        if not JOB_POOL_SIZE or not NODES_POOL:
            raise SystemExit("Pools not initialized — check on_test_start logs.")

        self._rng = random.Random(self.environment.parsed_options.seed)  # noqa: S311

        if MatchMode(self.environment.parsed_options.match_mode) is MatchMode.PYTHON:
            self._db_conn = sqlite3.connect(f"file:{self.environment.parsed_options.db_path}?mode=ro", uri=True)
        else:
            self.job_ids = list(redis_client.hkeys(PY_REDIS_JOB_KEY))

    def on_stop(self):
        if self._db_conn:
            self._db_conn.close()

    @task
    def evaluate_select_job(self):
        if MatchMode(self.environment.parsed_options.match_mode) is MatchMode.PYTHON:
            self.evaluate_select_job_python()
        else:
            self.evaluate_select_job_python_redis()

    def evaluate_select_job_python(self):
        """Simulate a pilot requesting a job: filter compatible candidates, then rank.

        The candidate pool is loaded and validated once at test startup so both
        Locust latency and throughput measure the same matchmaking operation.
        """
        node = self._rng.choice(NODES_POOL)

        start_time = time.perf_counter()
        selected_job = None
        error = None

        try:
            selected_job = select_job(node)
        except Exception as e:
            error = e
            logger.error("Error during select_job: %s", e)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Python",
            name="select_job_cycle",
            response_time=total_time_ms,
            response_length=sys.getsizeof(selected_job) if selected_job else 0,
            exception=error,
            context={"matched": selected_job is not None},
        )

    def evaluate_select_job_python_redis(self):
        if not self.job_ids:
            return

        node = self._rng.choice(NODES_POOL)

        utils.JOBS = fetch_candidate_jobs(redis_client, self._candidate_jobs_count)

        start_time = time.perf_counter()
        selected_job = None
        error = None

        try:
            selected_job = select_job(node)
        except Exception as e:
            error = e
            logger.error("Error during select_job: %s", e)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Redis-Python",
            name="select_job_cycle",
            response_time=total_time_ms,
            response_length=sys.getsizeof(selected_job) if selected_job else 0,
            exception=error,
            context={"matched": selected_job is not None},
        )
