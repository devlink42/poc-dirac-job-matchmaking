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

_rng = random.Random()  # noqa: S311

MAX_JOB_ID_IN_DB = 0
NODES_POOL = []
SCHEDULING_CONFIG = SchedulingConfig()

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
    parser.add_argument("--num-jobs", type=int, default=100000, help="Number of jobs to load from the database")
    parser.add_argument("--num-nodes", type=int, default=10000, help="Number of nodes to load from the database")
    parser.add_argument(
        "--candidate-jobs-count",
        type=int,
        default=500,
        help="Number of candidate jobs to evaluate per select_job call",
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
    global MAX_JOB_ID_IN_DB, NODES_POOL, SCHEDULING_CONFIG

    configure_logger(opts.log_level)

    try:
        SCHEDULING_CONFIG = SchedulingConfig.load_from_yaml(opts.config_path)
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

    if MAX_JOB_ID_IN_DB < opts.candidate_jobs_count:
        logger.warning(
            "Job pool (%s) is smaller than --candidate-jobs-count (%s). Candidates will be capped to pool size.",
            MAX_JOB_ID_IN_DB,
            opts.candidate_jobs_count,
        )

    logger.info("Ready: %s nodes, %s jobs available.", len(NODES_POOL), MAX_JOB_ID_IN_DB)


class MatchmakingUser(User):
    """Simulates a scheduler process matching jobs to nodes."""

    wait_time = between(0.001, 1.0)

    def __init__(self, environment):
        super().__init__(environment)
        self._candidate_jobs_count = None
        self._db_conn = None
        self.job_ids = []

    def on_start(self):
        """Cache per-user options to avoid repeated attribute lookups in the hot path."""
        if not MAX_JOB_ID_IN_DB or not NODES_POOL:
            raise SystemExit("Pools not initialized — check on_test_start logs.")

        self._candidate_jobs_count = min(
            self.environment.parsed_options.candidate_jobs_count,
            MAX_JOB_ID_IN_DB,
        )

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

        Instead of holding all jobs in RAM, we pick random IDs, load their JSON
        from the database and deserialize them. The IO/Parse times are excluded
        from the final benchmark latency reported to Locust.
        """
        node = _rng.choice(NODES_POOL)

        candidate_ids = _rng.sample(range(1, MAX_JOB_ID_IN_DB + 1), self._candidate_jobs_count)
        placeholders = ",".join("?" * len(candidate_ids))
        cur = self._db_conn.execute(f"SELECT data FROM jobs WHERE id IN ({placeholders})", candidate_ids)  # noqa: S608

        utils.JOBS = [Job.model_validate_json(row[0]) for row in cur.fetchall()]

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

        node = _rng.choice(NODES_POOL)

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
