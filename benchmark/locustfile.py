#!/usr/bin/env python3
"""Locust load testing suite for the DIRAC matchmaking prototype.

This module tests the throughput and latency of the Python matching algorithm
by directly firing events to Locust's metric system.

Workflow:
    1. Generate the benchmark database once:
           pixi run generate_db --num-jobs 800000 --num-nodes 50000

    2. Run the benchmark:
           pixi run benchmark -u 100 -r 50 -t 15m --num-nodes 50000 --log-level ERROR
"""

from __future__ import annotations

import random
import sqlite3
import time
from collections.abc import Iterable

from locust import User, between, events, task
from locust.runners import MasterRunner

from matchmaking.config.logger import configure_logger, logger
from matchmaking.core import utils
from matchmaking.core.main import select_job
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

JOB_POOL_SIZE = 0
NODES_POOL: list[Node] = []
CANDIDATE_POOL: list[Job] = []
SCHEDULING_CONFIG: SchedulingConfig | None = None

_CANDIDATE_WINDOW_QUERY = """
    WITH candidate_window AS (
        SELECT data, id, 0 AS window_segment
        FROM jobs
        WHERE id BETWEEN ? AND ?
        UNION ALL
        SELECT data, id, 1 AS window_segment
        FROM jobs
        WHERE id BETWEEN 1 AND ?
    )
    SELECT data
    FROM candidate_window
    ORDER BY window_segment, id
"""


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
    parser.add_argument("--num-nodes", type=int, default=50000, help="Number of nodes to load from the database")
    parser.add_argument(
        "--num-jobs",
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
    global CANDIDATE_POOL, JOB_POOL_SIZE, NODES_POOL, SCHEDULING_CONFIG

    configure_logger(opts.log_level)

    try:
        SCHEDULING_CONFIG = SchedulingConfig.load_from_yaml(opts.config_path)
        logger.info("Loaded scheduling config from %s", opts.config_path)
    except Exception as e:
        logger.error("Failed to load scheduling config: %s", e)
        raise SystemExit(1) from e

    try:
        JOB_POOL_SIZE = _get_max_job_id(opts.db_path)
        NODES_POOL = _load_nodes(opts.db_path, opts.num_nodes)
    except Exception as e:
        logger.error("Failed to load pools from %s: %s", opts.db_path, e)
        logger.error("Generate the database first: pixi run generate_db")
        raise SystemExit(1) from e

    if JOB_POOL_SIZE < opts.num_jobs:
        logger.error(
            "Database contains %s jobs, but --num-jobs requires %s.",
            JOB_POOL_SIZE,
            opts.num_jobs,
        )
        raise SystemExit(1)

    start_id = random.Random(opts.seed).randint(1, JOB_POOL_SIZE)  # noqa: S311
    CANDIDATE_POOL = _load_candidate_jobs(opts.db_path, start_id, opts.num_jobs, JOB_POOL_SIZE)
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

    def on_start(self):
        """Create the per-user random generator outside the hot path."""
        if not JOB_POOL_SIZE or not NODES_POOL or SCHEDULING_CONFIG is None:
            raise SystemExit("Pools not initialized — check on_test_start logs.")

        self._rng = random.Random(self.environment.parsed_options.seed)  # noqa: S311

    @task
    def evaluate_select_job(self):
        """Simulate a pilot requesting a job: filter_by_job_type compatible candidates, then rank.

        The candidate pool is loaded and validated once at test startup so both
        Locust latency and throughput measure the same matchmaking operation.
        """
        node = self._rng.choice(NODES_POOL)

        start_time = time.perf_counter()
        selected_job = None
        error = None

        try:
            selected_job = select_job(node, rng=self._rng, config=SCHEDULING_CONFIG)
        except Exception as e:
            error = e
            logger.error("Error during select_job: %s", e)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Python",
            name="select_job_cycle",
            response_time=total_time_ms,
            exception=error,
            context={"matched": selected_job is not None},
        )
