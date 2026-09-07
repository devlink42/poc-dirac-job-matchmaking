#!/usr/bin/env python3
"""Locust load testing suite for the DIRAC matchmaking prototype.

This module tests the throughput and latency of the Python matching algorithm
by directly firing events to Locust's metric system.

Workflow:
    1. Generate the benchmark database once (required for Python and Lua):
        pixi run generate_db --num-jobs 10000000 --num-nodes 50000

    2. Run the benchmark:
        pixi run benchmark -u 100 -r 50 -t 15m --match-mode python --num-nodes 50000 --log-level ERROR
"""

from __future__ import annotations

import itertools
import random
import sqlite3
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime

import gevent
import redis
from locust import User, constant, events, task
from locust.runners import MasterRunner

from matchmaking.config.logger import configure_logger, logger
from matchmaking.config.py_redis.config import PY_REDIS_JOB_KEY, PY_REDIS_NODES_KEY
from matchmaking.core.main import select_job
from matchmaking.core.py_redis.scheduler import fetch_candidate_jobs
from matchmaking.core.router import MatchMode
from matchmaking.core.utils import set_jobs
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node
from matchmaking.models.utils import JobStatus

JOB_POOL_SIZE = 0
NODES_POOL: list[Node] = []
CANDIDATE_POOL: list[Job] = []
SCHEDULING_CONFIG: SchedulingConfig | None = None

_USER_SEQ = itertools.count()
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

redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def _reset_job(job: Job) -> None:
    """Reset a running job back to WAITING so it can be picked up again."""
    job.status = JobStatus.WAITING
    job.assigned_site = None
    job.submit_time = datetime.now(tz=UTC)


with open("./matchmaking/core/lua/alt_a/match_making.lua") as file:
    match_script_alt_a = redis_client.register_script(file.read())

with open("./matchmaking/logic/lua/alt_c/match_making.lua") as file:
    match_script_alt_c = redis_client.register_script(file.read())


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
    number_of_jobs: int,
    pool_size: int,
) -> Iterable[tuple[str]]:
    """Load one circular candidate window with a single indexed query.

    Args:
        connection: Read-only benchmark database connection.
        start_id: First job identifier in the window.
        number_of_jobs: Exact number of jobs to load.
        pool_size: Number of densely indexed jobs available to the benchmark.

    Returns:
        An iterable over the serialized jobs in circular key order.

    Raises:
        ValueError: If the requested window cannot fit the configured pool.
    """
    if pool_size <= 0 or not 1 <= start_id <= pool_size or not 0 <= number_of_jobs <= pool_size:
        raise ValueError("Invalid candidate window for the configured job pool.")

    if number_of_jobs == 0:
        return ()

    last_unwrapped_id = start_id + number_of_jobs - 1
    first_range_end = min(last_unwrapped_id, pool_size)
    second_range_end = max(last_unwrapped_id - pool_size, 0)

    return connection.execute(
        _CANDIDATE_WINDOW_QUERY,
        (start_id, first_range_end, second_range_end),
    )


def _load_candidate_jobs(db_path: str, start_id: int, number_of_jobs: int, pool_size: int) -> list[Job]:
    """Load and validate the candidate pool once before the benchmark starts."""
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return [
            Job.model_validate_json(row[0])
            for row in _load_candidate_data(connection, start_id, number_of_jobs, pool_size)
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
    parser.add_argument(
        "--reset-delay",
        type=float,
        default=5.0,
        help="Delay in seconds before resetting a RUNNING job back to WAITING.",
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
        match_mode = MatchMode(opts.match_mode)
        if match_mode in (MatchMode.PYTHON_REDIS, MatchMode.LUA_ALT_A, MatchMode.LUA_ALT_C):
            raw_nodes = redis_client.hvals(PY_REDIS_NODES_KEY)
            JOB_POOL_SIZE = redis_client.hlen(PY_REDIS_JOB_KEY)
            NODES_POOL = [Node.model_validate_json(n) for n in raw_nodes][: opts.num_nodes]

            logger.info("Loaded from Redis")
        elif match_mode is MatchMode.PYTHON:
            JOB_POOL_SIZE = _get_max_job_id(opts.db_path)
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

    if JOB_POOL_SIZE < opts.num_jobs:
        logger.error(
            "Database contains %s jobs, but --num-jobs requires %s.",
            JOB_POOL_SIZE,
            opts.num_jobs,
        )
        raise SystemExit(1)

    start_id = random.Random(opts.seed).randint(1, JOB_POOL_SIZE)  # noqa: S311
    CANDIDATE_POOL = _load_candidate_jobs(opts.db_path, start_id, opts.num_jobs, JOB_POOL_SIZE)
    set_jobs(CANDIDATE_POOL)

    logger.info(
        "Ready: %s nodes, %s candidates loaded from %s available jobs in %s.",
        len(NODES_POOL),
        len(CANDIDATE_POOL),
        JOB_POOL_SIZE,
        opts.db_path,
    )


class MatchmakingUser(User):
    """Simulates a scheduler process matching jobs to nodes."""

    wait_time = constant(0)

    def __init__(self, environment):
        super().__init__(environment)
        self._rng = None
        self._db_conn = None
        self.job_ids = []
        self._candidates_count = 0

    def on_start(self):
        """Create the per-user random generator outside the hot path."""
        if not JOB_POOL_SIZE or not NODES_POOL or SCHEDULING_CONFIG is None:
            raise SystemExit("Pools not initialized — check on_test_start logs.")

        self._rng = random.Random(self.environment.parsed_options.seed + next(_USER_SEQ))  # noqa: S311
        self._candidates_count = len(CANDIDATE_POOL)

        match_mode = MatchMode(self.environment.parsed_options.match_mode)
        if match_mode is MatchMode.PYTHON:
            self._db_conn = sqlite3.connect(f"file:{self.environment.parsed_options.db_path}?mode=ro", uri=True)
        elif match_mode in (MatchMode.PYTHON_REDIS, MatchMode.LUA_ALT_A, MatchMode.LUA_ALT_C):
            self.job_ids = list(redis_client.hkeys(PY_REDIS_JOB_KEY))

    def on_stop(self):
        if self._db_conn:
            self._db_conn.close()

    @task
    def evaluate_select_job(self):
        match_mode = MatchMode(self.environment.parsed_options.match_mode)
        if match_mode is MatchMode.PYTHON:
            self.evaluate_select_job_python()
        elif match_mode is MatchMode.PYTHON_REDIS:
            self.evaluate_select_job_python_redis()
        elif match_mode is MatchMode.LUA_ALT_A:
            self.evaluate_select_job_redis_alt_a()
        elif match_mode is MatchMode.LUA_ALT_C:
            self.evaluate_select_job_redis_alt_c()

    def evaluate_select_job_python(self):
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
            if selected_job is not None:
                delay = self.environment.parsed_options.reset_delay
                if delay > 0:
                    gevent.spawn_later(delay, _reset_job, selected_job)
        except Exception as e:
            error = e
            logger.error("Error during select_job: %s", e)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Python",
            name="select_job[match]" if selected_job else "select_job[no_match]",
            response_time=total_time_ms,
            response_length=sys.getsizeof(selected_job) if selected_job else 0,
            exception=error,
            context={"matched": selected_job is not None},
        )

    def evaluate_select_job_python_redis(self):
        if not self.job_ids:
            return

        node = self._rng.choice(NODES_POOL)

        set_jobs(fetch_candidate_jobs(redis_client, self.environment.parsed_options.num_jobs))

        start_time = time.perf_counter()
        selected_job = None
        error = None

        try:
            selected_job = select_job(node)
            if selected_job is not None:
                delay = self.environment.parsed_options.reset_delay
                if delay > 0:
                    gevent.spawn_later(delay, _reset_job, selected_job)
        except Exception as e:
            error = e
            logger.error("Error during select_job: %s", e)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Redis-Python",
            name="select_job[match]" if selected_job else "select_job[no_match]",
            response_time=total_time_ms,
            response_length=sys.getsizeof(selected_job) if selected_job else 0,
            exception=error,
            context={"matched": selected_job is not None},
        )

    def evaluate_select_job_redis_alt_a(self):
        """Simulate a pilot requesting a job using Redis Lua script (Alternative A)."""
        node = self._rng.choice(NODES_POOL)

        args = [
            node.cpu.ram_mb,
            node.cpu.num_cores,
            node.site,
            self._candidates_count,
            str(node.system.name),
            str(node.system.glibc),
            1 if node.system.user_namespaces else 0,
            node.wall_time,
            node.cpu_work,
            str(node.cpu.architecture.name),
            node.cpu.architecture.microarchitecture_level,
            node.gpu.count,
            node.gpu.ram_mb if node.gpu.ram_mb else 0,
            node.gpu.vendor if node.gpu.vendor else "",
            str(node.gpu.compute_capability) if node.gpu.compute_capability else "",
            str(node.gpu.driver_version) if node.gpu.driver_version else "",
            node.io.scratch_mb if node.io else 0,
        ]

        start_time = time.perf_counter()
        selected_job_json = None
        error = None

        try:
            selected_job_json = match_script_alt_a(
                keys=["jobs:pending", "job:"],
                args=args,
            )
        except Exception as e:
            error = e
            logger.error("Error during Redis select_job (Alt A): %s", e)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Redis-Lua-AltA",
            name="select_job_cycle",
            response_time=total_time_ms,
            response_length=len(selected_job_json) if selected_job_json else 0,
            exception=error,
            context={"matched": selected_job_json is not None},
        )

    def evaluate_select_job_redis_alt_c(self):
        """Simulate a pilot requesting a job using Redis Lua script (Alternative C)."""
        node = self._rng.choice(NODES_POOL)

        args = [
            node.site,
            "ANALYSIS",  # Placeholder job type
            str(node.cpu.architecture.name),
            "1" if node.gpu.count > 0 else "0",
            node.cpu.ram_mb,
            node.cpu.num_cores,
        ]

        start_time = time.perf_counter()
        selected_job_id = None
        error = None

        try:
            selected_job_id = match_script_alt_c(
                keys=[],
                args=args,
            )
        except Exception as e:
            error = e
            logger.error("Error during Redis select_job (Alt C): %s", e)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Redis-Lua-AltC",
            name="select_job_cycle",
            response_time=total_time_ms,
            response_length=len(selected_job_id) if selected_job_id else 0,
            exception=error,
            context={"matched": selected_job_id is not None},
        )
