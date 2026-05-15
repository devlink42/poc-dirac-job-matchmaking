#!/usr/bin/env python3
"""Locust load testing suite for the DIRAC matchmaking prototype.

This module tests the throughput and latency of the Python matching algorithm
by directly firing events to Locust's metric system.

Workflow:
    1. Generate the benchmark database once (required for Python and Lua):
        pixi run generate_db --num-jobs 10000000 --num-nodes 50000

    2. Run the benchmark:
        pixi run benchmark -u 100 -r 50 -t 15m --match-mode python --num-jobs 10000000 --num-nodes 50000
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
from matchmaking.core.match_making import valid_job_specs_with_node
from matchmaking.core.scheduler import select_job
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

configure_logger("INFO")

_rng = random.Random()  # noqa: S311

MAX_JOB_ID_IN_DB = 0
NODES_POOL = []
SCHEDULING_CONFIG = SchedulingConfig()

redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

with open("./matchmaking/core/lua/alt_a/match_making.lua", "r") as file:
    match_script = redis_client.register_script(file.read())


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
        choices=["python", "lua_alt_a", "lua_alt_b", "lua_alt_c"],
        default="python",
        help="Matchmaking algorithm to evaluate",
    )
    parser.add_argument("--num-jobs", type=int, default=100000, help="Number of jobs to load from the database")
    parser.add_argument("--num-nodes", type=int, default=10000, help="Number of nodes to load from the database")
    parser.add_argument(
        "--candidates-count",
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


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Load pools from the database and the scheduling config before the test starts."""
    if isinstance(environment.runner, MasterRunner):
        return

    opts = environment.parsed_options
    global MAX_JOB_ID_IN_DB, NODES_POOL, SCHEDULING_CONFIG

    try:
        SCHEDULING_CONFIG = SchedulingConfig.load_from_yaml(opts.config_path)
        logger.info("Loaded scheduling config from %s", opts.config_path)
    except Exception as e:
        logger.error("Failed to load scheduling config: %s", e)
        raise SystemExit(1) from e

    try:
        MAX_JOB_ID_IN_DB = _get_max_job_id(opts.db_path)
        NODES_POOL = _load_nodes(opts.db_path, opts.num_nodes)
    except Exception as e:
        logger.error("Failed to load pools from %s: %s", opts.db_path, e)
        logger.error("Generate the database first: pixi run python -m benchmark.generate_db")
        raise SystemExit(1) from e

    if MAX_JOB_ID_IN_DB < opts.candidates_count:
        logger.warning(
            "Job pool (%s) is smaller than --candidates-count (%s). Candidates will be capped to pool size.",
            MAX_JOB_ID_IN_DB,
            opts.candidates_count,
        )

    logger.info("Ready: %s nodes, %s jobs available from %s.", len(NODES_POOL), MAX_JOB_ID_IN_DB, opts.db_path)


class MatchmakingUser(User):
    """Simulates a scheduler process matching jobs to nodes."""

    wait_time = between(0.001, 1.0)

    def __init__(self, environment):
        super().__init__(environment)
        self._candidates_count = None
        self._db_conn = None

    def on_start(self):
        """Cache per-user options to avoid repeated attribute lookups in the hot path."""
        if not MAX_JOB_ID_IN_DB or not NODES_POOL:
            raise SystemExit("Pools not initialized — check on_test_start logs.")

        self._candidates_count = min(
            self.environment.parsed_options.candidates_count,
            MAX_JOB_ID_IN_DB,
        )
        self._db_conn = sqlite3.connect(f"file:{self.environment.parsed_options.db_path}?mode=ro", uri=True)

    def on_stop(self):
        if self._db_conn:
            self._db_conn.close()

    @task
    def evaluate_select_job(self):
        match_mode = self.environment.parsed_options.match_mode

        if match_mode == "python":
            self.evaluate_select_job_python()
        elif match_mode == "lua_alt_a":
            self.evaluate_select_job_redis_alt_a()
        elif match_mode == "lua_alt_b":
            # self.evaluate_select_job_redis_alt_b()
            pass
        elif match_mode == "lua_alt_c":
            # self.evaluate_select_job_redis_alt_c()
            pass
        else:
            logger.error(f"Mode inconnu: {match_mode}")

    def evaluate_select_job_python(self):
        """Simulate a pilot requesting a job: filter compatible candidates, then rank.

        Instead of holding all jobs in RAM, we pick random IDs, load their JSON
        from the database and deserialize them. The IO/Parse times are excluded
        from the final benchmark latency reported to Locust.
        """
        node = _rng.choice(NODES_POOL)

        candidate_ids = _rng.sample(range(1, MAX_JOB_ID_IN_DB + 1), self._candidates_count)
        placeholders = ",".join("?" * len(candidate_ids))
        cur = self._db_conn.execute(f"SELECT data FROM jobs WHERE id IN ({placeholders})", candidate_ids)  # noqa: S608

        candidates = [Job.model_validate_json(row[0]) for row in cur.fetchall()]

        start_time = time.perf_counter()
        selected_job = None
        error = None

        try:
            compatible = [
                job for job in candidates if valid_job_specs_with_node(job.job_id, job.matching_specs[0], node)
            ]
            if compatible:
                selected_job = select_job(node, compatible, SCHEDULING_CONFIG)

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

    def evaluate_select_job_redis_alt_a(self):
        """Simulate a pilot requesting a job using Redis Lua script."""
        node = _rng.choice(NODES_POOL)

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
            selected_job_json = match_script(
                keys=["jobs:pending", "job:"],
                args=args,
            )
        except Exception as e:
            error = e
            logger.error("Error during Redis select_job: %s", e)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Redis-Lua-AltA",
            name="select_job_cycle",
            response_time=total_time_ms,
            response_length=len(selected_job_json) if selected_job_json else 0,
            exception=error,
            context={"matched": selected_job_json is not None},
        )
