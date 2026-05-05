#!/usr/bin/env python3
"""Locust load testing suite for the DIRAC matchmaking prototype.

This module tests the throughput and latency of the Python matching algorithm
by directly firing events to Locust's metric system.

Workflow:
    1. Generate the benchmark database once:
           pixi run python -m benchmark.generate_db --num-jobs 10000000 --num-nodes 20000

    2. Run the benchmark:
           pixi run benchmark -u 100 -r 50 -t 5m --num-jobs 10000000 --num-nodes 20000
"""

from __future__ import annotations

import random
import sqlite3
import sys
import time

from locust import User, between, events, task
from locust.runners import MasterRunner

from matchmaking.config.logger import configure_logger, logger
from matchmaking.core.match_making import valid_job_specs_with_node
from matchmaking.core.scheduler import select_job
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

configure_logger("ERROR")

_rng = random.Random()  # noqa: S311

# Both pools are loaded once from SQLite at test start and reused via
# random.sample / random.choice — no object creation in the hot path.
JOBS_POOL: list[Job] = []
NODES_POOL: list[Node] = []
SCHEDULING_CONFIG = SchedulingConfig()


def _load_pools(db_path: str, num_jobs: int, num_nodes: int) -> tuple[list[Job], list[Node]]:
    """Load job and node pools from the SQLite benchmark database."""
    conn = sqlite3.connect(db_path)
    try:
        jobs = [Job.model_validate_json(row[0]) for row in conn.execute("SELECT data FROM jobs LIMIT ?", (num_jobs,))]
        nodes = [
            Node.model_validate_json(row[0]) for row in conn.execute("SELECT data FROM nodes LIMIT ?", (num_nodes,))
        ]
    finally:
        conn.close()

    return jobs, nodes


@events.init_command_line_parser.add_listener
def _(parser):
    """Register custom benchmark arguments."""
    parser.add_argument("--num-jobs", type=int, default=10000, help="Number of jobs to load from the database")
    parser.add_argument("--num-nodes", type=int, default=1000, help="Number of nodes to load from the database")
    parser.add_argument(
        "--candidates-count",
        type=int,
        default=1000,
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
    global JOBS_POOL, NODES_POOL, SCHEDULING_CONFIG

    try:
        SCHEDULING_CONFIG = SchedulingConfig.load_from_yaml(opts.config_path)
        logger.info("Loaded scheduling config from %s", opts.config_path)
    except Exception as e:
        logger.error("Failed to load scheduling config: %s", e)
        raise SystemExit(1) from e

    try:
        JOBS_POOL, NODES_POOL = _load_pools(opts.db_path, opts.num_jobs, opts.num_nodes)
    except Exception as e:
        logger.error("Failed to load pools from %s: %s", opts.db_path, e)
        logger.error("Generate the database first: pixi run python -m benchmark.generate_db")
        raise SystemExit(1) from e

    if len(JOBS_POOL) < opts.candidates_count:
        logger.warning(
            "Job pool (%s) is smaller than --candidates-count (%s). Candidates will be capped to pool size.",
            len(JOBS_POOL),
            opts.candidates_count,
        )

    logger.info("Ready: %s nodes, %s jobs loaded from %s.", len(NODES_POOL), len(JOBS_POOL), opts.db_path)


class MatchmakingUser(User):
    """Simulates a scheduler process matching jobs to nodes."""

    wait_time = between(0.01, 0.1)

    def __init__(self, environment):
        super().__init__(environment)
        self._candidates_count = None

    def on_start(self):
        """Cache per-user options to avoid repeated attribute lookups in the hot path."""
        if not JOBS_POOL or not NODES_POOL:
            raise SystemExit("Pools not initialized — check on_test_start logs.")

        self._candidates_count = min(
            self.environment.parsed_options.candidates_count,
            len(JOBS_POOL),
        )

    @task
    def evaluate_select_job(self):
        """Simulate a pilot requesting a job: filter compatible candidates, then rank.

        Both pool lookups are O(k) list operations — no object creation in the
        hot path. Only matchmaking logic is timed.
        """
        node = _rng.choice(NODES_POOL)
        candidates = _rng.sample(JOBS_POOL, self._candidates_count)

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
