#!/usr/bin/env python3
"""Locust load testing suite for the DIRAC matchmaking prototype.

This module tests the throughput and latency of the Python matching algorithm
by directly firing events to Locust's metric system.
"""

from __future__ import annotations

import random
import sys
import time

from locust import User, between, events, task
from locust.runners import MasterRunner

from benchmark.data_generator import job_generator, node_generator
from matchmaking.config.logger import configure_logger, logger
from matchmaking.core.match_making import valid_job_specs_with_node
from matchmaking.core.scheduler import select_job
from matchmaking.models.config import SchedulingConfig

configure_logger("INFO")

_rng = random.Random()  # noqa: S311

# Hard cap on the job pool to keep memory bounded.
# The pool must be at least as large as --candidates-count; it is computed
# dynamically in on_test_start as min(num_jobs, max(_JOB_POOL_CAP, candidates_count)).
_JOB_POOL_CAP = 10_000

# Both pools are pre-built once and reused across all tasks via random.sample /
# random.choice — no Pydantic object is created in the hot path.
JOBS_POOL: list = []
NODES_POOL: list = []
SCHEDULING_CONFIG = SchedulingConfig()


@events.init_command_line_parser.add_listener
def _(parser):
    """Register custom benchmark arguments."""
    parser.add_argument("--num-jobs", type=int, default=100000, help="Simulated job queue depth")
    parser.add_argument("--num-nodes", type=int, default=1000, help="Number of nodes to preload")
    parser.add_argument(
        "--config-path",
        type=str,
        default="matchmaking/config/scheduling.yaml",
        help="Path to the scheduling configuration YAML",
    )
    parser.add_argument(
        "--candidates-count",
        type=int,
        default=1000,
        help="Number of candidate jobs to evaluate per select_job call",
    )


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Build job/node pools and load the scheduling config before the test starts."""
    if isinstance(environment.runner, MasterRunner):
        return

    opts = environment.parsed_options
    global JOBS_POOL, NODES_POOL, SCHEDULING_CONFIG

    try:
        SCHEDULING_CONFIG = SchedulingConfig.load_from_yaml(opts.config_path)
        logger.info(f"Loaded scheduling config from {opts.config_path}")
    except Exception as e:
        logger.error(f"Failed to load scheduling config: {e}")
        raise SystemExit(1) from e

    # Cap the pool so RAM stays bounded while keeping enough variety for sampling.
    pool_size = min(opts.num_jobs, max(_JOB_POOL_CAP, opts.candidates_count))
    JOBS_POOL = list(job_generator(pool_size))
    NODES_POOL = list(node_generator(opts.num_nodes))
    logger.info(
        f"Ready: {len(NODES_POOL)} nodes, {len(JOBS_POOL)} jobs pooled "
        f"(queue depth: {opts.num_jobs}, candidates per task: {opts.candidates_count})."
    )


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

        self._candidates_count = self.environment.parsed_options.candidates_count

    @task
    def evaluate_select_job(self):
        """Simulate a pilot requesting a job: filter compatible candidates, then rank.

        Both pool lookups are O(k) list operations — no object creation in the
        hot path. Only matchmaking logic is timed.
        """
        node = _rng.choice(NODES_POOL)
        candidates = _rng.sample(JOBS_POOL, min(self._candidates_count, len(JOBS_POOL)))

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
            logger.error(f"Error during select_job: {e}")

        total_time_ms = (time.perf_counter() - start_time) * 1000

        events.request.fire(
            request_type="Python",
            name="select_job_cycle",
            response_time=total_time_ms,
            response_length=sys.getsizeof(selected_job) if selected_job else 0,
            exception=error,
            context={"matched": selected_job is not None},
        )
