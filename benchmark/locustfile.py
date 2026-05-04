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

from benchmark.data_generator import generate_mock_job, node_generator
from matchmaking.config.logger import configure_logger, logger
from matchmaking.core.match_making import valid_job_specs_with_node
from matchmaking.core.scheduler import select_job
from matchmaking.models.config import SchedulingConfig

configure_logger("INFO")

_rng = random.Random()  # noqa: S311

# Nodes are pre-loaded once (typically ~100) and reused across all tasks.
# Jobs are generated on-demand per task so memory usage stays constant
# regardless of the --num-jobs queue depth.
NODES_POOL: list = []
SCHEDULING_CONFIG = SchedulingConfig()
_num_jobs: int = 0


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
    """Build the node pool and load the scheduling config before the test starts."""
    if isinstance(environment.runner, MasterRunner):
        return

    opts = environment.parsed_options
    global NODES_POOL, SCHEDULING_CONFIG, _num_jobs

    try:
        SCHEDULING_CONFIG = SchedulingConfig.load_from_yaml(opts.config_path)
        logger.info(f"Loaded scheduling config from {opts.config_path}")
    except Exception as e:
        logger.error(f"Failed to load scheduling config: {e}")
        raise SystemExit(1) from e

    _num_jobs = opts.num_jobs
    NODES_POOL = list(node_generator(opts.num_nodes))
    logger.info(f"Ready: {opts.num_nodes} nodes loaded, simulating {opts.num_jobs}-job queue.")


class MatchmakingUser(User):
    """Simulates a scheduler process matching jobs to nodes."""

    wait_time = between(0.01, 0.1)

    def on_start(self):
        """Cache per-user options to avoid repeated attribute lookups in the hot path."""
        if not NODES_POOL or _num_jobs == 0:
            raise SystemExit("Pools not initialized — check on_test_start logs.")
        self._candidates_count = self.environment.parsed_options.candidates_count

    @task
    def evaluate_select_job(self):
        """Simulate a pilot requesting a job: filter compatible candidates, then rank.

        Candidate generation happens before start_time so only matchmaking
        logic is measured.
        """
        node = _rng.choice(NODES_POOL)

        # Sample job IDs from [0, _num_jobs) to simulate a realistic queue depth
        # without holding all job objects in RAM simultaneously.
        candidates = [generate_mock_job(f"job-{_rng.randrange(_num_jobs)}") for _ in range(self._candidates_count)]

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
