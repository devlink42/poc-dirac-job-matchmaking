#!/usr/bin/env python3
"""Locust load testing suite for the DIRAC matchmaking prototype.

This module tests the throughput and latency of the Python matching algorithm
by directly firing events to Locust's metric system.
"""

from __future__ import annotations

import random
import sys
import time

import gevent
from locust import User, between, events, task
from locust.runners import MasterRunner

from benchmark.data_generator import generate_mock_job, generate_mock_node
from config import configure_logger, logger
from src.core.scheduler import select_job
from src.core.valid_pilot import valid_job_with_node
from src.models.config import SchedulingConfig

# Force INFO level logging for benchmarking
configure_logger("INFO")

# Global state to hold generated workloads and configuration
JOBS_POOL = []
NODES_POOL = []
SCHEDULING_CONFIG = SchedulingConfig()

secure_random = random.SystemRandom()


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize the data pools before the test starts.

    Generates realistic distributions based on environment custom arguments.
    """
    # Prevent Master from generating data
    if isinstance(environment.runner, MasterRunner):
        return

    num_jobs = environment.parsed_options.num_jobs
    num_nodes = environment.parsed_options.num_nodes
    config_path = environment.parsed_options.config_path

    global JOBS_POOL, NODES_POOL, SCHEDULING_CONFIG

    try:
        SCHEDULING_CONFIG = SchedulingConfig.load_from_yaml(config_path)
        logger.info(f"Loaded scheduling configuration from {config_path}")
    except Exception as e:
        logger.error(f"Failed to load scheduling configuration: {e}")

    max_preload = 100000
    preload_jobs = min(num_jobs, max_preload)

    logger.info(f"Generating {preload_jobs} jobs (out of {num_jobs} requested) and {num_nodes} nodes for the worker...")

    JOBS_POOL = []
    for i in range(preload_jobs):
        JOBS_POOL.append(generate_mock_job(f"job-{i}"))
        if i % 1000 == 0:
            gevent.sleep(0)

    logger.debug("Job pool complete")

    NODES_POOL = []
    for i in range(num_nodes):
        NODES_POOL.append(generate_mock_node(f"node-{i}"))
        if i % 1000 == 0:
            gevent.sleep(0)

    logger.debug("Node pool complete")

    logger.info("Data generation complete.")


@events.init_command_line_parser.add_listener
def _(parser):
    """Add custom command line arguments for the benchmark scaling."""
    parser.add_argument("--num-jobs", type=int, is_secret=False, default=1000, help="Number of jobs to preload")
    parser.add_argument("--num-nodes", type=int, is_secret=False, default=100, help="Number of nodes to preload")
    parser.add_argument(
        "--config-path",
        type=str,
        is_secret=False,
        default="config/scheduling.yaml",
        help="Path to the scheduling configuration YAML",
    )
    parser.add_argument(
        "--candidates-count",
        type=int,
        is_secret=False,
        default=50,
        help="Number of candidate jobs to evaluate in each select_job call",
    )


class MatchmakingUser(User):
    """Simulates a scheduler process attempting to match jobs to nodes."""

    wait_time = between(0.01, 0.1)

    @task
    def evaluate_select_job(self):
        """Simulate a pilot requesting a job.

        Evaluates compatibility for a set of candidates and then selects the best one.
        Measures the execution time and reports it to Locust.
        """
        if not JOBS_POOL or not NODES_POOL or not SCHEDULING_CONFIG:
            return

        node = secure_random.choice(NODES_POOL)
        candidates_count = self.environment.parsed_options.candidates_count

        # Pick random candidates from the pool
        candidates = secure_random.sample(JOBS_POOL, min(candidates_count, len(JOBS_POOL)))

        start_time = time.perf_counter()
        selected_job = None
        error = None

        try:
            # 1. First step: find which jobs are compatible with this node
            matching_jobs = []
            for job in candidates:
                # We check compatibility for the first matching spec
                if valid_job_with_node(job.job_id, job.matching_specs[0], node):
                    matching_jobs.append(job)

            # 2. Second step: select the best job from compatible ones
            if matching_jobs:
                selected_job = select_job(node, matching_jobs, SCHEDULING_CONFIG)
                if selected_job:
                    logger.info(f"Selected job {selected_job.job_id} for node {node.node_id}")

        except Exception as e:
            error = e
            logger.error(f"Error during select_job: {e}")

        total_time_ms = (time.perf_counter() - start_time) * 1000

        # Fire the event to Locust
        events.request.fire(
            request_type="Python",
            name="select_job_cycle",
            response_time=total_time_ms,
            response_length=sys.getsizeof(selected_job) if selected_job else 0,
            exception=error,
            context={"matched": selected_job is not None},
        )
