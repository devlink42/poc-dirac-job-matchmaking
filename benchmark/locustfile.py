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
from src.core.valid_pilot import valid_job_with_node

# Force INFO level logging for benchmarking
configure_logger("INFO")

# Global state to hold generated workloads
JOBS_POOL = []
NODES_POOL = []

secure_random = random.SystemRandom()


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize the data pools before the test starts.

    Generates realistic distributions based on environment custom arguments.
    """
    # Prevent workers from re-generating data if running in distributed mode
    if isinstance(environment.runner, MasterRunner):
        return

    num_jobs = environment.parsed_options.num_jobs
    num_nodes = environment.parsed_options.num_nodes

    logger.info(f"Generating {num_jobs} jobs and {num_nodes} nodes for the benchmark...")

    global JOBS_POOL, NODES_POOL

    JOBS_POOL = []
    for i in range(num_jobs):
        JOBS_POOL.append(generate_mock_job(f"job-{i}"))
        if i % 1000 == 0:
            gevent.sleep(0)  # Yield to event loop so worker doesn't miss heartbeats
    logger.debug("Job complete")

    NODES_POOL = []
    for i in range(num_nodes):
        NODES_POOL.append(generate_mock_node(f"node-{i}"))
        if i % 1000 == 0:
            gevent.sleep(0)  # Yield to event loop so worker doesn't miss heartbeats
    logger.debug("Node complete")

    logger.info("Data generation complete.")


@events.init_command_line_parser.add_listener
def _(parser):
    """Add custom command line arguments for the benchmark scaling."""
    parser.add_argument("--num-jobs", type=int, is_secret=False, default=1000, help="Number of jobs to preload")
    parser.add_argument("--num-nodes", type=int, is_secret=False, default=100, help="Number of nodes to preload")


class MatchmakingUser(User):
    """Simulates a scheduler process attempting to match jobs to nodes."""

    # Arrival rate configuration: waits between 0.01 and 0.1 seconds between tasks
    wait_time = between(0.01, 0.1)

    @task
    def evaluate_match(self):
        """Randomly selects a job and a node and evaluates their compatibility.

        Measures the execution time and reports it to Locust's event bus.
        """
        if not JOBS_POOL or not NODES_POOL:
            return

        job = secure_random.choice(JOBS_POOL)
        node = secure_random.choice(NODES_POOL)
        spec = job.matching_specs[0]

        start_time = time.perf_counter()
        is_match = False
        response_size = 0
        error = None

        try:
            # Baseline benchmark of the Python prototype
            is_match = valid_job_with_node(job.job_id, spec, node)
            response_size = sys.getsizeof(is_match)
        except Exception as e:
            error = e

        total_time_ms = (time.perf_counter() - start_time) * 1000

        # Fire the event to Locust to record throughput and latency
        events.request.fire(
            request_type="Python",
            name="valid_job_with_node",
            response_time=total_time_ms,
            response_length=response_size,
            exception=error,
            context={"is_match": is_match},
        )
