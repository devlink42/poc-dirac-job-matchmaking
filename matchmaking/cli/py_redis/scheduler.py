#!/usr/bin/env python3
"""Redis-backed job scheduler CLI.

Connects to Redis, loads a node specification from a YAML file, then selects
the best matching job for that node using the py_redis scheduling pipeline.

Workflow:
    1. Populate Redis with synthetic data:
           pixi run data_loader --num-jobs 10000000 --num-nodes 50000

    2. Run a single scheduling cycle against a node:
           pixi run py_redis_scheduler node.yaml matchmaking/config/scheduling.yaml
"""

from __future__ import annotations

import argparse
import sys

import redis

from matchmaking.config.logger import configure_logger, logger
from matchmaking.core.main import select_job
from matchmaking.core.py_redis.match_making import match_jobs_with_node_redis
from matchmaking.core.py_redis.scheduler import fetch_candidate_jobs


def main() -> None:
    """Entry point for the Redis-backed scheduler CLI."""
    parser = argparse.ArgumentParser(description="Select a job from Redis for a given node.")
    parser.add_argument("node", nargs="?", help="Path to the node YAML file.")
    parser.add_argument("--redis-host", default="localhost", help="Redis host (default: localhost).")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port (default: 6379).")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis DB index (default: 0).")
    parser.add_argument(
        "--candidate-jobs-count",
        type=int,
        default=500,
        help="Number of candidate jobs to sample per scheduling cycle (default: 500).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity level.",
    )

    args = parser.parse_args()

    configure_logger(args.log_level)

    if not args.node:
        parser.print_help()
        return

    try:
        r = redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db, decode_responses=True)
        r.ping()
    except redis.ConnectionError as exc:
        logger.error("Could not connect to Redis at %s:%s — %s", args.redis_host, args.redis_port, exc)
        sys.exit(1)

    try:
        candidates = fetch_candidate_jobs(r, args.candidate_jobs_count)

        if valid_jobs_node := match_jobs_with_node_redis(candidates, args.node):
            jobs, node = valid_jobs_node

            if jobs:
                allowed_job = select_job(node)

                if allowed_job:
                    logger.info("Job %s selected for execution on %s.", allowed_job.job_id, node.site)
                else:
                    logger.info("No allowed job found for node %s.", node.node_id or node.site)
    except Exception as exc:
        logger.error("Error during Redis matchmaking: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
