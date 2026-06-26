#!/usr/bin/env python3
"""Populate Redis with synthetic matchmaking data.

This script generates synthetic job and node data and loads it into a Redis
database. It uses the ``benchmark.data_generator`` module to create the data
and the ``redis`` library to interact with Redis.

Workflow:
  1. Make sure Redis is running and accessible via the default host and port:
    docker compose up -d redis

  2. Generate the benchmark database once:
    pixi run data_loader --num-jobs 10000000 --num-nodes 50000

  3. Run the benchmark:
    pixi run benchmark -u 100 -r 50 -t 15m --match-mode python_redis --num-jobs 10000000 --num-nodes 50000
"""

from __future__ import annotations

import argparse

import redis

from benchmark.data_generator import job_generator, node_generator
from matchmaking.config.logger import configure_logger, logger
from matchmaking.config.py_redis.config import PY_REDIS_JOB_KEY, PY_REDIS_NODES_KEY

# Number of HSET commands buffered in the pipeline before flushing to Redis.
# Keeps per-request memory bounded to O(_BATCH_SIZE) regardless of total volume.
_BATCH_SIZE = 10000


def load_data(redis_client: redis.Redis, num_jobs: int, num_nodes: int) -> None:
    """Load generated jobs and nodes into Redis using batched pipelines.

    Uses non-transactional pipelines (``transaction=False``) for maximum
    throughput — no ``MULTI``/``EXEC`` overhead — while flushing every
    :data:`_BATCH_SIZE` commands to keep memory usage bounded.

    Args:
        redis_client: A connected Redis client with ``decode_responses=True``.
        num_jobs: Number of synthetic jobs to generate and load.
        num_nodes: Number of synthetic nodes to generate and load.
    """
    logger.info("Generating and loading %s jobs into Redis...", num_jobs)

    pipe = redis_client.pipeline(transaction=False)
    pending = 0

    for i, job in enumerate(job_generator(num_jobs), 1):
        pipe.hset(PY_REDIS_JOB_KEY, job.job_id, job.model_dump_json())
        pending += 1

        if pending >= _BATCH_SIZE:
            pipe.execute()
            pending = 0
            logger.info("Loaded %s/%s jobs into Redis", i, num_jobs)

    if pending:
        pipe.execute()

    logger.info("All %s jobs loaded into Redis.", num_jobs)
    logger.info("Generating and loading %s nodes into Redis...", num_nodes)

    pipe = redis_client.pipeline(transaction=False)
    pending = 0

    for i, node in enumerate(node_generator(num_nodes), 1):
        pipe.hset(PY_REDIS_NODES_KEY, node.node_id, node.model_dump_json())
        pending += 1

        if pending >= _BATCH_SIZE:
            pipe.execute()
            pending = 0
            logger.info("Loaded %s/%s nodes into Redis", i, num_nodes)

    if pending:
        pipe.execute()

    logger.info("All %s nodes loaded into Redis.", num_nodes)
    logger.info("Data loaded successfully.")


def main() -> None:
    """Entry point: parse CLI arguments and populate Redis."""
    parser = argparse.ArgumentParser(description="Populate Redis with synthetic matchmaking data.")
    parser.add_argument(
        "--num-jobs", type=int, default=1000000, help="Number of jobs to generate (default: 1 000 000)."
    )
    parser.add_argument("--num-nodes", type=int, default=10000, help="Number of nodes to generate (default: 10 000).")
    parser.add_argument("--redis-host", default="localhost", help="Redis host.")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port.")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis DB index.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity level.",
    )

    args = parser.parse_args()

    configure_logger(args.log_level)

    logger.info(
        "Connecting to Redis at %s:%s/%s",
        args.redis_host,
        args.redis_port,
        args.redis_db,
    )

    try:
        r = redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db, decode_responses=True)
        r.ping()
    except redis.ConnectionError as exc:
        logger.error("Could not connect to Redis: %s", exc)
        raise SystemExit(1) from exc

    logger.info(
        "Wiping stale data from Redis at %s:%s/%s",
        args.redis_host,
        args.redis_port,
        args.redis_db,
    )

    # Wipe any stale data before loading a fresh dataset.
    r.delete(PY_REDIS_JOB_KEY)
    r.delete(PY_REDIS_NODES_KEY)

    logger.info("Loading %d jobs and %d nodes into Redis", args.num_jobs, args.num_nodes)

    load_data(r, args.num_jobs, args.num_nodes)


if __name__ == "__main__":
    main()
