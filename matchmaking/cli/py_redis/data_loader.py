#!/usr/bin/env python3
"""Populate Redis with synthetic matchmaking data.

This script generates synthetic job and node data and loads it into a Redis
database. It uses the `benchmark.data_generator` module to create the data and
the `redis` library to interact with Redis.

Workflow:
    1. Make sure Redis is running and accessible via the default host and port.
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


def load_data(redis_client, num_jobs: int, num_nodes: int):
    logger.info("Generating and loading %s jobs into Redis...", num_jobs)
    job_pipe = redis_client.pipeline()

    for i, job in enumerate(job_generator(num_jobs), 1):
        job_pipe.hset("jobs", job.job_id, job.model_dump_json())

        if i % 10000 == 0:
            job_pipe.execute()
            logger.info(f"Loaded {i} jobs into Redis")

    job_pipe.execute()

    logger.info("Generating and loading %s nodes into Redis...", num_nodes)
    node_pipe = redis_client.pipeline()

    for i, node in enumerate(node_generator(num_nodes), 1):
        node_pipe.hset("nodes", node.node_id, node.model_dump_json())

        if i % 10000 == 0:
            node_pipe.execute()
            logger.info(f"Loaded {i} nodes into Redis")

    node_pipe.execute()
    logger.info("Data loaded successfully.")


def main():
    parser = argparse.ArgumentParser(description="Populate Redis with synthetic matchmaking data.")
    parser.add_argument("--num-jobs", type=int, default=1000000, help="Number of jobs to generate")
    parser.add_argument("--num-nodes", type=int, default=10000, help="Number of nodes to generate")
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis DB index")
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Log level"
    )
    args = parser.parse_args()

    configure_logger(args.log_level)

    logger.info(f"Connecting to Redis at {args.redis_host}:{args.redis_port}/{args.redis_db}")
    try:
        r = redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db, decode_responses=True)
        r.ping()
    except redis.ConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}")
        exit(1)

    r.delete("py_redis:jobs")
    r.delete("py_redis:nodes")

    load_data(r, args.num_jobs, args.num_nodes)


if __name__ == "__main__":
    main()
