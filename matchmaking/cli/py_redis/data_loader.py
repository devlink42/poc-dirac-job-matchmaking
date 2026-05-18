#!/usr/bin/env python3

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

        if i % 1000 == 0:
            job_pipe.execute()

    job_pipe.execute()

    logger.info("Generating and loading %s nodes into Redis...", num_nodes)
    node_pipe = redis_client.pipeline()

    for i, node in enumerate(node_generator(num_nodes), 1):
        node_pipe.hset("nodes", node.node_id, node.model_dump_json())

        if i % 1000 == 0:
            node_pipe.execute()

    node_pipe.execute()
    logger.info("Data loaded successfully.")


def main():
    parser = argparse.ArgumentParser(description="Populate Redis with synthetic matchmaking data.")
    parser.add_argument("--num-jobs", type=int, default=1000000, help="Number of jobs to generate")
    parser.add_argument("--num-nodes", type=int, default=10000, help="Number of nodes to generate")
    parser.add_argument("--host", type=str, default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    args = parser.parse_args()

    configure_logger("INFO")

    r = redis.Redis(host=args.host, port=args.port, db=0, decode_responses=True)

    # Clear existing data
    r.delete("jobs")
    r.delete("nodes")

    load_data(r, args.num_jobs, args.num_nodes)


if __name__ == "__main__":
    main()
