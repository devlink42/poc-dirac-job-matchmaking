#!/usr/bin/env python3
"""A script to seed a Redis database with jobs for testing purposes.

This script connects to a SQLite database to retrieve jobs data, processes the data
into a format compatible with Redis, and interacts with a Redis database to load
the jobs into appropriate structures such as hash mappings and priority queues.

The script also provides command-line interface options for customizing the SQLite
database file path, Redis connection settings, and log level.

Workflow:
    1. Generate the benchmark database once (required for Python and Lua):
        pixi run generate_db --num-jobs 10000000 --num-nodes 50000

    2. Make sure Redis is running and accessible via the default host and port.
        docker compose up -d redis

    3. Seed the Redis database with jobs data from the SQLite database.
        pixi run seed_redis_alt_a --db-path benchmark/benchmark.db --redis-host localhost --log-level INFO

    4. Run the benchmark:
        pixi run benchmark -u 100 -r 50 -t 15m --match-mode lua_alt_a --num-jobs 10000000 --num-nodes 50000
"""

from __future__ import annotations

import argparse
import json
import sqlite3

import redis

from matchmaking.config.logger import configure_logger, logger
from matchmaking.models.lua.alt_a.job import Job


def seed_database(redis_client: redis.Redis, db_path: str = "benchmark/benchmark.db"):
    logger.info(f"Connecting to SQLite database at {db_path}...")
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to SQLite database at {db_path}: {e}")
        raise

    logger.info("Clearing the Redis database...")
    try:
        redis_client.flushdb()
    except redis.RedisError as e:
        logger.error(f"Failed to flush Redis database: {e}")
        raise

    logger.info("Loading jobs into Redis...")
    pipeline = redis_client.pipeline()

    logger.debug("Executing SELECT query on jobs table")
    try:
        cursor.execute("SELECT id, data FROM jobs")
    except sqlite3.Error as e:
        logger.error(f"Failed to execute SELECT query on jobs table: {e}")
        raise

    count = 0
    for _, data_str in cursor:
        try:
            job_data = json.loads(data_str)
            job_alt_a = Job.model_validate(job_data)

            # 3. Inject into Redis
            redis_key = f"job:{job_alt_a.job_id}"

            # Hash mapping
            pipeline.hset(redis_key, mapping=job_alt_a.to_redis_hash())

            # ZSET priority queue
            pipeline.zadd("jobs:pending", {job_alt_a.job_id: job_alt_a.priority})

            count += 1
            if count % 10000 == 0:
                pipeline.execute()
                logger.info(f"{count} jobs inserted...")
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Error parsing job data. Skipping job. Error: {e}")
            continue
        except redis.RedisError as e:
            logger.error(f"Redis pipeline error while adding job {job_data.get('job_id')} to pipeline. Error: {e}")
            continue

    try:
        pipeline.execute()
    except redis.RedisError as e:
        logger.error(f"Failed to execute final Redis pipeline: {e}")
        raise

    logger.info(f"Successfully finished loading {count} jobs!")


def main():
    parser = argparse.ArgumentParser(description="Seed Redis database with jobs for testing.")
    parser.add_argument("--db-path", default="benchmark/benchmark.db", help="Path to the benchmark SQLite DB")
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
        r.ping()  # test connection immediately
    except redis.ConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}")
        exit(1)

    try:
        seed_database(r, args.db_path)
    except Exception as e:
        logger.critical(f"A critical error occurred preventing the database seeding: {e}")
        exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
