#!/usr/bin/env python3

from __future__ import annotations

import json
import sqlite3

import redis

from matchmaking.config.logger import configure_logger, logger
from matchmaking.models.lua.alt_a.job import Job

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def seed_database(db_path="benchmark/benchmark.db"):
    logger.info("Clearing the Redis database...")
    r.flushdb()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()

    logger.info("Loading jobs into Redis...")
    pipeline = r.pipeline()
    cursor.execute("SELECT id, data FROM jobs")

    count = 0
    for _, data_str in cursor:
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

    pipeline.execute()
    logger.info("Successfully finished loading jobs!")


# TODO: function main with probably some args and add it to pyproject.toml

if __name__ == "__main__":
    configure_logger("INFO")
    seed_database()
