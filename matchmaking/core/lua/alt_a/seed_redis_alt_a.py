#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from datetime import datetime

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
        spec = job_data.get("matching_specs", [{}])[0]

        # 1. Safe extraction: Ensure we get a dict even if the field is null in JSON
        cpu = spec.get("cpu") or {}
        ram_mb = cpu.get("ram_mb") or {}
        ram_request = ram_mb.get("request") or {}

        system = spec.get("system") or {}
        gpu = spec.get("gpu") or {}

        # Safe time conversion
        try:
            sub_time = datetime.fromisoformat(job_data.get("submission_time", "")).timestamp()
        except (ValueError, TypeError):
            sub_time = 0.0

        # 2. Fill the Alt A Model
        job_alt_a = Job(
            job_id=job_data.get("job_id", ""),
            priority=job_data.get("priority", 0),
            submission_time=sub_time,
            min_cores=cpu.get("num_cores", {}).get("min", 1) if cpu.get("num_cores") else 1,
            max_cores=cpu.get("num_cores", {}).get("max", 1) if cpu.get("num_cores") else 1,
            ram_overhead=ram_request.get("overhead", 0),
            ram_per_core=ram_request.get("per_core", 0),
            walltime=spec.get("wall_time", 0),
            site=spec.get("site", "ANY"),
            job_type=spec.get("job_type", "Unknown"),
            tags=spec.get("tags", ""),
            system_arch=system.get("arch", "ANY"),
            system_os=system.get("os", "ANY"),
            gpu_count=gpu.get("count", 0),
            gpu_model=gpu.get("model", "ANY"),
            gpu_memory=gpu.get("ram_mb", 0),
            owner=job_data.get("owner_dn", "Unknown"),
            group=job_data.get("owner_group", "Unknown"),
        )

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


if __name__ == "__main__":
    configure_logger("INFO")
    seed_database()
