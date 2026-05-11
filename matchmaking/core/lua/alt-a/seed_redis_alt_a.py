#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from datetime import datetime

import redis

from matchmaking.config.logger import logger

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def seed_database(db_path="../../../benchmark/benchmark.db"):
    logger.info("Vidage de la base Redis...")
    r.flushdb()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()

    logger.info("Chargement des jobs dans Redis...")
    # On utilise un pipeline pour envoyer les requêtes par paquets (très rapide)
    pipeline = r.pipeline()

    cursor.execute("SELECT id, data FROM jobs")
    count = 0

    for _, data_str in cursor:
        job_data = json.loads(data_str)
        job_id = job_data["job_id"]

        # On simplifie : on prend la première spécification de matching
        spec = job_data["matching_specs"][0]

        # Aplatissement des données pour le HASH Redis
        min_cores = spec.get("cpu", {}).get("num-cores", {}).get("min", 1)
        max_cores = spec.get("cpu", {}).get("num-cores", {}).get("max", 1)

        ram_overhead = spec.get("cpu", {}).get("ram-mb", {}).get("request", {}).get("overhead", 0)
        ram_per_core = spec.get("cpu", {}).get("ram-mb", {}).get("request", {}).get("per-core", 0)

        site = spec.get("site", "ANY")
        walltime = spec.get("wall-time", 0)

        # Création du HASH
        redis_key = f"job:{job_id}"
        pipeline.hset(
            redis_key,
            mapping={
                "min_cores": min_cores,
                "max_cores": max_cores,
                "ram_overhead": ram_overhead,
                "ram_per_core": ram_per_core,
                "site": site,
                "walltime": walltime,
            },
        )

        # Ajout dans la file d'attente (ZSET). Score = Timestamp pour du FIFO
        # (Les scores les plus bas seront traités en premier)
        timestamp = datetime.fromisoformat(job_data["submission_time"]).timestamp()
        pipeline.zadd("jobs:pending", {job_id: timestamp})

        count += 1
        if count % 10000 == 0:
            pipeline.execute()
            logger.info(f"{count} jobs insérés...")

    pipeline.execute()
    logger.info("Terminé !")


if __name__ == "__main__":
    seed_database()
