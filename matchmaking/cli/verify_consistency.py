#!/usr/bin/env python3
"""Verify that the SQLite-backed and Redis-backed matchmaking pipelines return
exactly the same results when fed the same underlying data.

The two pipelines share the same scheduling function (``select_job``); only
the storage layer differs. Equivalence therefore reduces to:

    1. **Data consistency** — for every sampled ``job_id`` - ``node_id``, the
       Pydantic model rebuilt from SQLite equals the one rebuilt from Redis.

    2. **Pipeline consistency** — given the same node and the same candidate
       set, ``select_job`` returns the same job whichever backend supplied
       the inputs.

Workflow:
    1. Populate both stores from the same generator seed:
           pixi run generate_db --num-jobs 10000000 --num-nodes 50000
           pixi run data_loader --num-jobs 10000000 --num-nodes 50000

    2. Run the check:
           pixi run verify_consistency --sample-jobs 5000 --sample-nodes 1000 --pipeline-rounds 200
"""

from __future__ import annotations

import argparse
import random
import sqlite3
import sys
from dataclasses import dataclass

import redis

from matchmaking.config.logger import configure_logger, logger
from matchmaking.config.py_redis.config import PY_REDIS_JOB_KEY, PY_REDIS_NODES_KEY
from matchmaking.core.scheduler import select_job
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

# SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 999 on older builds and 32766
# on newer ones. Stay well under both so the same script runs anywhere.
_SQL_BATCH = 900
# Redis HMGET accepts huge arglists, but keeping batches modest avoids
# blocking the server for long stretches on multi-million-key requests.
_REDIS_BATCH = 10000


def _chunked(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start : start + size]


@dataclass
class CheckReport:
    name: str
    checked: int
    mismatches: list[str]

    @property
    def ok(self) -> bool:
        return not self.mismatches

    def log(self) -> None:
        if self.ok:
            logger.info("[OK] %s: %s items consistent.", self.name, self.checked)
        else:
            logger.error(
                "[FAIL] %s: %s/%s mismatches. First 5: %s",
                self.name,
                len(self.mismatches),
                self.checked,
                self.mismatches[:5],
            )


def _fetch_sqlite_jobs(conn: sqlite3.Connection, job_ids: list[str]) -> dict[str, Job]:
    """Fetch jobs from SQLite indexed by ``job_id``, batching to stay below the variable limit."""
    out: dict[str, Job] = {}

    for chunk in _chunked(job_ids, _SQL_BATCH):
        placeholders = ",".join("?" * len(chunk))
        cur = conn.execute(
            f"SELECT job_id, data FROM jobs WHERE job_id IN ({placeholders})",  # noqa: S608
            chunk,
        )
        for row in cur.fetchall():
            out[row[0]] = Job.model_validate_json(row[1])

    return out


def _fetch_redis_jobs(r: redis.Redis, job_ids: list[str]) -> dict[str, Job]:
    """Fetch jobs from Redis indexed by ``job_id``."""
    out: dict[str, Job] = {}
    for chunk in _chunked(job_ids, _REDIS_BATCH):
        raw = r.hmget(PY_REDIS_JOB_KEY, chunk)
        for job_id, data in zip(chunk, raw, strict=False):
            if data is not None:
                out[job_id] = Job.model_validate_json(data)

    return out


def _fetch_sqlite_nodes(conn: sqlite3.Connection, node_ids: list[str]) -> dict[str, Node]:
    out: dict[str, Node] = {}

    for chunk in _chunked(node_ids, _SQL_BATCH):
        placeholders = ",".join("?" * len(chunk))
        cur = conn.execute(
            f"SELECT node_id, data FROM nodes WHERE node_id IN ({placeholders})",  # noqa: S608
            chunk,
        )
        for row in cur.fetchall():
            out[row[0]] = Node.model_validate_json(row[1])

    return out


def _fetch_redis_nodes(r: redis.Redis, node_ids: list[str]) -> dict[str, Node]:
    out: dict[str, Node] = {}

    for chunk in _chunked(node_ids, _REDIS_BATCH):
        raw = r.hmget(PY_REDIS_NODES_KEY, chunk)
        for node_id, data in zip(chunk, raw, strict=False):
            if data is not None:
                out[node_id] = Node.model_validate_json(data)

    return out


def check_population(conn: sqlite3.Connection, r: redis.Redis) -> CheckReport:
    """Compare total counts and key sets between both stores."""
    mismatches: list[str] = []

    sqlite_job_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    sqlite_node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    redis_job_count = r.hlen(PY_REDIS_JOB_KEY)
    redis_node_count = r.hlen(PY_REDIS_NODES_KEY)

    if sqlite_job_count != redis_job_count:
        mismatches.append(f"job count: sqlite={sqlite_job_count} redis={redis_job_count}")

    if sqlite_node_count != redis_node_count:
        mismatches.append(f"node count: sqlite={sqlite_node_count} redis={redis_node_count}")

    logger.info(
        "Population — sqlite: %s jobs / %s nodes, redis: %s jobs / %s nodes",
        sqlite_job_count,
        sqlite_node_count,
        redis_job_count,
        redis_node_count,
    )

    return CheckReport("population", checked=2, mismatches=mismatches)


def check_jobs(conn: sqlite3.Connection, r: redis.Redis, sample_size: int, rng: random.Random) -> CheckReport:
    """Compare a random sample of jobs by job_id."""
    sqlite_ids = [row[0] for row in conn.execute("SELECT job_id FROM jobs")]
    redis_ids = list(r.hkeys(PY_REDIS_JOB_KEY))

    common = sorted(set(sqlite_ids) & set(redis_ids))
    only_sqlite = set(sqlite_ids) - set(redis_ids)
    only_redis = set(redis_ids) - set(sqlite_ids)

    mismatches: list[str] = []
    if only_sqlite:
        mismatches.append(f"{len(only_sqlite)} job_ids only in sqlite (e.g. {sorted(only_sqlite)[:3]})")

    if only_redis:
        mismatches.append(f"{len(only_redis)} job_ids only in redis (e.g. {sorted(only_redis)[:3]})")

    sample = rng.sample(common, min(sample_size, len(common)))
    sqlite_jobs = _fetch_sqlite_jobs(conn, sample)
    redis_jobs = _fetch_redis_jobs(r, sample)

    for job_id in sample:
        if sqlite_jobs.get(job_id) != redis_jobs.get(job_id):
            mismatches.append(f"job {job_id}")

    return CheckReport("jobs", checked=len(sample), mismatches=mismatches)


def check_nodes(conn: sqlite3.Connection, r: redis.Redis, sample_size: int, rng: random.Random) -> CheckReport:
    """Compare a random sample of nodes by node_id."""
    sqlite_ids = [row[0] for row in conn.execute("SELECT node_id FROM nodes")]
    redis_ids = list(r.hkeys(PY_REDIS_NODES_KEY))

    common = sorted(set(sqlite_ids) & set(redis_ids))
    only_sqlite = set(sqlite_ids) - set(redis_ids)
    only_redis = set(redis_ids) - set(sqlite_ids)

    mismatches: list[str] = []
    if only_sqlite:
        mismatches.append(f"{len(only_sqlite)} node_ids only in sqlite (e.g. {sorted(only_sqlite)[:3]})")

    if only_redis:
        mismatches.append(f"{len(only_redis)} node_ids only in redis (e.g. {sorted(only_redis)[:3]})")

    sample = rng.sample(common, min(sample_size, len(common)))
    sqlite_nodes = _fetch_sqlite_nodes(conn, sample)
    redis_nodes = _fetch_redis_nodes(r, sample)

    for node_id in sample:
        if sqlite_nodes.get(node_id) != redis_nodes.get(node_id):
            mismatches.append(f"node {node_id}")

    return CheckReport("nodes", checked=len(sample), mismatches=mismatches)


def check_pipeline(
    conn: sqlite3.Connection,
    r: redis.Redis,
    config: SchedulingConfig,
    rounds: int,
    candidate_jobs_count: int,
    rng: random.Random,
) -> CheckReport:
    """Run ``select_job`` against both backends with identical inputs and compare outcomes."""
    sqlite_job_ids = [row[0] for row in conn.execute("SELECT job_id FROM jobs")]
    sqlite_node_ids = [row[0] for row in conn.execute("SELECT node_id FROM nodes")]
    redis_job_ids = set(r.hkeys(PY_REDIS_JOB_KEY))
    redis_node_ids = set(r.hkeys(PY_REDIS_NODES_KEY))

    common_job_ids = [j for j in sqlite_job_ids if j in redis_job_ids]
    common_node_ids = [n for n in sqlite_node_ids if n in redis_node_ids]

    mismatches: list[str] = []
    if not common_job_ids or not common_node_ids:
        mismatches.append("no overlapping job/node ids between backends")

        return CheckReport("pipeline", checked=0, mismatches=mismatches)

    effective_candidates = min(candidate_jobs_count, len(common_job_ids))

    for i in range(rounds):
        node_id = rng.choice(common_node_ids)
        candidate_ids = rng.sample(common_job_ids, effective_candidates)

        sqlite_nodes = _fetch_sqlite_nodes(conn, [node_id])
        redis_nodes = _fetch_redis_nodes(r, [node_id])
        sqlite_node = sqlite_nodes[node_id]
        redis_node = redis_nodes[node_id]

        sqlite_jobs_map = _fetch_sqlite_jobs(conn, candidate_ids)
        redis_jobs_map = _fetch_redis_jobs(r, candidate_ids)

        # Preserve the candidate_ids order so the selection is reproducible
        # in the event of a tie on the sorting key.
        sqlite_candidates = [sqlite_jobs_map[j] for j in candidate_ids if j in sqlite_jobs_map]
        redis_candidates = [redis_jobs_map[j] for j in candidate_ids if j in redis_jobs_map]

        sqlite_selected = select_job(sqlite_node, sqlite_candidates, config)
        redis_selected = select_job(redis_node, redis_candidates, config)

        sqlite_id = sqlite_selected.job_id if sqlite_selected else None
        redis_id = redis_selected.job_id if redis_selected else None

        if sqlite_id != redis_id:
            mismatches.append(f"round {i}: node={node_id} sqlite={sqlite_id} redis={redis_id}")

    return CheckReport("pipeline", checked=rounds, mismatches=mismatches)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify the SQLite and Redis matchmaking backends agree on identical data.",
    )
    parser.add_argument("--db-path", default="benchmark/benchmark.db", help="Path to the SQLite benchmark database.")
    parser.add_argument("--redis-host", default="localhost", help="Redis host.")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port.")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis DB index.")
    parser.add_argument(
        "--config-path",
        default="matchmaking/config/scheduling.yaml",
        help="Scheduling configuration YAML.",
    )
    parser.add_argument("--sample-jobs", type=int, default=2000, help="Number of jobs to compare by id.")
    parser.add_argument("--sample-nodes", type=int, default=500, help="Number of nodes to compare by id.")
    parser.add_argument("--pipeline-rounds", type=int, default=100, help="Number of select_job rounds to compare.")
    parser.add_argument(
        "--candidate-jobs-count",
        type=int,
        default=500,
        help="Candidate jobs per select_job round.",
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )

    args = parser.parse_args()

    configure_logger(args.log_level)
    rng = random.Random(args.seed)  # noqa: S311

    try:
        conn = sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        logger.error("Could not open SQLite DB at %s: %s", args.db_path, exc)

        return 1

    try:
        r = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            db=args.redis_db,
            decode_responses=True,
        )
        r.ping()
    except redis.ConnectionError as exc:
        logger.error("Could not connect to Redis at %s:%s — %s", args.redis_host, args.redis_port, exc)
        conn.close()

        return 1

    try:
        config = SchedulingConfig.load_from_yaml(args.config_path)
    except Exception as exc:
        logger.error("Failed to load scheduling config: %s", exc)
        conn.close()

        return 1

    reports: list[CheckReport] = []
    try:
        reports.append(check_population(conn, r))
        reports.append(check_jobs(conn, r, args.sample_jobs, rng))
        reports.append(check_nodes(conn, r, args.sample_nodes, rng))
        reports.append(
            check_pipeline(
                conn,
                r,
                config,
                rounds=args.pipeline_rounds,
                candidate_jobs_count=args.candidate_jobs_count,
                rng=rng,
            ),
        )
    finally:
        conn.close()

    for report in reports:
        report.log()

    if all(report.ok for report in reports):
        logger.info("All %s checks passed — SQLite and Redis backends are consistent.", len(reports))

        return 0

    logger.error("Backends diverge. See [FAIL] entries above.")

    return 1


if __name__ == "__main__":
    sys.exit(main())
