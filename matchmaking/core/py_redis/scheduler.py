#!/usr/bin/env python3

from __future__ import annotations

import redis
from pydantic import ValidationError

from matchmaking.config.logger import logger
from matchmaking.core.py_redis.match_making import JOBS_KEY
from matchmaking.core.router import MatchMode, select_job_for_node
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

_DEFAULT_CANDIDATES_COUNT = 500


def fetch_candidate_jobs(
    redis_client: redis.Redis,
    candidates_count: int,
) -> list[Job]:
    """Sample random jobs from Redis without loading the full key space into memory.

    Uses ``HRANDFIELD`` to obtain *candidates_count* unique random job IDs in a
    single O(count) operation, then fetches their JSON payloads via ``HMGET``.
    Memory consumption is therefore proportional to *candidates_count*, not to
    the total number of jobs stored — critical when the job hash has millions of
    entries and many Locust users run concurrently.

    Args:
        redis_client: A connected Redis client with ``decode_responses=True``.
        candidates_count: How many jobs to sample.  When the hash contains
            fewer entries than requested, all available jobs are returned.

    Returns:
        A list of validated :class:`~matchmaking.models.job.Job` objects.  May
        be shorter than *candidates_count* if some stored payloads are missing
        or fail Pydantic validation (those are silently skipped with a warning).
    """
    job_ids: list[str] = redis_client.hrandfield(JOBS_KEY, candidates_count)
    if not job_ids:
        return []

    raw_jobs: list[str | None] = redis_client.hmget(JOBS_KEY, job_ids)
    jobs: list[Job] = []

    for raw in raw_jobs:
        if raw is None:
            continue

        try:
            jobs.append(Job.model_validate_json(raw))
        except ValidationError as exc:
            logger.warning("Skipping malformed job payload in Redis: %s", exc)

    return jobs


def select_job_from_redis(
    node: Node,
    redis_client: redis.Redis,
    candidates_count: int = _DEFAULT_CANDIDATES_COUNT,
    config: SchedulingConfig | None = None,
) -> Job | None:
    """Full Redis-backed scheduling pipeline for a single node cycle.

    Executes two steps:

    1. **Sample** — fetch *candidates_count* random jobs from Redis via
       :func:`fetch_candidate_jobs`.
    2. **Match** — delegate filtering and scheduling to
       :func:`~matchmaking.core.router.select_job_for_node` under
       :attr:`~matchmaking.core.router.MatchMode.PYTHON_REDIS`.

    Args:
        node: The node (pilot) requesting work.
        redis_client: A connected Redis client with ``decode_responses=True``.
        candidates_count: Number of jobs to sample per scheduling cycle.
        config: Scheduling policy configuration.  When *None*, the default
            config path is used by :func:`~matchmaking.core.scheduler.select_job`.

    Returns:
        The selected :class:`~matchmaking.models.job.Job`, or *None* when no
        compatible candidate is found.
    """
    candidates = fetch_candidate_jobs(redis_client, candidates_count)
    if not candidates:
        logger.debug(
            "No candidate jobs fetched from Redis for node %s.",
            node.node_id,
        )
        return None

    return select_job_for_node(MatchMode.PYTHON_REDIS, node, candidates, config)
